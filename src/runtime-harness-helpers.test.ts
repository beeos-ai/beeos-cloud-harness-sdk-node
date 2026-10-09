import { generateKeyPairSync, verify } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { CANVAS_RELAY_PATH, CanvasRelayClient, canvasRelayUpgradeHeaders, canvasRelayWebSocketUrl, encodeCanvasRelayBinary } from "./canvas-relay.js";
import { operationPath } from "./generated/operations.gen.js";
import { RuntimeClaimClient } from "./runtime-claim.js";
import { CLOUD_GATEWAY_RUNTIME_ROUTES, CLOUD_MESSAGE_ROUTES, RuntimeLeaseHttp } from "./runtime-lease-http.js";
import { HarnessProtocolError, RuntimeMessagePlane, RuntimeOperationFiles, putPresignedUpload } from "./runtime-message-plane.js";
import { mintRuntimeRealtimeToken, subscribeRuntimeRealtime } from "./runtime-realtime.js";
import { TerminalBridgeClient, createTerminalAgentAuth, terminalAgentAuthSigningMessage } from "./terminal-bridge.js";

const credential = { runtimeLeaseCredential: "lease-credential", scopedDeliveryKey: "delivery-key" };

function keys() {
  const generated = generateKeyPairSync("ed25519");
  const publicKey = new Uint8Array((generated.publicKey.export({ type: "spki", format: "der" }) as Buffer).subarray(12));
  const privateKey = new Uint8Array((generated.privateKey.export({ type: "pkcs8", format: "der" }) as Buffer).subarray(16));
  return { publicKey, privateKey, verifyKey: generated.publicKey };
}

function jsonFetch(handler: (url: URL, init: RequestInit) => Response): typeof fetch {
  return (async (url: URL, init?: RequestInit) => handler(url, init ?? {})) as typeof fetch;
}

describe("runtime message plane", () => {
  it("posts delivery, history, and file routes from the generated table", async () => {
    const seen: Array<{ url: string; body?: unknown; retry?: boolean }> = [];
    const fetch = jsonFetch((url, init) => {
      seen.push({ url: url.toString(), body: init.body ? JSON.parse(String(init.body)) : undefined });
      return new Response(JSON.stringify({ status: "ok" }), { status: 200 });
    });
    const http = new RuntimeLeaseHttp({ origin: "https://ms.test", fetch });
    const plane = new RuntimeMessagePlane(http);
    await plane.readDeliveries(credential, { maxCount: 1, blockMs: 0 });
    await plane.renewDeliveries(credential, ["d1"]);
    await plane.ackDeliveries(credential, ["d1"]);
    await plane.operationHistory(credential, "op/1");
    await plane.appendOperationMessage(credential, "op/1", { type: "runtime_operation_started", payload: { a: 1 } });
    await plane.historyBoundary(credential, "c/1", "prepare", { schemaVersion: 1 });
    await plane.projectSessionModel(credential, "c/1", { schemaVersion: 1 });
    await plane.deliverCron(credential, "c/1", { schemaVersion: 1 });
    const files = new RuntimeOperationFiles(new RuntimeLeaseHttp({ origin: "https://gw.test", fetch }));
    await files.resolveInput(credential, "op/1", { fileIds: ["f"] });
    await files.presignOutput(credential, "op/1", { name: "a" });
    await files.confirmOutput(credential, "op/1", { fileId: "f" });
    expect(seen.map((call) => call.url.replace(/^https:\/\/(ms|gw)\.test/, ""))).toEqual([
      CLOUD_MESSAGE_ROUTES.deliveriesRead,
      CLOUD_MESSAGE_ROUTES.deliveriesRenew,
      CLOUD_MESSAGE_ROUTES.deliveriesAck,
      CLOUD_MESSAGE_ROUTES.operationHistory("op/1"),
      CLOUD_MESSAGE_ROUTES.operationMessages("op/1"),
      CLOUD_MESSAGE_ROUTES.historyBoundary("c/1", "prepare"),
      CLOUD_MESSAGE_ROUTES.metadataModel("c/1"),
      CLOUD_MESSAGE_ROUTES.cronDelivery("c/1"),
      CLOUD_GATEWAY_RUNTIME_ROUTES.inputFilesResolve("op/1"),
      CLOUD_GATEWAY_RUNTIME_ROUTES.outputFiles("op/1", "presign"),
      CLOUD_GATEWAY_RUNTIME_ROUTES.outputFiles("op/1", "confirm"),
    ]);
    expect(seen[0]!.body).toEqual({ schemaVersion: 1, maxCount: 1, blockMs: 0 });
    expect(seen[4]!.body).toEqual({ schemaVersion: 1, type: "runtime_operation_started", payload: { a: 1 } });
    expect(operationPath("runtimeDeliveriesRead")).toBe("/api/v1/runtime/deliveries/read");
  });

  it("refuses session model and cron without the delivery key", async () => {
    const fetch = vi.fn();
    const plane = new RuntimeMessagePlane(new RuntimeLeaseHttp({ origin: "https://ms.test", fetch: fetch as unknown as typeof fetch }));
    const bare = { runtimeLeaseCredential: "lease" };
    await expect(plane.projectSessionModel(bare, "c", {})).rejects.toThrow(HarnessProtocolError);
    await expect(plane.deliverCron(bare, "c", {})).rejects.toThrow(/X-Runtime-Delivery-Key/);
    expect(fetch).not.toHaveBeenCalled();
  });

  it("puts presigned bytes only to the allowed https origin", async () => {
    const fetch = vi.fn(async () => new Response(null, { status: 200 }));
    const body = new Uint8Array([1, 2]);
    await putPresignedUpload({
      url: "https://uploads.test/obj",
      allowedOrigin: "https://uploads.test",
      body,
      requiredHeaders: { "x-amz-meta": "a" },
      fetchImpl: fetch as unknown as typeof fetch,
    });
    expect(fetch.mock.calls[0]![1].method).toBe("PUT");
    expect(new Headers(fetch.mock.calls[0]![1].headers).get("authorization")).toBeNull();
    await expect(putPresignedUpload({
      url: "https://evil.test/obj", allowedOrigin: "https://uploads.test", body, fetchImpl: fetch as unknown as typeof fetch,
    })).rejects.toThrow(/allowed https origin/);
    await expect(putPresignedUpload({
      url: "http://uploads.test/obj", allowedOrigin: "http://uploads.test", body, fetchImpl: fetch as unknown as typeof fetch,
    })).rejects.toThrow(/allowed https origin/);
  });
});

describe("runtime claims", () => {
  it("rejects an unknown status and requires 204 on release", async () => {
    const http = new RuntimeLeaseHttp({
      origin: "https://ms.test",
      fetch: jsonFetch(() => new Response(JSON.stringify({ status: "nope" }), { status: 200 })),
    });
    await expect(new RuntimeClaimClient(http).claimNext(credential, {
      claimRequestId: "r", instanceId: "i", handlerIdentity: "h", runtimeEpoch: "1",
      journalStoreId: "s", journalGeneration: "1", runtimeLeaseCredential: "lease",
    })).rejects.toThrow(/invalid status/);
    const release = new RuntimeLeaseHttp({
      origin: "https://ms.test",
      fetch: jsonFetch((url) => {
        expect(url.pathname).toBe("/api/v1/runtime/operations/op/claims/release");
        return new Response(null, { status: 204 });
      }),
    });
    await new RuntimeClaimClient(release).releaseClaim(credential, {
      claimId: "c", handlerIdentity: "h", runtimeEpoch: "1", fencingToken: "f",
      journalStoreId: "s", journalGeneration: "1", operationId: "op", requestMessageId: "m",
      runtimeLeaseCredential: "lease", reason: "completed",
    });
  });
});

describe("terminal bridge", () => {
  it("signs the lease-bound agent_auth frame and waits for auth_ok", async () => {
    const material = keys();
    const lease = {
      instanceId: "inst", leaseId: "lease-1", runtimeEpoch: "7",
      runtimeLeaseCredential: "cred", handlerIdentity: "handler",
    };
    const sent: string[] = [];
    const socket = {
      send(data: string) { sent.push(data); },
      close() {},
      on(event: string, listener: (data: string) => void) {
        if (event === "message") queueMicrotask(() => listener(JSON.stringify({ type: "auth_ok" })));
      },
      once(event: string, listener: () => void) {
        if (event === "open") queueMicrotask(listener);
      },
    };
    const frame = await new TerminalBridgeClient({
      bridgeUrl: "wss://bridge.test",
      keys: material,
      lease,
      now: () => 1_700_000_000_000,
      nonce: () => "nonce-1",
      socketFactory: () => socket,
    }).connect();
    const preimage = terminalAgentAuthSigningMessage({
      publicKey: Buffer.from(material.publicKey).toString("base64"),
      timestamp: 1_700_000_000,
      nonce: "nonce-1",
      runtimeLeaseCredential: "cred",
      handlerIdentity: "handler",
      runtimeEpoch: "7",
      leaseId: "lease-1",
    });
    expect(preimage).toBe("terminal|" + frame.public_key + "|1700000000|nonce-1|cred|handler|7|lease-1");
    expect(preimage.includes("inst")).toBe(false);
    expect(verify(null, Buffer.from(preimage), material.verifyKey, Buffer.from(frame.signature, "base64"))).toBe(true);
    expect(JSON.parse(sent[0]!)).toEqual(frame);
    expect(frame.type).toBe("agent_auth");
    expect(frame.service).toBe("terminal");
    expect(() => createTerminalAgentAuth({
      lease: { ...lease, runtimeEpoch: "0" }, keys: material,
    })).toThrow(/invalid Terminal/);
  });
});

describe("canvas relay", () => {
  it("dials /ws/agent with upgrade headers and a length-prefixed frame", async () => {
    const material = keys();
    expect(canvasRelayWebSocketUrl("https://relay.test/base", { agentId: "a", token: "t" }))
      .toBe("wss://relay.test/base/ws/agent?agentId=a&token=t");
    expect(canvasRelayWebSocketUrl("wss://relay.test/ws/agent")).toBe("wss://relay.test/ws/agent");
    const headers = canvasRelayUpgradeHeaders(material);
    expect(Object.keys(headers).sort()).toEqual([
      "X-Agent-Nonce", "X-Agent-Public-Key", "X-Agent-Signature", "X-Agent-Timestamp",
    ]);
    const opened: string[] = [];
    const socket = {
      send() {},
      close() {},
      on(event: string, listener: () => void) {
        if (event === "open") queueMicrotask(listener);
      },
    };
    await new CanvasRelayClient({
      relayUrl: "https://relay.test",
      keys: material,
      socketFactory: (url, upgrade) => {
        opened.push(url);
        expect(upgrade["X-Agent-Public-Key"]).toBe(headers["X-Agent-Public-Key"]);
        return socket;
      },
    }).connect();
    expect(opened).toEqual([`wss://relay.test${CANVAS_RELAY_PATH}`]);
    const binary = encodeCanvasRelayBinary("c1", new Uint8Array([9]));
    expect(binary[0]).toBe(2);
    expect(Buffer.from(binary.subarray(1, 3)).toString()).toBe("c1");
    expect(binary[3]).toBe(9);
  });
});

describe("runtime realtime", () => {
  it("mints a connection token and does not call newSubscription", async () => {
    const fetch = jsonFetch(() => new Response(JSON.stringify({
      token: "jwt", centrifugo_url: "wss://centrifugo.test/connection/websocket",
      channels: ["personal:instance:inst"], principal_id: "inst", expires_at: 9,
    }), { status: 200 }));
    const token = await mintRuntimeRealtimeToken(
      new RuntimeLeaseHttp({ origin: "https://ms.test", fetch }), credential,
    );
    expect(token.channels).toEqual(["personal:instance:inst"]);
    let subscribed = false;
    const sub = await subscribeRuntimeRealtime({
      origin: "https://ms.test",
      credential,
      fetch,
      onEvent: () => {},
      factory: {
        create() {
          return {
            on() {},
            connect() {},
            disconnect() {},
            newSubscription() { subscribed = true; },
          };
        },
      },
    });
    expect(subscribed).toBe(false);
    expect(sub.token.centrifugoUrl).toBe("wss://centrifugo.test/connection/websocket");
    sub.close();
  });
});
