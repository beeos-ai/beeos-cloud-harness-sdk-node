import { generateKeyPairSync, sign } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentGatewayClient } from "./gateway-client.js";
import { CloudHttpError } from "./retry.js";
import type { AgentRequestIdentity } from "./agent-request-signing.js";

const { privateKey, publicKey } = generateKeyPairSync("ed25519");
const identity: AgentRequestIdentity = {
  publicKey: new Uint8Array(publicKey.export({ type: "spki", format: "der" }).subarray(12)),
  sign: (message) => new Uint8Array(sign(null, Buffer.from(message), privateKey)),
};
const BASE = "https://gateway.test";
afterEach(() => vi.unstubAllGlobals());

function stubFetch(handler: (url: URL, init: RequestInit) => Response | Promise<Response>) {
  const spy = vi.fn(async (input: string | URL, init?: RequestInit) => await handler(new URL(input), init ?? {}));
  vi.stubGlobal("fetch", spy);
  return spy;
}
const client = (retry?: false) => new AgentGatewayClient({ baseUrl: BASE, identity,
  ...(retry === false ? { retry: false as const } : {}) });
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status });

describe("AgentGatewayClient", () => {
  it("signs every request with the Gateway v2 headers and refuses another origin", async () => {
    const spy = stubFetch(() => json({ files: [] }));
    await client().files.list();
    const headers = new Headers(spy.mock.calls[0]![1]!.headers);
    for (const name of ["x-agent-public-key", "x-agent-signature", "x-agent-timestamp", "x-agent-nonce"]) {
      expect(headers.get(name)).toBeTruthy();
    }
    await expect(client().fetch("https://evil.test/api/v1/agent/files")).rejects.toThrow("another origin");
  });

  it("retries GET through a 503 blip but surfaces the final 4xx as CloudHttpError", async () => {
    let calls = 0;
    stubFetch(() => (++calls < 3 ? json({}, 503) : json({ error: { code: "no_such_file" } }, 404)));
    await expect(client().files.resolve("f1")).rejects.toMatchObject({ status: 404, code: "no_such_file" });
    expect(calls).toBe(3);
  });

  it("does not replay a POST unless the caller opts in", async () => {
    const spy = stubFetch(() => json({}, 503));
    await expect(client().channels.create({ participants: ["a"], metadata: {} })).rejects.toBeInstanceOf(CloudHttpError);
    expect(spy).toHaveBeenCalledOnce();
  });

  it("hands back the last transient Response when raw fetch exhausts its retries", async () => {
    stubFetch(() => json({ error: "down" }, 502));
    const response = await client().fetch("/api/v1/agent/files");
    expect(response.status).toBe(502);
  });

  it("uploads presign → PUT → confirm with one signal", async () => {
    const seen: string[] = [];
    stubFetch((url, init) => {
      seen.push(`${init.method} ${url.host}${url.pathname}`);
      if (url.pathname.endsWith("/presign")) return json({ fileId: "f1", uploadUrl: "https://s3.test/put" });
      if (url.host === "s3.test") return new Response(null, { status: 200 });
      return json({ fileId: "f1", fileName: "a.txt", mimeType: "text/plain" });
    });
    const uploaded = await client().files.upload({ fileName: "a.txt", mimeType: "text/plain", data: new Uint8Array([1]) });
    expect(uploaded).toEqual({ fileId: "f1", uri: "beeos-file://f1", fileName: "a.txt", mimeType: "text/plain" });
    expect(seen).toEqual(["POST gateway.test/api/v1/agent/files/presign", "PUT s3.test/put",
      "POST gateway.test/api/v1/agent/files/confirm"]);
  });

  it("returns A2A status and RPC error instead of throwing so the host can map 401/403", async () => {
    const spy = stubFetch(() => json({ error: { code: -32600 } }, 401));
    const result = await client().a2a.rpc("agent 1", "SendMessage", { x: 1 });
    expect(new URL(String(spy.mock.calls[0]![0])).pathname).toBe("/api/v1/a2a/agent%201/jsonrpc");
    expect(result).toMatchObject({ ok: false, status: 401, rpc: { error: { code: -32600 } } });
  });

  it("bridge discovery degrades to undefined when the Gateway is unreachable", async () => {
    stubFetch(() => { throw new TypeError("fetch failed"); });
    expect(await client(false).bridge.config()).toBeUndefined();
  });

  it("builds automation routes, drops empty query values and sends the idempotency key on run", async () => {
    const seen: Array<[string, string, string | null]> = [];
    stubFetch((url, init) => {
      seen.push([String(init.method), url.pathname + url.search, new Headers(init.headers).get("idempotency-key")]);
      return json({ data: [] });
    });
    const gw = client();
    await gw.automations.list({ limit: 1, status: "" });
    await gw.automations.run("a/1", { sourceRunId: "r" }, "key-1");
    await gw.automations.runDetail("a/1", "r 2");
    expect(seen).toEqual([
      ["GET", "/api/v1/automations?limit=1", null],
      ["POST", "/api/v1/automations/a%2F1/runs", "key-1"],
      ["GET", "/api/v1/automations/a%2F1/runs/r%202", null],
    ]);
  });

  it("reports the template origin as a signed POST with the templateId body", async () => {
    const seen: Array<[string, string, unknown]> = [];
    stubFetch((url, init) => { seen.push([String(init.method), url.pathname, JSON.parse(String(init.body))]); return json({}); });
    await client().agents.reportTemplateOrigin("agent/1", "tpl-9");
    expect(seen).toEqual([["POST", "/api/v1/agents/agent%2F1/template-origin", { templateId: "tpl-9" }]]);
  });

  it("connector JSON-RPC call is never replayed and asks for JSON", async () => {
    let calls = 0;
    stubFetch((_url, init) => { calls += 1; expect(new Headers(init.headers).get("accept")).toBe("application/json");
      return json({}, 503); });
    await expect(client().connectors.callJson({ jsonrpc: "2.0", method: "tools/call" })).rejects.toBeInstanceOf(CloudHttpError);
    expect(calls).toBe(1);
  });

  it("normalizes the canvas token envelope and rejects an answer without a token", async () => {
    stubFetch(() => json({ data: { token: "t", relay_url: "wss://relay", expires_at: 9 } }));
    expect(await client().canvas.token()).toEqual({ token: "t", relayUrl: "wss://relay", expiresAt: 9 });
    stubFetch(() => json({ data: {} }));
    await expect(client().canvas.token()).rejects.toBeInstanceOf(CloudHttpError);
  });

  it("redeems an operation resource with encoded segments and never replays it", async () => {
    let calls = 0;
    const spy = stubFetch(() => { calls += 1; return new Response("bytes", { status: 503 }); });
    const response = await client().runtime.redeemResource("op/1", "skill", "ref 1", { a: 1 });
    expect(response.status).toBe(503);
    expect(calls).toBe(1);
    expect(new URL(String(spy.mock.calls[0]![0])).pathname)
      .toBe("/api/v1/internal/runtime/operations/op%2F1/resources/skill/ref%201/redeem");
  });
});
