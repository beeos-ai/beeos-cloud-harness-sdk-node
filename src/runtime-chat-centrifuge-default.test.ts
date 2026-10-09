import type { AddressInfo } from "node:net";

import { afterEach, describe, expect, it, vi } from "vitest";
import WebSocket, { WebSocketServer } from "ws";

import { createCentrifugoRealtimePort } from "./runtime-chat-centrifuge.js";
import { createRuntimeChatClient, type RuntimeChatHttpPort, type RuntimeChatLease } from "./runtime-chat.js";

type WsSocket = WebSocket;

interface WireCapture {
  /** Raw client -> server frames, in order. */
  frames: string[];
  /** `Sec-WebSocket-Extensions` offered on each upgrade. */
  extensions: Array<string | undefined>;
}

function frameText(data: WebSocket.RawData): string {
  if (Buffer.isBuffer(data)) return data.toString("utf8");
  if (Array.isArray(data)) return Buffer.concat(data).toString("utf8");
  return Buffer.from(data).toString("utf8");
}

/**
 * A minimal Centrifugo server over the real `ws` package. It speaks just
 * enough of the JSON protocol to answer the client's `connect` command: the
 * reply is `{id, connect:{}}`, which is all `centrifuge` needs to reach the
 * `connected` state. It NEVER sends a server-side subscription, and it records
 * every frame so the test can prove no client `subscribe` is ever written.
 */
async function startCentrifugoStub(capture: WireCapture, autoAck = true): Promise<{
  url: string;
  close: () => Promise<void>;
  pending: Array<{ socket: WsSocket; id: number }>;
}> {
  const wss = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  const sockets = new Set<WsSocket>();
  const pending: Array<{ socket: WsSocket; id: number }> = [];
  await new Promise<void>((resolve, reject) => {
    wss.once("listening", () => resolve());
    wss.once("error", reject);
  });
  wss.on("connection", (socket, request) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    const offered = request.headers["sec-websocket-extensions"];
    capture.extensions.push(typeof offered === "string" ? offered : undefined);
    socket.on("message", (data) => {
      const text = frameText(data);
      capture.frames.push(text);
      for (const line of text.split("\n")) {
        if (!line.trim()) continue;
        let command: { id?: number; connect?: unknown };
        try {
          command = JSON.parse(line) as { id?: number; connect?: unknown };
        } catch {
          continue;
        }
        if (command.connect !== undefined) {
          pending.push({ socket, id: command.id! });
          if (autoAck) socket.send(JSON.stringify({ id: command.id, connect: {} }));
        }
      }
    });
  });
  const port = (wss.address() as AddressInfo).port;
  return {
    url: `ws://127.0.0.1:${port}`,
    pending,
    async close(): Promise<void> {
      for (const socket of sockets) socket.terminate();
      await new Promise<void>((resolve) => wss.close(() => resolve()));
    },
  };
}

function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const deadline = Date.now() + timeoutMs;
    const tick = (): void => {
      if (predicate()) {
        resolve();
        return;
      }
      if (Date.now() > deadline) {
        reject(new Error("timed out waiting for the Centrifugo connection"));
        return;
      }
      setTimeout(tick, 10);
    };
    tick();
  });
}

describe("default Centrifugo factory over a real ws wire", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("exposes authenticated state only after the real wire ACK, publications, reconnect and lease loss", async () => {
    const capture: WireCapture = { frames: [], extensions: [] };
    const server = await startCentrifugoStub(capture, false);
    let activeLease: RuntimeChatLease | null = {
      instanceId: "inst-1", handlerIdentity: "handler-1", runtimeEpoch: "7",
      leaseId: "lease-1", runtimeLeaseCredential: "test-lease",
      leaseExpiresAt: new Date(Date.now() + 600_000).toISOString(),
    };
    let leaseChanged: (() => void) | undefined;
    const http = {
      personalToken: async () => ({ token: "test-token", realtimeUrl: server.url, channels: ["personal:instance:inst-1"] }),
    } as RuntimeChatHttpPort;
    const client = createRuntimeChatClient({
      authority: {
        currentLease: () => activeLease,
        subscribeLease: (listener) => { leaseChanged = () => listener(activeLease); return () => { leaseChanged = undefined; }; },
      },
      http, realtime: createCentrifugoRealtimePort(), reconnectDelayMs: () => 10,
    });
    const states: string[] = [];
    const messages: unknown[] = [];
    client.on("connection", (state) => states.push(state));
    client.on("message", (message) => messages.push(message));
    client.on("error", () => undefined);
    const acknowledge = (index: number) => {
      const { socket, id } = server.pending[index]!;
      socket.send(JSON.stringify({ id, connect: { client: `client-${index}`, subs: { "personal:instance:inst-1": {} } } }));
    };
    try {
      await client.connect();
      await waitFor(() => server.pending.length === 1, 3_000);
      expect(client.connectionState).toBe("connecting");
      expect(states).not.toContain("connected");
      acknowledge(0);
      await waitFor(() => client.connectionState === "connected", 3_000);
      const inbound = { id: "msg-1", conversationId: "conversation-1", type: "chat_message", content: { parts: [{ type: "text", text: "hello" }] }, replyTo: "prior-1", custom: { preserved: true } };
      server.pending[0]!.socket.send(JSON.stringify({ push: { channel: "personal:instance:inst-1", pub: { data: { type: "message.created", data: inbound } } } }));
      await waitFor(() => messages.length === 1, 3_000);
      expect(messages[0]).toMatchObject(inbound);
      server.pending[0]!.socket.terminate();
      await waitFor(() => client.connectionState !== "connected", 3_000);
      await waitFor(() => server.pending.length === 2, 5_000);
      expect(client.connectionState).toBe("connecting");
      acknowledge(1);
      await waitFor(() => client.connectionState === "connected", 3_000);
      activeLease = null;
      leaseChanged!();
      expect(client.connectionState).toBe("disconnected");
      expect(capture.frames.some((frame) => frame.includes('"subscribe"'))).toBe(false);
    } finally {
      await client.disconnect();
      await server.close();
    }
  }, 15_000);

  it("never announces connected when the real server rejects authentication, or no lease exists", async () => {
    const capture: WireCapture = { frames: [], extensions: [] };
    const server = await startCentrifugoStub(capture, false);
    const personalToken = vi.fn(async () => ({ token: "rejected-test-token", realtimeUrl: server.url, channels: ["personal:instance:inst-1"] }));
    let activeLease: RuntimeChatLease | null = null;
    const client = createRuntimeChatClient({
      authority: { currentLease: () => activeLease },
      http: { personalToken } as unknown as RuntimeChatHttpPort,
      realtime: createCentrifugoRealtimePort(), reconnectDelayMs: () => 60_000,
    });
    const states: string[] = [];
    client.on("connection", (state) => states.push(state));
    client.on("error", () => undefined);
    try {
      await client.connect();
      expect(client.connectionState).toBe("disconnected");
      expect(personalToken).not.toHaveBeenCalled();
      activeLease = { instanceId: "inst-1", handlerIdentity: "handler-1", runtimeEpoch: "7", leaseId: "lease-1", runtimeLeaseCredential: "test-lease", leaseExpiresAt: new Date(Date.now() + 600_000).toISOString() };
      await client.connect();
      await waitFor(() => server.pending.length === 1, 3_000);
      const { socket, id } = server.pending[0]!;
      socket.send(JSON.stringify({ id, error: { code: 103, message: "permission denied" } }));
      await waitFor(() => client.connectionState === "disconnected", 3_000);
      expect(states).not.toContain("connected");
    } finally {
      await client.disconnect();
      await server.close();
    }
  }, 8_000);

  it("connects with the injected real ws transport and never sends a subscribe frame", async () => {
    const capture: WireCapture = { frames: [], extensions: [] };
    const server = await startCentrifugoStub(capture);
    // The default factory must pass the real `ws` implementation. If it ever
    // dropped that option, Centrifuge would fall back to the platform
    // constructor below and the client could not connect.
    const platformWebSocket = vi.fn(function platformWebSocket() {
      throw new Error("platform WebSocket must not be used");
    });
    vi.stubGlobal("WebSocket", platformWebSocket);

    const states: string[] = [];
    const errors: unknown[] = [];
    const port = createCentrifugoRealtimePort(); // NO injected factory
    const session = port.open({
      token: "lease-token",
      url: server.url,
      channel: "personal:instance:inst-1",
      onEvent: () => undefined,
      onState: (state) => states.push(state),
      onError: (error) => errors.push(error),
    });

    try {
      await waitFor(() => states.includes("connected"), 3_000);
      // Give any incorrect subscribe frame time to be written and captured.
      await new Promise((resolve) => setTimeout(resolve, 150));

      expect(platformWebSocket).not.toHaveBeenCalled();
      expect(states).toContain("connected");
      expect(errors).toEqual([]);

      // A real connect command travelled over the real socket with the token.
      const connectFrame = capture.frames.find((frame) =>
        frame.includes('"connect"'),
      );
      expect(connectFrame).toBeDefined();
      expect(JSON.parse(connectFrame!.split("\n")[0]!)).toMatchObject({
        connect: { token: "lease-token" },
      });

      // The injected ws client offers permessage-deflate on the handshake.
      expect(
        capture.extensions.some((value) =>
          value?.includes("permessage-deflate"),
        ),
      ).toBe(true);

      // Server-owned publications only: a client subscribe would be rejected.
      expect(capture.frames.some((frame) => frame.includes('"subscribe"'))).toBe(
        false,
      );
    } finally {
      session.close();
      await server.close();
    }
  }, 8_000);
});
