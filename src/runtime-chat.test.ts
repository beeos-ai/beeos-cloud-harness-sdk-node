import { afterEach, describe, expect, it, vi } from "vitest";

import {
  createRuntimeChatClient,
  normalizeMessage,
  type RuntimeChatAuthorityPort,
  type RuntimeChatHttpPort,
  type RuntimeChatLease,
  type RuntimeChatMessage,
  type RuntimeChatRealtimePort,
} from "./runtime-chat.js";

const NOW = Date.parse("2026-07-29T00:00:00.000Z");

afterEach(() => {
  vi.useRealTimers();
});

function lease(overrides: Partial<RuntimeChatLease> = {}): RuntimeChatLease {
  return {
    instanceId: "inst-bound-1",
    handlerIdentity: "device-agent:abc",
    runtimeEpoch: "7",
    leaseId: "lease-1",
    runtimeLeaseCredential: "cred-1",
    leaseExpiresAt: "2026-07-29T00:10:00.000Z",
    ...overrides,
  };
}

class FakeAuthority implements RuntimeChatAuthorityPort {
  current: RuntimeChatLease | null = lease();
  private readonly listeners = new Set<
    (lease: RuntimeChatLease | null) => void
  >();
  currentLease(): RuntimeChatLease | null {
    return this.current;
  }
  subscribeLease(listener: (lease: RuntimeChatLease | null) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  set(next: RuntimeChatLease | null): void {
    this.current = next;
    for (const listener of this.listeners) listener(next);
  }
}

type PersonalTokenResult = Awaited<
  ReturnType<RuntimeChatHttpPort["personalToken"]>
>;
type PatchInput = Parameters<RuntimeChatHttpPort["patchMessage"]>[1];
type OpenStreamInput = Parameters<RuntimeChatHttpPort["openStream"]>[1];

class FakeHttp implements RuntimeChatHttpPort {
  readonly calls: string[] = [];
  readonly tokenLeases: RuntimeChatLease[] = [];
  readonly patches: PatchInput[] = [];
  readonly streams: OpenStreamInput[] = [];
  openStreamError: unknown = null;
  personalTokenHook:
    | ((lease: RuntimeChatLease) => Promise<PersonalTokenResult>)
    | null = null;

  async personalToken(lease: RuntimeChatLease): Promise<PersonalTokenResult> {
    this.calls.push("personalToken");
    this.tokenLeases.push(lease);
    if (this.personalTokenHook) return await this.personalTokenHook(lease);
    return {
      token: `cent-${lease.leaseId}`,
      realtimeUrl: "wss://centrifugo.example/ws",
      channels: [`personal:instance:${lease.instanceId}`],
    };
  }
  async readInbox() {
    this.calls.push("readInbox");
    return { deliveries: [] };
  }
  async ackInbox() {
    this.calls.push("ackInbox");
  }
  async getHistory() {
    this.calls.push("getHistory");
    return {
      messages: [],
      hasMore: false,
      latestOffset: "0",
      historyGeneration: "0",
      historyBoundaryOffset: "0",
    };
  }
  async syncConversation() {
    this.calls.push("syncConversation");
    return {
      id: "conv-1",
      participants: [],
      metadata_version: 1,
      history_generation: 1,
      state: "open" as const,
      created_at: "2026-07-29T00:00:00.000Z",
    };
  }
  async sendMessage() {
    this.calls.push("sendMessage");
    return { messageId: "m-sent" };
  }
  async openStream(_lease: RuntimeChatLease, input: OpenStreamInput) {
    this.calls.push("openStream");
    this.streams.push(input);
    if (this.openStreamError) throw this.openStreamError;
    return { messageId: input.id };
  }
  async patchMessage(_lease: RuntimeChatLease, input: PatchInput) {
    this.calls.push("patchMessage");
    this.patches.push(input);
  }
}

interface FakeRealtimeSession {
  input: Parameters<RuntimeChatRealtimePort["open"]>[0];
  closed: boolean;
}

class FakeRealtime implements RuntimeChatRealtimePort {
  readonly sessions: FakeRealtimeSession[] = [];
  open(input: Parameters<RuntimeChatRealtimePort["open"]>[0]) {
    const session: FakeRealtimeSession = { input, closed: false };
    this.sessions.push(session);
    return {
      close: () => {
        session.closed = true;
      },
    };
  }
  latest(): FakeRealtimeSession {
    return this.sessions[this.sessions.length - 1]!;
  }
}

function build(reconnectDelayMs: (attempt: number) => number = () => 1) {
  const authority = new FakeAuthority();
  const http = new FakeHttp();
  const realtime = new FakeRealtime();
  const messages: RuntimeChatMessage[] = [];
  const disconnects: Array<{ code: number; reason: string }> = [];
  const errors: unknown[] = [];
  const client = createRuntimeChatClient({
    authority,
    http,
    realtime,
    now: () => NOW,
    reconnectDelayMs,
  });
  client.on("message", (message) => messages.push(message));
  client.on("disconnect", (info) => disconnects.push(info));
  client.on("error", (error) => errors.push(error));
  return { authority, http, realtime, client, messages, disconnects, errors };
}

describe("native runtime chat client", () => {
  it("normalizes inbound message.created, preserves unknown fields and dedupes", async () => {
    const { client, http, realtime, messages } = build();
    await client.connect();
    expect(http.calls).toContain("personalToken");
    expect(realtime.sessions[0]!.input.channel).toBe(
      "personal:instance:inst-bound-1",
    );

    const event = {
      type: "message.created",
      data: {
        message: {
          id: "m1",
          conversationId: "ch-1",
          type: "chat_message",
          content: { message: "hello" },
          senderId: "user:owner",
          unknown_field: "kept",
        },
      },
    };
    realtime.latest().input.onEvent(event);
    realtime.latest().input.onEvent(event);

    expect(messages).toHaveLength(1);
    expect(messages[0]).toMatchObject({
      id: "m1",
      conversationId: "ch-1",
      type: "chat_message",
      content: { message: "hello" },
      senderId: "user:owner",
      unknown_field: "kept",
    });
    await client.disconnect();
  });

  it("sends outbound messages with the active lease", async () => {
    const { client, http } = build();
    await client.connect();
    await client.messages.send({
      conversationId: "ch-1",
      type: "device_result",
      content: { ok: true },
    });
    expect(http.calls).toContain("sendMessage");
    await client.disconnect();
  });

  it("syncs a session/new conversation over the active lease", async () => {
    const { client, http } = build();
    await client.connect();
    const conversation = await client.syncConversation({
      schemaVersion: 1,
      operationId: "op-1",
      runtimeSessionId: "s-1",
      runtimeSessionKey: "k-1",
      title: "T",
    });
    expect(conversation.id).toBe("conv-1");
    expect(http.calls).toContain("syncConversation");
    await client.disconnect();
  });

  it("streams reply parts/body and commits terminal state", async () => {
    const { client, http } = build();
    await client.connect();
    const stream = client.messages.startStream({
      conversationId: "ch-1",
      id: "m-reply",
      type: "agent_reply",
      content: { session_key: "s" },
    });
    await stream.opened();
    stream.appendBody("hello");
    stream.addPart({ type: "text", text: "hello" });
    await stream.flush();
    expect(stream.parts).toEqual([{ type: "text", text: "hello" }]);
    await stream.finalize({ stopReason: "end_turn" });
    expect(stream.isTerminated).toBe(true);
    expect(http.calls).toContain("openStream");
    expect(
      http.calls.filter((call) => call === "patchMessage").length,
    ).toBeGreaterThanOrEqual(3);
    expect(() => stream.appendBody("late")).toThrow(/terminated/);
    await client.disconnect();
  });

  it("fails closed on lease loss and stops HTTP", async () => {
    const { authority, client, http, realtime, disconnects } = build();
    await client.connect();
    const callsBefore = http.calls.length;
    authority.set(null);
    await Promise.resolve();

    expect(realtime.latest().closed).toBe(true);
    expect(disconnects.at(-1)).toMatchObject({ reason: "runtime lease lost" });
    await expect(
      client.messages.send({ conversationId: "ch-1", type: "x", content: {} }),
    ).rejects.toThrow(/lease/);
    expect(http.calls.length).toBe(callsBefore);
    await client.disconnect();
  });

  it("reconnects after a transport failure and closes on dispose", async () => {
    const { client, http, realtime } = build(() => 1);
    await client.connect();
    const first = realtime.sessions.length;
    realtime.latest().input.onState("failed");
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(realtime.sessions.length).toBeGreaterThan(first);
    expect(
      http.calls.filter((call) => call === "personalToken").length,
    ).toBeGreaterThanOrEqual(2);
    client.dispose();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(realtime.latest().closed).toBe(true);
  });

  // Checklist 1: fail closed before HTTP on a missing/expired lease.
  it("issues no HTTP or WS for a missing or expired lease", async () => {
    const { authority, http, realtime, client } = build();
    authority.current = null;
    await client.connect();
    authority.current = lease({
      leaseExpiresAt: "2026-07-28T23:00:00.000Z",
    });
    await client.connect();

    expect(http.calls).toEqual([]);
    expect(realtime.sessions).toHaveLength(0);
    await client.disconnect();
  });

  // Checklist 1: an already-expired personal token must not open a stale WS.
  it("does not open a WS when the minted personal token is already expired", async () => {
    const { http, realtime, client } = build(() => 3_600_000);
    http.personalTokenHook = async (active) => ({
      token: `cent-${active.leaseId}`,
      realtimeUrl: "wss://centrifugo.example/ws",
      channels: [`personal:instance:${active.instanceId}`],
      expiresAt: NOW - 1_000,
    });

    await client.connect();

    expect(
      http.calls.filter((call) => call === "personalToken"),
    ).toHaveLength(1);
    expect(realtime.sessions).toHaveLength(0);
    await client.disconnect();
  });

  // Checklist 1: proactive re-auth/reconnect before token/lease expiry.
  it("schedules an unref'd proactive refresh before lease expiry", async () => {
    vi.useFakeTimers();
    const { authority, http, realtime, client } = build();
    authority.current = lease({
      leaseExpiresAt: new Date(NOW + 60_000).toISOString(),
    });

    await client.connect();
    expect(
      http.calls.filter((call) => call === "personalToken"),
    ).toHaveLength(1);
    expect(realtime.sessions).toHaveLength(1);

    // REFRESH_MARGIN is 30s, so a 60s lease refreshes at +30s.
    await vi.advanceTimersByTimeAsync(30_000);

    expect(
      http.calls.filter((call) => call === "personalToken").length,
    ).toBeGreaterThanOrEqual(2);
    expect(realtime.sessions.length).toBeGreaterThanOrEqual(2);
    expect(realtime.sessions[0]!.closed).toBe(true);
    await client.disconnect();
  });

  it("does not spin token requests when a lease is shorter than the refresh margin", async () => {
    vi.useFakeTimers();
    const { authority, http, client } = build();
    authority.current = lease({ leaseExpiresAt: new Date(NOW + 10_000).toISOString() });
    await client.connect();
    await vi.advanceTimersByTimeAsync(4_999);
    expect(http.tokenLeases).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(http.tokenLeases).toHaveLength(2);
    await client.disconnect();
  });

  // Checklist 2: a replaced lease epoch closes the old WS and opens a new one.
  it("replaces the realtime connection when the lease epoch changed", async () => {
    const { authority, http, realtime, client } = build();
    await client.connect();
    const first = realtime.sessions[0]!;
    expect(first.input.token).toBe("cent-lease-1");

    authority.set(
      lease({
        leaseId: "lease-2",
        runtimeEpoch: "8",
        runtimeLeaseCredential: "cred-2",
      }),
    );
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(first.closed).toBe(true);
    expect(realtime.sessions).toHaveLength(2);
    expect(realtime.sessions[1]!.input.token).toBe("cent-lease-2");
    expect(http.tokenLeases[1]!.leaseId).toBe("lease-2");
    await client.disconnect();
  });

  // Checklist 3: a lease change during personalToken discards the stale token
  // and re-runs open() for the current lease without wedging `opening`.
  it("discards an in-flight token when the lease changes and reopens", async () => {
    const { authority, http, realtime, client } = build();
    let releaseFirst!: () => void;
    const gate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let tokenCalls = 0;
    http.personalTokenHook = async (active) => {
      tokenCalls += 1;
      if (tokenCalls === 1) await gate;
      return {
        token: `cent-${active.leaseId}`,
        realtimeUrl: "wss://centrifugo.example/ws",
        channels: [`personal:instance:${active.instanceId}`],
      };
    };

    const connecting = client.connect();
    await Promise.resolve();
    expect(http.tokenLeases[0]!.leaseId).toBe("lease-1");

    authority.set(
      lease({
        leaseId: "lease-2",
        runtimeEpoch: "8",
        runtimeLeaseCredential: "cred-2",
      }),
    );
    releaseFirst();
    await connecting;
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(realtime.sessions).toHaveLength(1);
    expect(realtime.sessions[0]!.input.token).toBe("cent-lease-2");
    expect(realtime.sessions[0]!.closed).toBe(false);
    expect(http.tokenLeases.map((entry) => entry.leaseId)).toEqual([
      "lease-1",
      "lease-2",
    ]);
    await client.disconnect();
  });

  // Review fix 1: a token failure during a lease change is handled explicitly —
  // it emits `error` for the identified transport failure and renews through the
  // existing bounded reconnect path, never as an unhandled rejection.
  it("emits error and schedules a bounded reconnect when a lease-change token mint fails", async () => {
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandled);
    const attempts: number[] = [];
    const { authority, http, realtime, client, errors } = build((attempt) => {
      attempts.push(attempt);
      return 1;
    });
    try {
      await client.connect();
      expect(realtime.sessions).toHaveLength(1);

      http.personalTokenHook = async () => {
        throw new Error(
          "runtime chat request failed (503 Service Unavailable) /api/v1/runtime/realtime/token",
        );
      };
      authority.set(
        lease({
          leaseId: "lease-2",
          runtimeEpoch: "8",
          runtimeLeaseCredential: "cred-2",
        }),
      );

      await new Promise((resolve) => setTimeout(resolve, 20));

      expect(realtime.sessions[0]!.closed).toBe(true);
      expect(errors.length).toBeGreaterThanOrEqual(1);
      expect((errors[0] as Error).message).toMatch(/503 Service Unavailable/);
      // The existing bounded reconnect path was used, 1-based attempt.
      expect(attempts[0]).toBe(1);
      expect(unhandled).toHaveLength(0);
    } finally {
      process.off("unhandledRejection", onUnhandled);
      await client.disconnect();
    }
  });

  // Checklist 4: server-owned `disconnected` is terminal for that connection.
  it("renews when the server-owned connection reports disconnected", async () => {
    const { client, http, realtime, disconnects } = build(() => 1);
    await client.connect();
    const first = realtime.sessions[0]!;

    realtime.latest().input.onState("disconnected");

    expect(first.closed).toBe(true);
    expect(disconnects.at(-1)).toMatchObject({
      reason: "runtime chat transport disconnected",
    });
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(realtime.sessions.length).toBeGreaterThan(1);
    expect(
      http.calls.filter((call) => call === "personalToken").length,
    ).toBeGreaterThanOrEqual(2);
    await client.disconnect();
  });

  // Checklist 5: open failure propagates and never patches an uncreated message.
  it("propagates stream open failure and never patches an uncreated message", async () => {
    const { client, http } = build();
    await client.connect();
    http.openStreamError = new Error("open rejected");

    const openedStream = client.messages.startStream({
      conversationId: "ch-1",
      id: "m-open",
      type: "agent_reply",
      content: {},
    });
    await expect(openedStream.opened()).rejects.toThrow(/open rejected/);

    const flushStream = client.messages.startStream({
      conversationId: "ch-1",
      id: "m-flush",
      type: "agent_reply",
      content: {},
    });
    flushStream.appendBody("never sent");
    await expect(flushStream.flush()).rejects.toThrow(/open rejected/);

    const finalizeStream = client.messages.startStream({
      conversationId: "ch-1",
      id: "m-finalize",
      type: "agent_reply",
      content: {},
    });
    await expect(finalizeStream.finalize()).rejects.toThrow(/open rejected/);

    const failStream = client.messages.startStream({
      conversationId: "ch-1",
      id: "m-fail",
      type: "agent_reply",
      content: {},
    });
    await expect(failStream.fail({ body: "no" })).rejects.toThrow(
      /open rejected/,
    );

    expect(http.calls).toContain("openStream");
    expect(http.patches).toHaveLength(0);
    await client.disconnect();
  });

  // Checklist 6: bodyFrom is the UTF-8 byte length, not the JS char count.
  it("sends UTF-8 byte offsets as body_from for multi-byte bodies", async () => {
    const { client, http } = build();
    await client.connect();
    const stream = client.messages.startStream({
      conversationId: "ch-1",
      id: "m-utf8",
      type: "agent_reply",
      content: {},
    });
    await stream.opened();
    stream.appendBody("héllo😀");
    stream.appendBody("!");
    await stream.flush();

    const bodyPatches = http.patches.filter(
      (patch) => patch.bodyAppend !== undefined,
    );
    expect(bodyPatches).toHaveLength(2);
    expect(bodyPatches[0]!.bodyFrom).toBe(0);
    expect(bodyPatches[1]!.bodyFrom).toBe(Buffer.byteLength("héllo😀", "utf8"));
    expect(bodyPatches[1]!.bodyFrom).toBe(
      new TextEncoder().encode("héllo😀").byteLength,
    );
    expect(bodyPatches[1]!.bodyFrom).toBe(10);
    // 7 JS chars but 10 UTF-8 bytes: the offset must not be the char count.
    expect("héllo😀".length).toBe(7);
    await client.disconnect();
  });

  // Checklist 7: initial `content` is not the `body` column; first append is 0.
  it("starts the first append at offset 0 despite initial content", async () => {
    const { client, http } = build();
    await client.connect();
    const stream = client.messages.startStream({
      conversationId: "ch-1",
      id: "m-content",
      type: "agent_reply",
      content: { session_key: "s", text: "seed" },
    });
    await stream.opened();
    stream.appendBody("first");
    await stream.flush();

    expect(http.streams[0]!.content).toEqual({
      session_key: "s",
      text: "seed",
    });
    expect(http.patches[0]).toMatchObject({
      bodyAppend: "first",
      bodyFrom: 0,
    });
    await client.disconnect();
  });

  // Checklist 8: parts are stored verbatim (unknown nested fields survive).
  it("preserves unknown part fields verbatim on add and replace", async () => {
    const { client, http } = build();
    await client.connect();
    const stream = client.messages.startStream({
      conversationId: "ch-1",
      id: "m-parts",
      type: "agent_reply",
      content: {},
    });
    await stream.opened();

    const first = {
      type: "text",
      text: "a",
      meta: { trace: "x", nested: { deep: [1, 2] } },
      vendor_flag: true,
    };
    const second = {
      kind: "tool_call",
      payload: { name: "t", args: { q: 1 } },
      unknown: { a: null },
    };
    const replacement = {
      type: "text",
      text: "b",
      meta: { trace: "y" },
      extra: "kept",
    };

    stream.addPart(first);
    stream.addPart(second);
    stream.replacePart(0, replacement);
    await stream.flush();

    expect(stream.parts).toEqual([replacement, second]);
    const partsPatch = http.patches
      .filter((patch) => patch.parts !== undefined)
      .at(-1)!;
    expect(partsPatch.parts).toEqual([replacement, second]);
    expect(
      (partsPatch.parts as Array<{ payload?: { args?: { q?: number } } }>)[1]!
        .payload!.args!.q,
    ).toBe(1);
    await client.disconnect();
  });

  // Review fix 4: setBody replaces the body with a full snapshot PATCH (`body`)
  // on the same serialized queue, never a `body_append`.
  it("setBody PATCHes a full body snapshot and re-bases later appends", async () => {
    const { client, http } = build();
    await client.connect();
    const stream = client.messages.startStream({
      conversationId: "ch-1",
      id: "m-set-body",
      type: "agent_reply",
      content: {},
    });
    await stream.opened();
    stream.appendBody("partial");
    stream.setBody("replacement body");
    stream.appendBody("!");
    await stream.flush();

    const snapshots = http.patches.filter((patch) => patch.body !== undefined);
    expect(snapshots).toHaveLength(1);
    expect(snapshots[0]).toMatchObject({ body: "replacement body" });
    expect(snapshots[0]!.bodyAppend).toBeUndefined();
    expect(snapshots[0]!.bodyFrom).toBeUndefined();

    const appends = http.patches.filter((patch) => patch.bodyAppend !== undefined);
    expect(appends).toHaveLength(2);
    expect(appends[0]).toMatchObject({ bodyAppend: "partial", bodyFrom: 0 });
    expect(appends[1]).toMatchObject({
      bodyAppend: "!",
      bodyFrom: Buffer.byteLength("replacement body", "utf8"),
    });
    await client.disconnect();
  });

  // Review fix 4: addToolResult is a thin addPart alias; the tool-result part is
  // stored and PATCHed verbatim, unknown nested fields included.
  it("addToolResult appends the part verbatim with unknown fields preserved", async () => {
    const { client, http } = build();
    await client.connect();
    const stream = client.messages.startStream({
      conversationId: "ch-1",
      id: "m-tool-result",
      type: "agent_reply",
      content: {},
    });
    await stream.opened();
    const toolResult = {
      type: "tool_result",
      tool_use_id: "toolu_1",
      content: [{ type: "text", text: "42" }],
      is_error: false,
      vendor_extension: { trace: "t", nested: { deep: [1, 2, 3] } },
    };
    stream.addToolResult(toolResult);
    await stream.flush();

    expect(stream.parts).toEqual([toolResult]);
    const partsPatch = http.patches
      .filter((patch) => patch.parts !== undefined)
      .at(-1)!;
    expect(partsPatch.parts).toEqual([toolResult]);
    // Round-tripping the PATCH payload keeps the unknown fields untouched.
    expect(JSON.parse(JSON.stringify(partsPatch.parts))).toEqual([toolResult]);
    await client.disconnect();
  });
});

describe("runtime chat message normalization", () => {
  it("maps broker rows to normalized messages and preserves unknown fields", () => {
    expect(
      normalizeMessage({
        message_id: "m1",
        channel_id: "ch-1",
        type: "chat_message",
        payload: { message: "hi" },
        extra: 1,
      }),
    ).toMatchObject({
      id: "m1",
      conversationId: "ch-1",
      type: "chat_message",
      content: { message: "hi" },
      extra: 1,
    });
    expect(normalizeMessage({ type: "chat_message" })).toBeNull();
  });
});
