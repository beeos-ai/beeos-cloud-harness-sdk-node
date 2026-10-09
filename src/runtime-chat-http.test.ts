import { afterEach, describe, expect, it, vi } from "vitest";

import type { RuntimeChatLease } from "./runtime-chat.js";
import {
  createRuntimeChatHttpPort,
  RUNTIME_CHAT_PATHS,
} from "./runtime-chat-http.js";

const NOW = Date.parse("2026-07-29T00:00:00.000Z");

function v3Message(id: string, state = "completed"): Record<string, unknown> {
  return { id, conversation_id: "ch-1", type: "agent_reply", sender: "inst-bound-1",
    body: "", state, created_at: new Date(NOW).toISOString() };
}

function lease(overrides: Partial<RuntimeChatLease> = {}): RuntimeChatLease {
  return {
    instanceId: "inst-bound-1",
    handlerIdentity: "device-agent:abc",
    runtimeEpoch: "7",
    leaseId: "lease-1",
    runtimeLeaseCredential: "cred-lease-1",
    leaseExpiresAt: "2026-07-29T00:10:00.000Z",
    ...overrides,
  };
}

interface Captured {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: unknown;
}

function stubFetch(captured: Captured[], responses: unknown[]): void {
  let index = 0;
  vi.stubGlobal("fetch", async (input: string | URL, init?: RequestInit) => {
    captured.push({
      url: String(input),
      method: init?.method ?? "GET",
      headers: Object.fromEntries(new Headers(init?.headers).entries()),
      body: init?.body ? JSON.parse(String(init.body)) : undefined,
    });
    return Response.json(responses[index++] ?? {});
  });
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("runtime chat native lease-auth HTTP transport", () => {
  const row = () => ({ id: "m1", conversation_id: "ch-1", type: "agent_reply",
    sender: "inst-bound-1", content: null, created_at: new Date(NOW).toISOString(),
    offset: 1, history_generation: 0, custom: { retained: true } });
  const page = () => ({ messages: [row()], has_more: false, latest_offset: 1,
    history_generation: 0, history_boundary_offset: 0 });

  it.each(["messages", "has_more", "latest_offset", "history_generation", "history_boundary_offset"])(
    "rejects a history page missing required %s", async (field) => {
      const malformed: Record<string, unknown> = page();
      delete malformed[field];
      stubFetch([], [malformed]);
      const port = createRuntimeChatHttpPort({ baseUrl: "https://message.example", now: () => NOW });
      await expect(port.getHistory(lease(), { conversationId: "ch-1" })).rejects.toThrow(/history.*invalid/);
    });

  it.each(["id", "conversation_id", "type", "sender", "content", "created_at", "offset", "history_generation"])(
    "rejects rather than drops a history message missing %s", async (field) => {
      const malformed: Record<string, unknown> = row();
      delete malformed[field];
      stubFetch([], [{ ...page(), messages: [row(), malformed] }]);
      const port = createRuntimeChatHttpPort({ baseUrl: "https://message.example", now: () => NOW });
      await expect(port.getHistory(lease(), { conversationId: "ch-1" })).rejects.toThrow(/history.*invalid/);
    });

  it.each([1.5, -1, Number.MAX_SAFE_INTEGER + 1, "1.5", "-1", null])(
    "rejects invalid or unsafe history decimals %s", async (value) => {
      stubFetch([], [{ ...page(), latest_offset: value },
        { ...page(), messages: [{ ...row(), offset: value }] }]);
      const port = createRuntimeChatHttpPort({ baseUrl: "https://message.example", now: () => NOW });
      await expect(port.getHistory(lease(), { conversationId: "ch-1" })).rejects.toThrow(/history.*invalid/);
      await expect(port.getHistory(lease(), { conversationId: "ch-1" })).rejects.toThrow(/history.*invalid/);
    });

  it("preserves exact large decimal strings, null content and unknown row fields", async () => {
    const large = "9223372036854775807";
    stubFetch([], [{ ...page(), latest_offset: large, history_generation: large,
      history_boundary_offset: large, messages: [{ ...row(), offset: large, history_generation: large }] }]);
    const port = createRuntimeChatHttpPort({ baseUrl: "https://message.example", now: () => NOW });
    const history = await port.getHistory(lease(), { conversationId: "ch-1" });
    expect(history).toMatchObject({ latestOffset: large, historyGeneration: large, historyBoundaryOffset: large,
      messages: [{ offset: large, historyGeneration: large, content: null, custom: { retained: true } }] });
  });

  it.each([[], ["personal:instance:other"], ["personal:instance:inst-bound-1", "extra"],
    ["personal:instance:inst-bound-1", 1], undefined])("rejects an unbound personal channel set %s", async (channels) => {
      stubFetch([], [{ token: "test-token", centrifugo_url: "wss://message.example/ws", channels }]);
      const port = createRuntimeChatHttpPort({ baseUrl: "https://message.example", now: () => NOW });
      await expect(port.personalToken(lease())).rejects.toThrow("personal token response invalid");
    });

  it.each([{}, { message_id: "m1" }, { ...v3Message("m1"), id: "" }, v3Message("other"),
    { ...v3Message("m1"), conversation_id: "other" }, { ...v3Message("m1"), idempotent: "true" }])(
    "rejects malformed or uncorrelated 201 create receipts %s", async (body) => {
      const port = createRuntimeChatHttpPort({ baseUrl: "https://message.example", now: () => NOW,
        fetchImpl: async () => Response.json(body, { status: 201 }) });
      const input = { conversationId: "ch-1", id: "m1", type: "agent_reply", content: {} };
      await expect(port.sendMessage(lease(), input)).rejects.toThrow("message receipt invalid");
      await expect(port.openStream(lease(), input)).rejects.toThrow("message receipt invalid");
    });

  it("reports the real v3 idempotent receipt as duplicate", async () => {
    stubFetch([], [{ ...v3Message("m1"), idempotent: true }]);
    const port = createRuntimeChatHttpPort({ baseUrl: "https://message.example", now: () => NOW });
    expect(await port.openStream(lease(), { conversationId: "ch-1", id: "m1", type: "agent_reply", content: {} }))
      .toEqual({ messageId: "m1", outcome: "duplicate" });
  });
  it("uses only the verified native endpoint constants", () => {
    expect(RUNTIME_CHAT_PATHS.personalToken).toBe(
      "/api/v1/runtime/realtime/token",
    );
    expect(RUNTIME_CHAT_PATHS.conversationMessages("ch-1")).toBe(
      "/api/v1/runtime/conversations/ch-1/messages",
    );
    expect(RUNTIME_CHAT_PATHS.conversationMessage("ch-1", "m-1")).toBe(
      "/api/v1/runtime/conversations/ch-1/messages/m-1",
    );
  });

  it("mints the personal token with the lease bearer only", async () => {
    const captured: Captured[] = [];
    stubFetch(captured, [
      {
        token: "cent-token",
        centrifugo_url: "wss://centrifugo.example/connection/websocket",
        channels: ["personal:instance:inst-bound-1"],
        principal_id: "inst-bound-1",
        expires_at: 1,
      },
    ]);
    const port = createRuntimeChatHttpPort({
      baseUrl: "https://message.example",
      now: () => NOW,
    });

    const token = await port.personalToken(lease());

    expect(token).toMatchObject({
      token: "cent-token",
      realtimeUrl: "wss://centrifugo.example/connection/websocket",
      channels: ["personal:instance:inst-bound-1"],
    });
    expect(captured[0]).toMatchObject({
      url: "https://message.example/api/v1/runtime/realtime/token",
      method: "POST",
      body: {},
    });
    expect(captured[0]!.headers.authorization).toBe("Bearer cred-lease-1");
    expect(captured[0]!.headers["x-runtime-delivery-key"]).toBeUndefined();
  });

  it("opens, patches and sends over the native lease conversation routes", async () => {
    const captured: Captured[] = [];
    stubFetch(captured, [v3Message("m1", "streaming"), {}, v3Message("m2")]);
    const port = createRuntimeChatHttpPort({
      baseUrl: "https://message.example",
      now: () => NOW,
    });

    await port.openStream(lease(), {
      conversationId: "ch-1",
      id: "m1",
      type: "agent_reply",
      content: { session_key: "s" },
      replyTo: "inbound-1",
    });
    await port.patchMessage(lease(), {
      conversationId: "ch-1",
      messageId: "m1",
      bodyAppend: "hello",
      bodyFrom: 0,
      parts: [{ type: "text", text: "hello" }],
      state: "completed",
      stopReason: "end_turn",
    });
    await port.sendMessage(lease(), {
      conversationId: "ch-1",
      id: "m2",
      type: "agent_reply",
      content: { text: "fallback" },
      replyTo: "inbound-1",
    });

    expect(captured[0]).toMatchObject({
      url: "https://message.example/api/v1/runtime/conversations/ch-1/messages",
      method: "POST",
      body: {
        type: "agent_reply",
        content: { session_key: "s" },
        reply_to: "inbound-1",
        state: "streaming",
      },
    });
    expect(captured[1]).toMatchObject({
      url: "https://message.example/api/v1/runtime/conversations/ch-1/messages/m1",
      method: "PATCH",
      body: {
        body_append: "hello",
        body_from: 0,
        parts: [{ type: "text", text: "hello" }],
        state: "completed",
        stop_reason: "end_turn",
      },
    });
    expect(captured[2]).toMatchObject({
      url: "https://message.example/api/v1/runtime/conversations/ch-1/messages",
      method: "POST",
      body: {
        type: "agent_reply",
        content: { text: "fallback" },
        reply_to: "inbound-1",
      },
    });
    for (const call of captured) {
      expect(call.headers.authorization).toBe("Bearer cred-lease-1");
    }
    expect(captured[0]!.headers["idempotency-key"]).toBe("m1");
    expect(captured[2]!.headers["idempotency-key"]).toBe("m2");
    expect(captured[0]!.body).not.toHaveProperty("id");
    expect(captured[2]!.body).not.toHaveProperty("id");
  });

  it("patches a full body snapshot as body, never body_append", async () => {
    const captured: Captured[] = [];
    stubFetch(captured, [{}]);
    const port = createRuntimeChatHttpPort({
      baseUrl: "https://message.example",
      now: () => NOW,
    });

    await port.patchMessage(lease(), {
      conversationId: "ch-1",
      messageId: "m1",
      body: "full replacement",
    });

    expect(captured[0]).toMatchObject({
      url: "https://message.example/api/v1/runtime/conversations/ch-1/messages/m1",
      method: "PATCH",
      body: { body: "full replacement" },
    });
    expect(
      (captured[0]!.body as Record<string, unknown>).body_append,
    ).toBeUndefined();
  });

  it("syncs session/new over the native lease conversation route", async () => {
    const captured: Captured[] = [];
    stubFetch(captured, [
      {
        id: "conv-1",
        participants: ["inst-bound-1", "agent:a"],
        metadata: { source: "session/new" },
        metadata_version: 1,
        history_generation: 1,
        state: "open",
        created_at: "2026-07-29T00:00:00.000Z",
      },
    ]);
    const port = createRuntimeChatHttpPort({
      baseUrl: "https://message.example",
      now: () => NOW,
    });

    const conversation = await port.syncConversation(lease(), {
      schemaVersion: 1,
      operationId: "op-1",
      runtimeSessionId: "s-1",
      runtimeSessionKey: "k-1",
      title: "T",
    });

    expect(captured[0]).toMatchObject({
      url: "https://message.example/api/v1/runtime/conversations",
      method: "POST",
      body: {
        schemaVersion: 1,
        operationId: "op-1",
        runtimeSessionId: "s-1",
        runtimeSessionKey: "k-1",
        title: "T",
      },
    });
    expect(captured[0]!.headers.authorization).toBe("Bearer cred-lease-1");
    expect(conversation.id).toBe("conv-1");
    expect(conversation.metadata).toEqual({ source: "session/new" });
  });

  it("reads native history recovery with the production V2MessagePage shape", async () => {
    const captured: Captured[] = [];
    stubFetch(captured, [
      {
        messages: [
          {
            id: "m1",
            conversation_id: "ch-1",
            type: "agent_reply",
            content: { session_key: "s" },
            sender: "agent:x",
            created_at: "2026-07-29T00:00:00.000Z",
            offset: 3,
            history_generation: 1,
            state: "completed",
            body: "hi",
            parts: [{ type: "text", text: "hi" }],
            unknown_field: "kept",
          },
        ],
        next_cursor: "c2",
        has_more: true,
        latest_offset: 5,
        history_generation: 1,
        history_boundary_offset: 2,
      },
    ]);
    const port = createRuntimeChatHttpPort({
      baseUrl: "https://message.example",
      now: () => NOW,
    });

    const page = await port.getHistory(lease(), {
      conversationId: "ch-1",
      cursor: "c1",
      limit: 50,
    });

    expect(captured[0]).toMatchObject({
      url: "https://message.example/api/v1/runtime/conversations/ch-1/messages?cursor=c1&limit=50",
      method: "GET",
    });
    expect(captured[0]!.headers.authorization).toBe("Bearer cred-lease-1");
    expect(page).toMatchObject({
      nextCursor: "c2",
      hasMore: true,
      latestOffset: "5",
      historyGeneration: "1",
      historyBoundaryOffset: "2",
    });
    expect(page.messages[0]).toMatchObject({
      id: "m1",
      conversationId: "ch-1",
      type: "agent_reply",
      content: { session_key: "s" },
      senderId: "agent:x",
      offset: "3",
      historyGeneration: "1",
      state: "completed",
      body: "hi",
      parts: [{ type: "text", text: "hi" }],
      unknown_field: "kept",
    });
  });

  it("sends nothing when the lease is expired", async () => {
    const captured: Captured[] = [];
    stubFetch(captured, []);
    const port = createRuntimeChatHttpPort({
      baseUrl: "https://message.example",
      now: () => NOW,
    });

    await expect(
      port.personalToken(lease({ leaseExpiresAt: "2026-07-28T23:00:00.000Z" })),
    ).rejects.toThrow(/lease/);
    expect(captured).toHaveLength(0);
  });
});
