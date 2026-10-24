import { describe, expect, it, vi } from "vitest";
import type { ChatAppendRequest, ChatLoadRequest } from "./chat-store-contract.js";
import { RuntimeLeaseHttp } from "./runtime-lease-http.js";
import { ChatStoreHttpError, ChatStoreOutcomeUnknown, RuntimeChatStore } from "./runtime-chat-store.js";

const credential = { runtimeLeaseCredential: "lease-1", executionGrant: "grant-1" };
const native: ChatAppendRequest = { context: { tenant_ref: "t1", scope_ref: "history1", authorization_revision: 1,
  erasure_epoch: 0, request_id: "request1" }, session_id: "s1", history_generation: "0", branch_id: "main",
  response_id: "response1", attempt_id: "attempt1", public_events: [], writer: { writer_id: "writer1", writer_epoch: "1" },
  expected_watermark: "0", operation_id: "operation1", idempotency_key: "original-key", payload_sha256: "a".repeat(64),
  events: [{ event_id: "event1", parent_id: null, kind: "compaction", codec: { name: "native", version: "1" },
    content_b64: "AA==", sha256: "b".repeat(64) }], terminal: null };
const load: ChatLoadRequest = { context: native.context, session_id: "s1", history_generation: "0", branch_id: "main",
  watermark: "8", after: "0", limit: 100 };
const committed = { receipt: { receipt_id: "receipt1", operation_id: native.operation_id, idempotency_key: native.idempotency_key,
  payload_sha256: native.payload_sha256, session_id: "s1", history_generation: "0", branch_id: "main", previous_watermark: "0",
  read_watermark: "1", event_ids: ["event1"], terminal: null, writer: native.writer, committed_at: "2026-10-24T00:00:00Z" }, replay: false };

describe("central native chat transport", () => {
  it("preserves native bytes, current authority and the original reconciliation identity", async () => {
    const send = vi.fn(async (_url: URL, _init?: RequestInit) => new Response(JSON.stringify(committed), { status: 200 }));
    const store = new RuntimeChatStore(new RuntimeLeaseHttp({ origin: "https://state.test", fetch: send as typeof fetch }));
    await store.load(credential, load);
    await store.append(credential, native);
    expect(await store.receipt(credential, native)).toEqual(committed.receipt);
    expect(send.mock.calls.map(([url]) => url.pathname)).toEqual([
      "/api/v1/runtime/chat-store/load", "/api/v1/runtime/chat-store/append", "/api/v1/runtime/chat-store/receipt"]);
    expect(JSON.parse(String(send.mock.calls[1]![1]!.body))).toEqual(native);
    const headers = new Headers(send.mock.calls[1]![1]!.headers);
    expect(headers.get("authorization")).toBe("Bearer lease-1");
    expect(headers.get("x-beeos-execution-grant")).toBe("grant-1");
    expect(send.mock.calls[1]![1]!.redirect).toBe("error");
  });

  it("does not retry ambiguous append even when the HTTP client has a default retry policy", async () => {
    const send = vi.fn(async () => new Response('{"error":{"code":"outcome_unknown"}}', { status: 503 }));
    const store = new RuntimeChatStore(new RuntimeLeaseHttp({ origin: "https://state.test", fetch: send,
      retry: { maxAttempts: 3, initialDelayMs: 1, maxDelayMs: 1, factor: 1 } }));
    await expect(store.append(credential, native)).rejects.toMatchObject({ name: "ChatStoreOutcomeUnknown", request: native });
    expect(send).toHaveBeenCalledOnce();
  });

  it.each(["network", "malformed", "wrong-shape"])("keeps %s append outcome unknown", async (kind) => {
    const send = vi.fn(async () => {
      if (kind === "network") throw new TypeError("connection reset");
      return new Response(kind === "malformed" ? "{broken" : "[]", { status: 200 });
    });
    const store = new RuntimeChatStore(new RuntimeLeaseHttp({ origin: "https://state.test", fetch: send }));
    await expect(store.append(credential, native)).rejects.toBeInstanceOf(ChatStoreOutcomeUnknown);
    expect(send).toHaveBeenCalledOnce();
  });

  it("keeps known authorization/fence conflicts visible and does not send without authority", async () => {
    const send = vi.fn(async () => new Response('{"error":{"code":"writer_fenced","message":"stale writer"}}', { status: 409 }));
    const store = new RuntimeChatStore(new RuntimeLeaseHttp({ origin: "https://state.test", fetch: send }));
    await expect(store.append(credential, native)).rejects.toBeInstanceOf(ChatStoreHttpError);
    await expect(store.load({ runtimeLeaseCredential: "lease-1" }, load)).rejects.toThrow("execution grant");
    expect(send).toHaveBeenCalledOnce();
  });
});
