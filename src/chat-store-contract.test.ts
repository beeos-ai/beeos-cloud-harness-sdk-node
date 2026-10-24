import { describe, expect, it } from "vitest";
import { chatAppendDigest, decodeNativeChatEvent, encodeNativeChatEvent, type ChatAppendRequest } from "./chat-store-contract.js";

// Fixed against State's Python json_dump (sort_keys, ensure_ascii=False).
const request: Omit<ChatAppendRequest, "payload_sha256"> = {
  context: { tenant_ref: "t", scope_ref: "s", authorization_revision: 1, erasure_epoch: 0, request_id: "q" },
  session_id: "s1", history_generation: "1", branch_id: "main", response_id: "r1", attempt_id: "a1",
  public_events: [{ type: "response.output", metadata: { "2": "two", "10": "ten", "😀": "astral", "\ue000": "bmp" }, text: "你好\n" }],
  writer: { writer_id: "w1", writer_epoch: "7" }, expected_watermark: "0", operation_id: "op1", idempotency_key: "k1",
  events: [{ event_id: "native-1", parent_id: null, kind: "tool_result", codec: { name: "lossless", version: "1" },
    content_b64: "AP/lt6Xlhbfwn5iA", sha256: "2ae7304dff009a24a7c5e7fdcbb191169e3fff595c80cb65a3e38df378ca833e" }],
  terminal: { response_id: "r1", status: "completed", event_id: "native-1" },
};

describe("State native codec and cross-language digest", () => {
  it("matches the Python golden digest with numeric and non-BMP object keys", () => {
    expect(chatAppendDigest(request)).toBe("88cfad72a4f0d23b8f66c27aa52b6f86a4ea43e7176b24fc73d865c0638627a2");
    expect(chatAppendDigest({ ...request, context: { ...request.context, request_id: "fresh" } })).toBe(chatAppendDigest(request));
    expect(chatAppendDigest({ ...request, attempt_id: "another-attempt" })).not.toBe(chatAppendDigest(request));
  });
  it("retains arbitrary native bytes and detects corruption", () => {
    const event = request.events[0]!;
    expect(encodeNativeChatEvent({ eventId: event.event_id, parentId: null, kind: event.kind,
      codec: event.codec, content: decodeNativeChatEvent(event) })).toEqual(event);
    expect(() => decodeNativeChatEvent({ ...event, content_b64: event.content_b64 + "=" })).toThrow();
    expect(() => decodeNativeChatEvent({ ...event, sha256: "0".repeat(64) })).toThrow();
  });
  it("rejects inexact envelope numbers before signing", () => {
    expect(() => chatAppendDigest({ ...request, public_events: [{ duration: 0.5 }] })).toThrow();
    expect(() => chatAppendDigest({ ...request, public_events: [{ size: Number.MAX_SAFE_INTEGER + 1 }] })).toThrow();
  });
});
