import { describe, expect, it, vi } from "vitest";
import { RuntimeMemory, type RuntimeMemoryAction } from "./runtime-memory.js";
import { RuntimeLeaseHttp } from "./runtime-lease-http.js";
const execution = { operation_id: "op", request_message_id: "request", native_request_hash: "a".repeat(64), attempt_id: "attempt" };
const credential = { runtimeLeaseCredential: "lease", executionGrant: "fresh-grant" };

describe("native Memory transport", () => {
  it.each(["remember", "recall", "revise", "forget"] as RuntimeMemoryAction[])("preserves %s arguments, real tool ID and owner receipt", async action => {
    const result = { receipt_id: "original", read_watermark: "actual-scope-watermark" };
    const send = vi.fn(async (_url: URL, _init?: RequestInit) => Response.json(result));
    const client = new RuntimeMemory(new RuntimeLeaseHttp({ origin: "https://gateway.test", fetch: send as typeof fetch }));
    const args = action === "recall" ? { query: "记忆", limit: 3 } : { content: "value", kind: "semantic" as const };
    expect(await client.call(action, credential, execution, "provider:real/tool/😀", args)).toEqual(result);
    expect(send.mock.calls[0]![0].pathname).toBe(`/api/v1/runtime/memory/${action}`);
    expect(JSON.parse(String(send.mock.calls[0]![1]!.body))).toEqual({ ...execution, tool_call_id: "provider:real/tool/😀", arguments: args });
    expect(new Headers(send.mock.calls[0]![1]!.headers).get("x-beeos-execution-grant")).toBe("fresh-grant");
  });
  it("keeps source denial visible once and never retries uncertain mutations", async () => {
    const send = vi.fn(async () => Response.json({ error: { code: "source_unavailable" } }, { status: 503 }));
    const client = new RuntimeMemory(new RuntimeLeaseHttp({ origin: "https://gateway.test", fetch: send,
      retry: { maxAttempts: 3, initialDelayMs: 1, maxDelayMs: 1, factor: 1 } }));
    await expect(client.call("remember", credential, execution, "real-call", { content: "value", kind: "core" })).rejects.toMatchObject({ status: 503, code: "source_unavailable" });
    expect(send).toHaveBeenCalledOnce();
    await expect(client.call("recall", { runtimeLeaseCredential: "lease" }, execution, "real-call", { query: "q" })).rejects.toThrow("current original");
    expect(send).toHaveBeenCalledOnce();
  });
});
