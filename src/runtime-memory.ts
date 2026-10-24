import type { RuntimeLeaseCredential } from "./runtime-lease-http.js";
import { RuntimeLeaseHttp } from "./runtime-lease-http.js";

export type RuntimeMemoryAction = "remember" | "recall" | "revise" | "forget";
export interface RuntimeMemoryExecution {
  operation_id: string; request_message_id: string; native_request_hash: string; attempt_id: string;
}
export interface RuntimeMemoryArguments {
  remember: { content: string; kind: "core" | "semantic" | "episodic" | "procedural"; source_id?: string };
  recall: { query: string; kinds?: ("core" | "semantic" | "episodic" | "procedural")[]; limit?: number; cursor?: string };
  revise: { record_id: string; expected_revision: number; content: string; reason: string; source_id?: string };
  forget: { record_ids: string[]; reason: string };
}
export class RuntimeMemoryHttpError extends Error {
  constructor(readonly status: number, readonly code: string) { super(`native Memory unavailable (${status})`); this.name = "RuntimeMemoryHttpError"; }
}

/** Existing Memory domain under the current original execution. No local fallback\n * or automatic mutation retry; the owner derives idempotency from real tool ID. */
export class RuntimeMemory {
  constructor(private readonly http: RuntimeLeaseHttp) {}
  async call<A extends RuntimeMemoryAction>(action: A, credential: RuntimeLeaseCredential,
    execution: RuntimeMemoryExecution, toolCallId: string, args: RuntimeMemoryArguments[A], signal?: AbortSignal): Promise<Record<string, unknown>> {
    if (!credential.runtimeLeaseCredential || !credential.executionGrant ||
        !execution.operation_id || !execution.request_message_id || !execution.attempt_id ||
        !/^[a-f0-9]{64}$/.test(execution.native_request_hash) || !toolCallId || toolCallId.includes("\0") ||
        toolCallId.trim().length === 0 || [...toolCallId].length > 256 || !["remember", "recall", "revise", "forget"].includes(action)) {
      throw new Error("native Memory requires the current original authority and actual tool-call ID");
    }
    const body = { operation_id: execution.operation_id, request_message_id: execution.request_message_id,
      native_request_hash: execution.native_request_hash, attempt_id: execution.attempt_id, tool_call_id: toolCallId, arguments: args };
    const response = await this.http.postJson(`/api/v1/runtime/memory/${action}`, credential, body, { retry: false }, signal);
    const raw: unknown = await response.json();
    if (!response.ok) {
      const detail = raw && typeof raw === "object" && "error" in raw ? raw.error : undefined;
      const code = detail && typeof detail === "object" && "code" in detail && typeof detail.code === "string" ? detail.code : "memory_unavailable";
      throw new RuntimeMemoryHttpError(response.status, code);
    }
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new Error("native Memory returned no original receipt/page");
    return raw as Record<string, unknown>;
  }
}
