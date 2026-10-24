import { createHash } from "node:crypto";

/** Authoritative producer: docs/architecture/agent-state/16-honcho-v1-implementation.md. */
export interface StateContext {
  tenant_ref: string;
  scope_ref: string;
  authorization_revision: number;
  erasure_epoch: number;
  request_id: string;
}
export interface ChatIdentity {
  session_id: string;
  history_generation: string;
  branch_id: string;
}
export interface ChatWriter { writer_id: string; writer_epoch: string }
export interface NativeChatEvent {
  event_id: string;
  parent_id: string | null;
  kind: string;
  codec: { name: string; version: string };
  content_b64: string;
  sha256: string;
}
export interface ChatTerminal {
  response_id: string;
  status: "completed" | "failed" | "cancelled" | "incomplete";
  event_id: string;
}
export interface ChatLoadRequest extends ChatIdentity {
  context: StateContext;
  watermark: string | null;
  after: string;
  limit: number;
}
export interface ChatLoadResult extends ChatIdentity {
  watermark: string;
  events: (NativeChatEvent & { sequence: string })[];
  next_after: string;
  has_more: boolean;
  terminal: ChatTerminal | null;
  writer: ChatWriter;
}
export interface ChatAppendRequest extends ChatIdentity {
  context: StateContext;
  response_id: string;
  attempt_id: string;
  public_events: Record<string, unknown>[];
  writer: ChatWriter;
  expected_watermark: string;
  operation_id: string;
  idempotency_key: string;
  payload_sha256: string;
  events: NativeChatEvent[];
  terminal: ChatTerminal | null;
}
export interface ChatReceiptRequest extends ChatIdentity {
  context: StateContext;
  idempotency_key: string;
  payload_sha256: string;
}
export interface ChatReceipt extends ChatIdentity {
  receipt_id: string;
  operation_id: string;
  idempotency_key: string;
  payload_sha256: string;
  previous_watermark: string;
  read_watermark: string;
  event_ids: string[];
  terminal: ChatTerminal | null;
  writer: ChatWriter;
  committed_at: string;
}
export interface ChatCommitResult { receipt: ChatReceipt; replay: boolean }

/** Match the State producer's UTF-8, code-point-key-sorted compact JSON. */
export function chatAppendDigest(request: Omit<ChatAppendRequest, "payload_sha256"> | ChatAppendRequest): string {
  const { session_id, history_generation, branch_id, response_id, attempt_id, public_events,
    writer, expected_watermark, operation_id,
    idempotency_key, events, terminal } = request;
  const canonical = (value: unknown): string => {
    if (typeof value === "number" && !Number.isSafeInteger(value)) {
      throw new TypeError("chat envelope numbers must be JSON-safe integers");
    }
    if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
    if (value && typeof value === "object") {
      const compare = (a: string, b: string): number => {
        const left = Array.from(a, char => char.codePointAt(0)!);
        const right = Array.from(b, char => char.codePointAt(0)!);
        for (let i = 0; i < Math.min(left.length, right.length); i++) {
          if (left[i] !== right[i]) return left[i]! - right[i]!;
        }
        return left.length - right.length;
      };
      return `{${Object.keys(value).sort(compare).map(key =>
        `${JSON.stringify(key)}:${canonical((value as Record<string, unknown>)[key])}`).join(",")}}`;
    }
    if (value !== null && !["string", "number", "boolean"].includes(typeof value)) {
      throw new TypeError("chat envelope must contain JSON values");
    }
    return JSON.stringify(value);
  };
  return createHash("sha256").update(canonical({ session_id, history_generation,
    branch_id, response_id, attempt_id, public_events, writer, expected_watermark, operation_id,
    idempotency_key, events, terminal })).digest("hex");
}

export function encodeNativeChatEvent(input: { eventId: string; parentId: string | null; kind: string;
  codec: NativeChatEvent["codec"]; content: Uint8Array }): NativeChatEvent {
  return { event_id: input.eventId, parent_id: input.parentId, kind: input.kind, codec: input.codec,
    content_b64: Buffer.from(input.content).toString("base64"),
    sha256: createHash("sha256").update(input.content).digest("hex") };
}

export function decodeNativeChatEvent(event: NativeChatEvent): Uint8Array {
  if (typeof event.content_b64 !== "string" || typeof event.sha256 !== "string" ||
      !/^[0-9a-f]{64}$/.test(event.sha256)) throw new Error("invalid native event encoding");
  const bytes = Buffer.from(event.content_b64, "base64");
  if (bytes.toString("base64") !== event.content_b64 ||
      createHash("sha256").update(bytes).digest("hex") !== event.sha256) {
    throw new Error("native event bytes do not match their digest");
  }
  return bytes;
}
