/**
 * Lease-authenticated HTTP transport for {@link RuntimeChatHttpPort}.
 *
 * Endpoints are the verified native runtime-chat contract (g692
 * `/tmp/g692-runtime-chat-contract.md`, sha
 * 99dde631b29f3a33e2013eaee3720fe6263fe8b81e3ffc4047a9e32d2be86655):
 *
 *   POST  /api/v1/runtime/realtime/token
 *   POST  /api/v1/runtime/conversations/{conversationId}/messages
 *   PATCH /api/v1/runtime/conversations/{conversationId}/messages/{messageId}
 *
 * Every request carries ONLY the current runtime lease
 * (`Authorization: Bearer <runtimeLeaseCredential>`). There is no messaging
 * token, no gateway token provider, no shared runtime-delivery key, and no
 * legacy v2/v3 JWT path. The RPC/cron inbox stays in `runtime-delivery.ts`.
 */

import {
  normalizeMessage,
  type RuntimeChatHistoryMessage,
  type RuntimeChatHttpPort,
  type RuntimeChatLease,
  type RuntimeNativeConversationSyncResponse,
  type RuntimeChatRetainedSnapshot,
} from "./runtime-chat.js";
import { operationPath } from "./generated/operations.gen.js";
import type { RuntimeConversationSyncRequest } from "./generated/types.gen.js";

export const RUNTIME_CHAT_PATHS = Object.freeze({
  personalToken: operationPath("runtimeRealtimeTokenCreate"),
  conversations: operationPath("runtimeConversationSync"),
  conversationMessages: (conversationId: string) =>
    operationPath("runtimeConversationMessageCreate", { conversationId }),
  conversationMessage: (conversationId: string, messageId: string) =>
    operationPath("runtimeConversationMessagePatch", { conversationId, messageId }),
});

/** Envelope-owner exact typed error projection; only this discriminator is clear freeze. */
export interface RuntimeChatPreparedClearProof {
  clearIntentId: string;
  activeRunRegistrationId: string;
  historyGeneration: `${bigint}`;
  runtimeEpoch: `${bigint}`;
}
export class RuntimeChatPreparedClearError extends Error {
  constructor(readonly proof: RuntimeChatPreparedClearProof) {
    super("native original run is clearing");
  }
}

export interface RuntimeChatHttpPortOptions {
  /** Message Service origin, e.g. `https://message.example`. */
  baseUrl: string;
  fetchImpl?: typeof fetch;
  now?: () => number;
}

export function createRuntimeChatHttpPort(
  options: RuntimeChatHttpPortOptions,
): RuntimeChatHttpPort {
  const baseUrl = options.baseUrl.replace(/\/+$/, "");
  const now = options.now ?? Date.now;
  const fetchImpl = () => options.fetchImpl ?? globalThis.fetch;

  const request = async (
    lease: RuntimeChatLease,
    path: string,
    init: RequestInit,
  ): Promise<Response> => {
    assertLease(lease, now);
    const f = fetchImpl();
    if (typeof f !== "function") {
      throw new Error("runtime chat HTTP transport requires fetch");
    }
    const url = `${baseUrl}${path}`;
    const headers = new Headers(init.headers);
    headers.set("authorization", `Bearer ${lease.runtimeLeaseCredential}`);
    if (init.body !== undefined) {
      headers.set("content-type", "application/json");
    }
    const response = await f(url, { ...init, headers, redirect: "error" });
    if (response.url && new URL(response.url).origin !== new URL(baseUrl).origin) {
      throw new Error("runtime chat response origin changed");
    }
    if (!response.ok) {
      const body = await response.text().catch(() => "");
      if (response.status === 409) {
        let raw: unknown;
        try { raw = JSON.parse(body); } catch { /* ordinary malformed error, not proof */ }
        const error = raw && typeof raw === "object" && !Array.isArray(raw)
          ? (raw as { error?: unknown }).error : undefined;
        if (error && typeof error === "object" && !Array.isArray(error)) {
          const wire = error as { code?: unknown; metadata?: unknown };
          const metadata = wire.metadata;
          if (wire.code === "history_clear_in_progress" && metadata && typeof metadata === "object" &&
              !Array.isArray(metadata)) {
            const p = metadata as Record<string, unknown>;
            if (Object.keys(p).sort().join("|") ===
                "activeRunRegistrationId|clearIntentId|historyGeneration|runtimeEpoch" &&
                typeof p.clearIntentId === "string" && p.clearIntentId.length > 0 &&
                typeof p.activeRunRegistrationId === "string" && p.activeRunRegistrationId.length > 0 &&
                typeof p.historyGeneration === "string" && /^(0|[1-9][0-9]*)$/.test(p.historyGeneration) &&
                typeof p.runtimeEpoch === "string" && /^(0|[1-9][0-9]*)$/.test(p.runtimeEpoch)) {
              throw new RuntimeChatPreparedClearError(p as unknown as RuntimeChatPreparedClearProof);
            }
          }
        }
      }
      throw new Error(
        `runtime chat request failed (${response.status} ${response.statusText}) ${path}${
          body ? `: ${body.slice(0, 200)}` : ""
        }`,
      );
    }
    return response;
  };

  const json = async <T>(
    lease: RuntimeChatLease,
    path: string,
    init: RequestInit,
  ): Promise<T> => {
    const response = await request(lease, path, init);
    const raw: unknown = await response.json();
    if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
      throw new Error(`runtime chat response invalid: ${path}`);
    }
    return raw as T;
  };

  return {
    async getMessage(lease, input) {
      const response = await request(lease,
        RUNTIME_CHAT_PATHS.conversationMessage(input.conversationId, input.messageId),
        { method: "GET" });
      const rawText = await response.text();
      // This projection reads only known string fields. Parts stay in rawText.
      const raw: unknown = JSON.parse(rawText);
      if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
        throw new Error("runtime chat single snapshot invalid");
      }
      const row = raw as Record<string, unknown>;
      if (row.id !== input.messageId || row.conversation_id !== input.conversationId ||
          typeof row.type !== "string" || !row.type || typeof row.sender !== "string" || !row.sender ||
          typeof row.created_at !== "string" || !validSnapshotTimestamp(row.created_at) ||
          (row.updated_at !== undefined && (typeof row.updated_at !== "string" || !validSnapshotTimestamp(row.updated_at))) ||
          (row.reply_to !== undefined && (typeof row.reply_to !== "string" || !row.reply_to)) ||
          typeof row.body !== "string" || !["streaming", "completed", "failed", "refused", "cancelled"].includes(String(row.state)) ||
          (row.stop_reason !== undefined && typeof row.stop_reason !== "string")) {
        throw new Error("runtime chat single snapshot binding invalid");
      }
      return { rawText, messageId: row.id, conversationId: row.conversation_id,
        type: row.type, sender: row.sender, createdAt: row.created_at,
        ...(row.updated_at !== undefined ? { updatedAt: row.updated_at } : {}),
        ...(row.reply_to !== undefined ? { replyTo: row.reply_to } : {}), body: row.body, state: row.state,
        ...(row.stop_reason !== undefined ? { stopReason: row.stop_reason } : {})
      } as RuntimeChatRetainedSnapshot;
    },
    async personalToken(lease) {
      const raw = await json<Record<string, unknown>>(
        lease,
        RUNTIME_CHAT_PATHS.personalToken,
        { method: "POST", body: JSON.stringify({}) },
      );
      const token = typeof raw.token === "string" ? raw.token : "";
      const realtimeUrl =
        typeof raw.centrifugo_url === "string"
          ? raw.centrifugo_url
          : typeof raw.centrifugoUrl === "string"
            ? raw.centrifugoUrl
            : "";
      const channels = raw.channels;
      if (!token || !realtimeUrl || !Array.isArray(channels) || channels.length !== 1 ||
          channels[0] !== `personal:instance:${lease.instanceId}`) {
        throw new Error("runtime chat personal token response invalid");
      }
      return {
        token,
        realtimeUrl,
        channels,
        ...(typeof raw.principal_id === "string"
          ? { principalId: raw.principal_id }
          : {}),
        ...(typeof raw.expires_at === "number" ? { expiresAt: raw.expires_at } : {}),
      };
    },

    async syncConversation(lease, input) {
      const raw = await json<Record<string, unknown>>(
        lease,
        RUNTIME_CHAT_PATHS.conversations,
        {
          method: "POST",
          body: JSON.stringify({
            schemaVersion: 1,
            operationId: input.operationId,
            runtimeSessionId: input.runtimeSessionId,
            ...(input.runtimeSessionKey
              ? { runtimeSessionKey: input.runtimeSessionKey }
              : {}),
            ...(input.title ? { title: input.title } : {}),
          } satisfies RuntimeConversationSyncRequest),
        },
      );
      if (typeof raw.id !== "string" || !raw.id) {
        throw new Error("runtime conversation sync response invalid");
      }
      return raw as unknown as RuntimeNativeConversationSyncResponse;
    },

    async getHistory(lease, input) {
      const query = new URLSearchParams();
      if (input.cursor) query.set("cursor", input.cursor);
      if (input.limit !== undefined) query.set("limit", String(input.limit));
      const suffix = query.size > 0 ? `?${query.toString()}` : "";
      const raw = await json<Record<string, unknown>>(
        lease,
        `${RUNTIME_CHAT_PATHS.conversationMessages(input.conversationId)}${suffix}`,
        { method: "GET" },
      );
      if (!Array.isArray(raw.messages) || typeof raw.has_more !== "boolean" ||
          (raw.next_cursor !== undefined && typeof raw.next_cursor !== "string")) {
        throw new Error("runtime chat history page invalid");
      }
      const messages = raw.messages.map(historyMessage);
      return {
        messages,
        ...(typeof raw.next_cursor === "string" && raw.next_cursor
          ? { nextCursor: raw.next_cursor }
          : {}),
        hasMore: raw.has_more,
        latestOffset: decimal(raw.latest_offset, "latest_offset"),
        historyGeneration: decimal(raw.history_generation, "history_generation"),
        historyBoundaryOffset: decimal(raw.history_boundary_offset, "history_boundary_offset"),
      };
    },

    async sendMessage(lease, input) {
      const raw = await json<Record<string, unknown>>(
        lease,
        RUNTIME_CHAT_PATHS.conversationMessages(input.conversationId),
        { method: "POST", headers: { "Idempotency-Key": input.id }, body: JSON.stringify(v3Body(input)) },
      );
      return messageReceipt(raw, input);
    },

    async openStream(lease, input) {
      const raw = await json<Record<string, unknown>>(
        lease,
        RUNTIME_CHAT_PATHS.conversationMessages(input.conversationId),
        { method: "POST", headers: { "Idempotency-Key": input.id }, body: JSON.stringify({ ...v3Body(input), state: "streaming" }) },
      );
      return messageReceipt(raw, input);
    },

    async patchMessage(lease, input) {
      await request(
        lease,
        RUNTIME_CHAT_PATHS.conversationMessage(
          input.conversationId,
          input.messageId,
        ),
        {
          method: "PATCH",
          ...(input.operationKey ? { headers: { "Idempotency-Key": input.operationKey } } : {}),
          body: JSON.stringify({
            ...(input.body !== undefined ? { body: input.body } : {}),
            ...(input.bodyAppend !== undefined ? { body_append: input.bodyAppend } : {}),
            ...(input.bodyFrom !== undefined ? { body_from: input.bodyFrom } : {}),
            ...(input.parts !== undefined ? { parts: input.parts } : {}),
            ...(input.state !== undefined ? { state: input.state } : {}),
            ...(input.stopReason !== undefined ? { stop_reason: input.stopReason } : {}),
          }),
        },
      );
    },
  };
}

function validSnapshotTimestamp(value: string): boolean {
  // Go RFC3339Nano uses up to nine fractional digits; retain original text.
  return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/.test(value) &&
    Number.isFinite(Date.parse(value));
}

function v3Body(input: {
  id: string;
  type: string;
  content: unknown;
  replyTo?: string;
}): Record<string, unknown> {
  return {
    type: input.type,
    content: input.content,
    ...(input.replyTo ? { reply_to: input.replyTo } : {}),
  };
}

function messageReceipt(
  raw: Record<string, unknown>,
  expected: { id: string; conversationId: string; type: string },
): { messageId: string; outcome?: string } {
  if (typeof raw.id !== "string" || !raw.id || raw.id !== expected.id ||
      raw.conversation_id !== expected.conversationId || raw.type !== expected.type ||
      typeof raw.sender !== "string" || typeof raw.body !== "string" ||
      typeof raw.state !== "string" || typeof raw.created_at !== "string" || !raw.created_at ||
      (raw.idempotent !== undefined && typeof raw.idempotent !== "boolean")) {
    throw new Error("runtime chat message receipt invalid");
  }
  return {
    messageId: raw.id,
    outcome: raw.idempotent === true ? "duplicate" : "created",
  };
}

function assertLease(lease: RuntimeChatLease, now: () => number): void {
  if (!lease || !lease.runtimeLeaseCredential) {
    throw new Error("runtime chat lease unavailable");
  }
  const expiresAt = Date.parse(lease.leaseExpiresAt);
  if (!Number.isFinite(expiresAt) || expiresAt <= now()) {
    throw new Error("runtime chat lease expired");
  }
}

/**
 * Normalize a production `V2MessageResponse` row into the SDK history shape,
 * preserving all unknown fields.
 */
function historyMessage(value: unknown): RuntimeChatHistoryMessage {
  const record =
    value !== null && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : null;
  if (!record || ![record.id, record.conversation_id, record.type, record.created_at]
      .every((field) => typeof field === "string" && field.length > 0) ||
      typeof record.sender !== "string" || !("content" in record)) {
    throw new Error("runtime chat history message invalid");
  }
  const base = normalizeMessage(record)!;
  return {
    ...base,
    content: record.content,
    offset: decimal(record.offset, "message.offset"),
    historyGeneration: decimal(record.history_generation, "message.history_generation"),
    ...(typeof record.state === "string" ? { state: record.state } : {}),
    ...(typeof record.stop_reason === "string"
      ? { stopReason: record.stop_reason }
      : {}),
    ...(typeof record.body === "string" ? { body: record.body } : {}),
    ...(Array.isArray(record.parts) ? { parts: record.parts } : {}),
  };
}

function decimal(value: unknown, field: string): string {
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) {
    return String(value);
  }
  if (typeof value === "string" && /^\d+$/.test(value)) return value;
  throw new Error(`runtime chat history ${field} invalid`);
}
