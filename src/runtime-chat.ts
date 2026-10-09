/**
 * Native Cloud personal-chat client.
 *
 * This is the Cloud-SDK-owned replacement for the legacy
 * `@beeos-ai/message-sdk` personal chat client. It reuses the proven Message
 * Service wire (v2 conversation messages + v3 streaming parts, Centrifugo
 * personal channel events) but takes BOTH the HTTP transport and the
 * realtime transport as ports, with SDK-owned default lease HTTP and
 * Centrifugo adapters:
 *
 *   - `RuntimeChatHttpPort` is expected to authenticate every request with
 *     the current runtime lease (`Authorization: Bearer
 *     <runtimeLeaseCredential>`), never a gateway messaging token or the
 *     shared runtime delivery key.
 *   - `RuntimeChatRealtimePort` is the Centrifugo seam; the default Node
 *     adapter uses server-owned connection publications.
 *
 * Unknown fields on inbound messages are preserved; the facade only adds
 * normalized aliases (`id`, `conversationId`, `content`, ...).
 */

import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";

export interface RuntimeChatLease {
  instanceId: string;
  handlerIdentity: string;
  runtimeEpoch: string;
  leaseId: string;
  runtimeLeaseCredential: string;
  leaseExpiresAt: string;
}

export interface RuntimeChatAuthorityPort {
  currentLease(): RuntimeChatLease | null;
  subscribeLease?(
    listener: (lease: RuntimeChatLease | null) => void,
  ): () => void;
}

export interface RuntimeChatMessage {
  id: string;
  conversationId: string;
  type: string;
  content: unknown;
  senderId?: string;
  replyTo?: string;
  createdAt?: string;
  [key: string]: unknown;
}

export interface RuntimeChatSendInput {
  conversationId: string;
  id?: string;
  type: string;
  content: unknown;
  replyTo?: string;
}

export interface RuntimeChatReplyStream {
  readonly id: string;
  readonly isTerminated: boolean;
  readonly parts: readonly unknown[];
  opened(): Promise<unknown>;
  flush(): Promise<void>;
  appendBody(chunk: string): void;
  /** Replace the accumulated body with a full snapshot (PATCH `body`). */
  setBody(body: string): void;
  /** Alias for {@link addPart}: append a tool-result part verbatim. */
  addToolResult(part: unknown): void;
  addPart(part: unknown): void;
  replacePart(index: number, part: unknown): void;
  finalize(options?: { stopReason?: string }): Promise<unknown>;
  fail(options?: { body?: string; stopReason?: string }): Promise<unknown>;
  refuse(options?: { stopReason?: string }): Promise<unknown>;
  cancel(options?: { stopReason?: string }): Promise<unknown>;
}

export interface RuntimeChatHistoryMessage extends RuntimeChatMessage {
  offset?: string;
  historyGeneration?: string;
  state?: string;
  stopReason?: string;
  body?: string;
  parts?: readonly unknown[];
}

/**
 * Durable history page (`GET /api/v1/runtime/conversations/{id}/messages`),
 * mirroring the production `V2MessagePage`. `historyGeneration` +
 * `historyBoundaryOffset` are the recovery fence; `latestOffset` is only the
 * scan upper bound and must not be treated as the boundary.
 */
export interface RuntimeChatHistoryPage {
  messages: RuntimeChatHistoryMessage[];
  nextCursor?: string;
  hasMore: boolean;
  latestOffset: string;
  historyGeneration: string;
  historyBoundaryOffset: string;
}

export interface RuntimeChatHistoryInput {
  cursor?: string;
  limit?: number;
}

/**
 * Native `session/new` conversation sync (`POST /api/v1/runtime/conversations`).
 * The retained runtime operation is the authority; the body never carries an
 * owner, and the server derives the conversation owner from the operation.
 */
export interface RuntimeNativeConversationSyncRequest {
  schemaVersion: 1;
  operationId: string;
  runtimeSessionId: string;
  runtimeSessionKey?: string;
  title?: string;
}

/** Canonical `V2ConversationResponse` fields. */
export interface RuntimeNativeConversationSyncResponse {
  id: string;
  participants: readonly string[];
  metadata?: Readonly<Record<string, string>>;
  owner_identity_id?: string;
  target_identity_id?: string;
  target_kind?: string;
  title?: string;
  metadata_version: number;
  history_generation: number;
  last_activity_at?: string;
  state: "open" | "closed";
  closed_reason?: string;
  single_shot?: boolean;
  deadline_at?: string;
  created_at: string;
  closed_at?: string;
}

export interface RuntimeChatHttpPort {
  /** Single authoritative V3 snapshot. Original JSON remains opaque content. */
  getMessage?(
    lease: RuntimeChatLease,
    input: { conversationId: string; messageId: string },
  ): Promise<RuntimeChatRetainedSnapshot>;
  personalToken(lease: RuntimeChatLease): Promise<{
    token: string;
    realtimeUrl: string;
    channels: string[];
    principalId?: string;
    expiresAt?: number;
  }>;
  getHistory(
    lease: RuntimeChatLease,
    input: { conversationId: string } & RuntimeChatHistoryInput,
  ): Promise<RuntimeChatHistoryPage>;
  syncConversation(
    lease: RuntimeChatLease,
    input: RuntimeNativeConversationSyncRequest,
  ): Promise<RuntimeNativeConversationSyncResponse>;
  sendMessage(
    lease: RuntimeChatLease,
    input: {
      conversationId: string;
      id: string;
      type: string;
      content: unknown;
      replyTo?: string;
    },
  ): Promise<{ messageId: string; outcome?: string }>;
  openStream(
    lease: RuntimeChatLease,
    input: {
      conversationId: string;
      id: string;
      type: string;
      content: unknown;
      replyTo?: string;
    },
  ): Promise<{ messageId: string }>;
  patchMessage(
    lease: RuntimeChatLease,
    input: {
      conversationId: string;
      messageId: string;
      body?: string;
      bodyAppend?: string;
      bodyFrom?: number;
      parts?: readonly unknown[];
      state?: string;
      stopReason?: string;
      operationKey?: string;
    },
  ): Promise<void>;
}

export interface RuntimeChatRetainedSnapshot {
  rawText: string;
  messageId: string;
  conversationId: string;
  type: string;
  sender: string;
  createdAt: string;
  updatedAt?: string;
  replyTo?: string;
  body: string;
  state: string;
  stopReason?: string;
}

export interface RuntimeChatRealtimePort {
  open(input: {
    token: string;
    url: string;
    channel: string;
    onEvent(event: { type: string; data?: unknown }): void;
    onState(state: "connected" | "connecting" | "disconnected" | "failed"): void;
    onError(error: unknown): void;
  }): { close(): void };
}

export interface RuntimeChatLogger {
  debug?: (...args: unknown[]) => void;
  info?: (...args: unknown[]) => void;
  warn?: (...args: unknown[]) => void;
  error?: (...args: unknown[]) => void;
}

export interface RuntimeChatClientOptions {
  authority: RuntimeChatAuthorityPort;
  http: RuntimeChatHttpPort;
  realtime: RuntimeChatRealtimePort;
  now?: () => number;
  /** Bounded reconnect backoff; `attempt` is 1-based. */
  reconnectDelayMs?: (attempt: number) => number;
  logger?: RuntimeChatLogger;
}

export interface RuntimeChatClient {
  readonly connectionState: RuntimeChatConnectionState;
  readonly messages: {
    send(input: RuntimeChatSendInput): Promise<unknown>;
    startStream(input: RuntimeChatSendInput): RuntimeChatReplyStream;
  };
  /**
   * Durable history recovery for a known conversation id. There is no
   * "list conversations for this instance" route in the verified contract,
   * so a fresh start cannot discover unbound conversation ids from here.
   */
  history(
    conversationId: string,
    input?: RuntimeChatHistoryInput,
  ): Promise<RuntimeChatHistoryPage>;
  /**
   * Native `session/new` conversation sync over the active runtime lease. The
   * retained operation id is the authority; no owner/token is sent in the body.
   */
  syncConversation(
    input: RuntimeNativeConversationSyncRequest,
  ): Promise<RuntimeNativeConversationSyncResponse>;
  on(event: "message", listener: (message: RuntimeChatMessage) => void): this;
  on(event: "error", listener: (error: unknown) => void): this;
  on(event: "connection", listener: (state: RuntimeChatConnectionState) => void): this;
  on(
    event: "disconnect",
    listener: (info: { code: number; reason: string }) => void,
  ): this;
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  dispose(): void;
}

export type RuntimeChatConnectionState = "connected" | "connecting" | "disconnected" | "failed";

const DEFAULT_RECONNECT_DELAY_MS = (attempt: number): number =>
  Math.min(30_000, 500 * 2 ** Math.min(attempt - 1, 6));

/** Re-auth this long before the token/lease deadline so the session never lapses. */
const REFRESH_MARGIN_MS = 30_000;

/** Token expiries may be epoch seconds or epoch milliseconds; normalize to ms. */
function expiryMs(value: number | undefined): number | null {
  if (value === undefined || !Number.isFinite(value)) return null;
  return value < 1_000_000_000_000 ? value * 1000 : value;
}

function sameLeaseIdentity(
  a: RuntimeChatLease,
  b: RuntimeChatLease,
): boolean {
  return (
    a.instanceId === b.instanceId &&
    a.runtimeEpoch === b.runtimeEpoch &&
    a.leaseId === b.leaseId
  );
}

export function createRuntimeChatClient(
  options: RuntimeChatClientOptions,
): RuntimeChatClient {
  return new NativeRuntimeChatClient(options);
}

class NativeRuntimeChatClient implements RuntimeChatClient {
  private state: RuntimeChatConnectionState = "disconnected";
  private realtimeGeneration = 0;
  readonly messages: RuntimeChatClient["messages"];
  private readonly emitter = new EventEmitter();
  private realtime: { close(): void } | null = null;
  private unsubscribeLease: (() => void) | null = null;
  private running = false;
  private opening = false;
  private openRequested = false;
  private attempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private refreshTimer: ReturnType<typeof setTimeout> | null = null;
  /** Lease that the currently open WS was minted against. */
  private openedLease: RuntimeChatLease | null = null;
  private readonly seen = new Map<string, number>();
  private readonly seenOrder: string[] = [];
  private readonly seenLimit = 2048;

  constructor(private readonly opts: RuntimeChatClientOptions) {
    this.messages = {
      send: async (input) => await this.send(input),
      startStream: (input) => new NativeRuntimeChatReplyStream(this.opts, input),
    };
  }

  on(event: string, listener: (...args: any[]) => void): this {
    this.emitter.on(event, listener);
    return this;
  }

  get connectionState(): RuntimeChatConnectionState {
    return this.state;
  }

  private setState(state: RuntimeChatConnectionState): void {
    if (state === this.state) return;
    this.state = state;
    this.emitter.emit("connection", state);
  }

  async connect(): Promise<void> {
    this.running = true;
    this.unsubscribeLease ??= this.opts.authority.subscribeLease?.(() => {
      // Mirror the refresh/reconnect timer handling: a token/transport failure
      // during a lease change must surface as an `error` and renew through the
      // existing bounded reconnect path, never as an unhandled rejection.
      void this.onLeaseChanged().catch((error) => {
        this.emitError(error);
        this.scheduleReconnect();
      });
    }) ?? null;
    try {
      await this.open();
    } catch (error) {
      this.setState("failed");
      this.scheduleReconnect();
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    this.running = false;
    this.clearReconnect();
    this.closeRealtime();
    this.unsubscribeLease?.();
    this.unsubscribeLease = null;
  }

  dispose(): void {
    void this.disconnect();
    this.emitter.removeAllListeners();
  }

  private activeLease(): RuntimeChatLease | null {
    const lease = this.opts.authority.currentLease();
    if (!lease || !lease.runtimeLeaseCredential) return null;
    const expiresAt = Date.parse(lease.leaseExpiresAt);
    if (!Number.isFinite(expiresAt) || expiresAt <= this.now()) return null;
    return lease;
  }

  private async send(input: RuntimeChatSendInput): Promise<unknown> {
    const lease = this.activeLease();
    if (!lease) throw new Error("runtime chat lease unavailable");
    const id = input.id ?? randomUUID();
    const receipt = await this.opts.http.sendMessage(lease, {
      conversationId: input.conversationId,
      id,
      type: input.type,
      content: input.content,
      ...(input.replyTo ? { replyTo: input.replyTo } : {}),
    });
    return receipt;
  }

  async history(
    conversationId: string,
    input: RuntimeChatHistoryInput = {},
  ): Promise<RuntimeChatHistoryPage> {
    const lease = this.activeLease();
    if (!lease) throw new Error("runtime chat lease unavailable");
    return await this.opts.http.getHistory(lease, { conversationId, ...input });
  }

  async syncConversation(
    input: RuntimeNativeConversationSyncRequest,
  ): Promise<RuntimeNativeConversationSyncResponse> {
    const lease = this.activeLease();
    if (!lease) throw new Error("runtime chat lease unavailable");
    return await this.opts.http.syncConversation(lease, input);
  }

  private async open(): Promise<void> {
    if (!this.running) return;
    if (this.opening) {
      // A lease change arrived mid-flight: make sure we re-run for it once
      // the in-flight attempt settles instead of dropping the reopen.
      this.openRequested = true;
      return;
    }
    this.opening = true;
    try {
      do {
        this.openRequested = false;
        await this.openOnce();
      } while (this.openRequested && this.running);
    } finally {
      this.opening = false;
    }
    // A change that landed between the loop guard and clearing `opening`.
    if (this.openRequested && this.running) {
      this.openRequested = false;
      await this.open();
    }
  }

  /** Single lease/token acquisition attempt; sets `openRequested` when stale. */
  private async openOnce(): Promise<void> {
    if (this.realtime) return;
    const lease = this.activeLease();
    if (!lease) {
      // Fail closed: no lease, no HTTP request, no realtime session.
      this.opts.logger?.warn?.("runtime chat: no active lease — staying closed");
      this.setState("disconnected");
      return;
    }
    this.setState("connecting");
    const token = await this.opts.http.personalToken(lease);
    if (!this.running) return;
    const current = this.activeLease();
    if (!current || !sameLeaseIdentity(current, lease)) {
      // The lease was replaced (or lost) while the token was in flight. The
      // minted token belongs to a stale lease: discard it and re-run for
      // whatever the authority holds now.
      this.openRequested = true;
      return;
    }
    const tokenExpiry = expiryMs(token.expiresAt);
    if (tokenExpiry !== null && tokenExpiry <= this.now()) {
      // Never open a socket with an already-expired personal token.
      this.opts.logger?.warn?.(
        "runtime chat: personal token already expired — not opening",
      );
      this.scheduleReconnect();
      return;
    }
    const channel = token.channels[0] as string;
    const generation = ++this.realtimeGeneration;
    this.openedLease = lease;
    const realtime = this.opts.realtime.open({
      token: token.token,
      url: token.realtimeUrl,
      channel,
      onEvent: (event) => {
        if (generation === this.realtimeGeneration) this.handleEvent(event);
      },
      onState: (state) => {
        if (generation === this.realtimeGeneration) this.handleState(state);
      },
      onError: (error) => {
        if (generation === this.realtimeGeneration) this.emitError(error);
      },
    });
    if (generation !== this.realtimeGeneration || !this.running) {
      realtime.close();
      return;
    }
    this.realtime = realtime;
    this.scheduleRefresh(lease, token.expiresAt);
  }

  /** Exposed for tests/callers that use durable read/ack instead of push. */
  private async onLeaseChanged(): Promise<void> {
    if (!this.running) return;
    const lease = this.activeLease();
    if (!lease) {
      this.closeRealtime();
      this.emitDisconnect(0, "runtime lease lost");
      return;
    }
    if (
      this.realtime &&
      this.openedLease &&
      sameLeaseIdentity(this.openedLease, lease)
    ) {
      // Same lease identity: the live WS is still authoritative.
      return;
    }
    // Lease epoch/identity replaced (or no live WS): always tear the old
    // connection down and open one bound to the new lease.
    this.closeRealtime();
    if (this.opening) {
      this.openRequested = true;
      return;
    }
    await this.open();
  }

  private scheduleRefresh(
    lease: RuntimeChatLease,
    tokenExpiresAt?: number,
  ): void {
    this.clearRefresh();
    const leaseExpiry = Date.parse(lease.leaseExpiresAt);
    const tokenExpiry = expiryMs(tokenExpiresAt);
    let deadline = leaseExpiry;
    if (tokenExpiry !== null) {
      deadline = Number.isFinite(deadline)
        ? Math.min(deadline, tokenExpiry)
        : tokenExpiry;
    }
    if (!Number.isFinite(deadline)) return;
    const remaining = deadline - this.now();
    // A short lease must not create a zero-delay token/reconnect loop.
    const delay = Math.max(1, remaining - Math.min(REFRESH_MARGIN_MS, remaining / 2));
    this.refreshTimer = setTimeout(() => {
      this.refreshTimer = null;
      if (!this.running) return;
      // Proactive re-auth/reconnect ahead of token/lease expiry.
      this.closeRealtime();
      void this.open().catch((error) => {
        this.emitError(error);
        this.scheduleReconnect();
      });
    }, delay);
    this.refreshTimer.unref?.();
  }

  private clearRefresh(): void {
    if (this.refreshTimer) clearTimeout(this.refreshTimer);
    this.refreshTimer = null;
  }

  private handleEvent(event: { type: string; data?: unknown }): void {
    if (event.type !== "message.created") return;
    const message = normalizeMessage(
      asRecord(asRecord(event.data)?.message) ?? asRecord(event.data),
    );
    if (!message) return;
    if (!this.markSeen(message.id)) return;
    this.emitter.emit("message", message);
  }

  private handleState(
    state: "connected" | "connecting" | "disconnected" | "failed",
  ): void {
    if (state === "connected" || state === "connecting") {
      this.setState(state);
      if (state === "connected") this.attempt = 0;
      return;
    }
    // The server-owned connection is terminal for THIS connection: close it
    // and renew through the bounded backoff. A `disconnected` state must not
    // be treated as a benign, ignorable transition.
    this.closeRealtime();
    this.setState(state);
    this.emitDisconnect(
      0,
      state === "disconnected"
        ? "runtime chat transport disconnected"
        : "runtime chat transport failed",
    );
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (!this.running || this.reconnectTimer) return;
    const delay = (
      this.opts.reconnectDelayMs ?? DEFAULT_RECONNECT_DELAY_MS
    )(++this.attempt);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.open().catch((error) => {
        this.emitError(error);
        this.scheduleReconnect();
      });
    }, delay);
    this.reconnectTimer.unref?.();
  }

  private clearReconnect(): void {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  }

  private closeRealtime(): void {
    ++this.realtimeGeneration;
    this.setState("disconnected");
    this.clearRefresh();
    const realtime = this.realtime;
    this.realtime = null;
    this.openedLease = null;
    if (realtime) {
      try {
        realtime.close();
      } catch {
        /* observers never own teardown */
      }
    }
  }

  private emitError(error: unknown): void {
    this.emitter.emit("error", error);
  }

  private emitDisconnect(code: number, reason: string): void {
    this.emitter.emit("disconnect", { code, reason });
  }

  private markSeen(id: string): boolean {
    const now = this.now();
    while (
      this.seenOrder.length > 0 &&
      (this.seen.get(this.seenOrder[0] as string) ?? 0) <= now
    ) {
      const expired = this.seenOrder.shift() as string;
      this.seen.delete(expired);
    }
    if ((this.seen.get(id) ?? 0) > now) return false;
    this.seen.set(id, now + 300_000);
    this.seenOrder.push(id);
    if (this.seenOrder.length > this.seenLimit) {
      const expired = this.seenOrder.shift() as string;
      this.seen.delete(expired);
    }
    return true;
  }

  private now(): number {
    return this.opts.now ? this.opts.now() : Date.now();
  }
}

class NativeRuntimeChatReplyStream implements RuntimeChatReplyStream {
  private readonly messageId: string;
  private readonly openPromise: Promise<{ messageId: string } | null>;
  private body = "";
  private streamParts: unknown[] = [];
  private terminated = false;
  private sequence = 0;
  private chain: Promise<void>;

  constructor(
    private readonly opts: RuntimeChatClientOptions,
    private readonly input: RuntimeChatSendInput,
  ) {
    this.messageId = input.id ?? randomUUID();
    this.openPromise = this.open();
    this.chain = this.openPromise.then(
      () => undefined,
      () => undefined,
    );
  }

  get id(): string {
    return this.messageId;
  }

  get isTerminated(): boolean {
    return this.terminated;
  }

  get parts(): readonly unknown[] {
    return this.streamParts;
  }

  async opened(): Promise<unknown> {
    return await this.openPromise;
  }

  async flush(): Promise<void> {
    // Propagate the open failure: a stream that never opened has nothing to
    // flush, and the caller must be able to observe why.
    await this.openPromise;
    await this.chain;
  }

  appendBody(chunk: string): void {
    this.assertActive();
    if (!chunk) return;
    const bodyFrom = new TextEncoder().encode(this.body).byteLength;
    this.body += chunk;
    this.enqueue((lease) =>
      this.opts.http.patchMessage(lease, {
        conversationId: this.input.conversationId,
        messageId: this.messageId,
        bodyAppend: chunk,
        bodyFrom,
      }),
    );
  }

  setBody(body: string): void {
    this.assertActive();
    // Full snapshot, not an append: same serialized queue as appendBody so a
    // setBody never overtakes an earlier append on the wire.
    this.body = body;
    this.enqueue((lease) =>
      this.opts.http.patchMessage(lease, {
        conversationId: this.input.conversationId,
        messageId: this.messageId,
        body,
      }),
    );
  }

  addToolResult(part: unknown): void {
    // Thin alias: tool results are ordinary parts, stored verbatim.
    this.addPart(part);
  }

  addPart(part: unknown): void {
    this.assertActive();
    this.streamParts = [...this.streamParts, part];
    this.enqueueParts();
  }

  replacePart(index: number, part: unknown): void {
    this.assertActive();
    if (!Number.isInteger(index) || index < 0 || index >= this.streamParts.length) {
      throw new Error("runtime chat reply part index is out of range");
    }
    const parts = [...this.streamParts];
    parts[index] = part;
    this.streamParts = parts;
    this.enqueueParts();
  }

  async finalize(options?: { stopReason?: string }): Promise<unknown> {
    return await this.terminate("completed", options?.stopReason, undefined);
  }

  async fail(options?: { body?: string; stopReason?: string }): Promise<unknown> {
    return await this.terminate("failed", options?.stopReason, options?.body);
  }

  async refuse(options?: { stopReason?: string }): Promise<unknown> {
    return await this.terminate("refused", options?.stopReason, undefined);
  }

  async cancel(options?: { stopReason?: string }): Promise<unknown> {
    return await this.terminate("cancelled", options?.stopReason, undefined);
  }

  private async open(): Promise<{ messageId: string } | null> {
    const lease = this.activeLease();
    if (!lease) throw new Error("runtime chat lease unavailable");
    const receipt = await this.opts.http.openStream(lease, {
      conversationId: this.input.conversationId,
      id: this.messageId,
      type: this.input.type,
      content: this.input.content,
      ...(this.input.replyTo ? { replyTo: this.input.replyTo } : {}),
    });
    return receipt;
  }

  private enqueueParts(): void {
    const parts = [...this.streamParts];
    this.enqueue((lease) =>
      this.opts.http.patchMessage(lease, {
        conversationId: this.input.conversationId,
        messageId: this.messageId,
        parts,
      }),
    );
  }

  private async terminate(
    state: "completed" | "failed" | "refused" | "cancelled",
    stopReason: string | undefined,
    body: string | undefined,
  ): Promise<unknown> {
    this.assertActive();
    this.terminated = true;
    // If the message was never created there is no row to patch: reject with
    // the open error instead of issuing a PATCH against a missing message.
    await this.openPromise;
    await this.chain;
    const lease = this.activeLease();
    if (!lease) throw new Error("runtime chat lease unavailable");
    if (body !== undefined) this.body = body;
    await this.opts.http.patchMessage(lease, {
      conversationId: this.input.conversationId,
      messageId: this.messageId,
      ...(body !== undefined ? { body } : {}),
      state,
      ...(stopReason ? { stopReason } : {}),
    });
    return this.openPromise as unknown;
  }

  private enqueue(write: (lease: RuntimeChatLease) => Promise<void>): void {
    this.sequence += 1;
    const sequence = this.sequence;
    this.chain = this.chain.then(async () => {
      // Gate every write on a successful open so a failed open never leaks a
      // PATCH for a message that does not exist.
      await this.openPromise;
      const lease = this.activeLease();
      if (!lease) throw new Error("runtime chat lease unavailable");
      void sequence;
      await write(lease);
    });
    void this.chain.catch(() => undefined);
  }

  private activeLease(): RuntimeChatLease | null {
    const lease = this.opts.authority.currentLease();
    if (!lease || !lease.runtimeLeaseCredential) return null;
    const expiresAt = Date.parse(lease.leaseExpiresAt);
    const now = this.opts.now ? this.opts.now() : Date.now();
    if (!Number.isFinite(expiresAt) || expiresAt <= now) return null;
    return lease;
  }

  private assertActive(): void {
    if (this.terminated) throw new Error("runtime chat reply stream is terminated");
  }
}

/** Preserve unknown inbound fields; add the normalized aliases handlers use. */
export function normalizeMessage(value: unknown): RuntimeChatMessage | null {
  const record = asRecord(value);
  if (!record) return null;
  const id =
    stringField(record, "id") ||
    stringField(record, "message_id") ||
    stringField(record, "messageId");
  if (!id) return null;
  const conversationId =
    stringField(record, "conversationId") ||
    stringField(record, "conversation_id") ||
    stringField(record, "channel_id") ||
    stringField(record, "channelId");
  const type = stringField(record, "type");
  const senderId =
    stringField(record, "senderId") ||
    stringField(record, "sender_id") ||
    stringField(record, "sender") ||
    stringField(record, "publisher_id");
  const replyTo =
    stringField(record, "replyTo") ||
    stringField(record, "reply_to") ||
    stringField(record, "in_reply_to");
  const createdAt =
    stringField(record, "createdAt") ||
    stringField(record, "created_at");
  return {
    ...record,
    id,
    conversationId,
    type,
    content: record.content ?? record.payload ?? {},
    ...(senderId ? { senderId } : {}),
    ...(replyTo ? { replyTo } : {}),
    ...(createdAt ? { createdAt } : {}),
  };
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function stringField(
  record: Record<string, unknown>,
  key: string,
): string {
  const value = record[key];
  return typeof value === "string" ? value : "";
}
