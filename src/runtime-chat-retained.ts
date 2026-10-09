import type { RuntimeChatAuthorityPort, RuntimeChatHttpPort, RuntimeChatLease,
  RuntimeChatRetainedSnapshot } from "./runtime-chat.js";

import { RuntimeChatPreparedClearError, type RuntimeChatPreparedClearProof } from "./runtime-chat-http.js";

// Node 22 native JSON numeric source retention; local shape, never a global patch.
type RawJson = typeof JSON & {
  rawJSON(source: string): unknown;
  parse(text: string, reviver: (key: string, value: unknown,
    context?: { source?: string }) => unknown): unknown;
};
const nativeJson = JSON as RawJson;

/** Opaque content only. These numeric wrappers must never enter canonical JCS. */
function retainedParts(rawText: string): unknown[] {
  if (typeof nativeJson.rawJSON !== "function") throw new Error("native raw JSON required");
  const row = nativeJson.parse(rawText, (_key: string, value: unknown, context?: { source?: string }) => {
    if (typeof value !== "number") return value;
    if (!context?.source) throw new Error("native numeric JSON source required");
    return nativeJson.rawJSON(context.source);
  }) as { parts?: unknown };
  if (row.parts === undefined) return [];
  if (!Array.isArray(row.parts)) throw new Error("retained reply parts invalid");
  return row.parts;
}

export interface RuntimeChatRetainedBinding {
  conversationId: string;
  messageId: string;
  /** Original task request, distinct from the operation RPC request message. */
  taskRequestMessageId: string;
  /** Supplied by the trusted journal owner, never inferred from prompt content. */
  assertAttempt(): void;
  /** Stable idempotency key from the owning attempt for each serialized mutation. */
  mutationKey(sequence: number): string;
  /** Supplied only by the owner of a real MS Start ACK; rejects any mismatch. */
  assertPreparedClear?(proof: RuntimeChatPreparedClearProof): void;
}

/** Bind an MS-created streaming reply. This path never opens/posts a message. */
export async function bindExistingRuntimeChatReply(input: {
  authority: RuntimeChatAuthorityPort;
  http: RuntimeChatHttpPort;
  binding: RuntimeChatRetainedBinding;
  now?: () => number;
}): Promise<BoundRuntimeChatReply> {
  const stream = new BoundRuntimeChatReply(input);
  await stream.initialize();
  return stream;
}

export class BoundRuntimeChatReply {
  readonly id: string;
  private readonly captured: RuntimeChatLease;
  private readonly read: NonNullable<RuntimeChatHttpPort["getMessage"]>;
  private body = "";
  private rawParts: unknown[] = [];
  private sequence = 0;
  private chain: Promise<void> = Promise.resolve();
  private terminated = false;
  private previewFrozen = false;
  private snapshot!: RuntimeChatRetainedSnapshot;

  constructor(private readonly input: {
    authority: RuntimeChatAuthorityPort; http: RuntimeChatHttpPort;
    binding: RuntimeChatRetainedBinding; now?: () => number;
  }) {
    this.id = input.binding.messageId;
    const lease = input.authority.currentLease();
    if (!lease) throw new Error("retained reply lease unavailable");
    this.captured = { ...lease };
    if (!input.http.getMessage) throw new Error("single snapshot capability required");
    this.read = input.http.getMessage.bind(input.http);
    this.lease();
  }

  get isTerminated(): boolean { return this.terminated; }
  /** Count only: opaque existing parts never become a canonical JS projection. */
  get initialPartCount(): number { return this.initialParts; }
  private initialParts = 0;
  async initialize(): Promise<void> {
    this.snapshot = await this.getSnapshot();
    if (this.snapshot.state !== "streaming") throw new Error("retained reply not streaming");
    this.body = this.snapshot.body;
    this.rawParts = retainedParts(this.snapshot.rawText);
    this.initialParts = this.rawParts.length;
  }
  opened(): Promise<RuntimeChatRetainedSnapshot> { return Promise.resolve(this.snapshot); }
  flush(): Promise<void> { return this.chain; }
  /** Owning native bridge's final read: no new parser or canonical parts DTO. */
  assertTerminalSnapshot(row: RuntimeChatRetainedSnapshot): void {
    this.lease();
    const original = this.snapshot;
    if (!this.terminated || original.state === "streaming" ||
        row.messageId !== this.id || row.conversationId !== this.input.binding.conversationId ||
        row.type !== "agent_reply" || row.replyTo !== this.input.binding.taskRequestMessageId ||
        row.state !== original.state || row.body !== original.body || row.stopReason !== original.stopReason ||
        JSON.stringify(retainedParts(row.rawText)) !== JSON.stringify(retainedParts(original.rawText))) {
      throw new Error("retained original terminal snapshot changed");
    }
  }
  appendBody(chunk: string): void {
    this.assertActive(); if (this.previewFrozen) return;
    const bodyFrom = new TextEncoder().encode(this.body).byteLength;
    this.body += chunk;
    this.enqueue({ bodyAppend: chunk, bodyFrom });
  }
  setBody(body: string): void {
    this.assertActive(); if (this.previewFrozen) return;
    this.body = body; this.enqueue({ body });
  }
  addPart(part: unknown): void {
    this.assertActive(); if (this.previewFrozen) return;
    this.rawParts = [...this.rawParts, part];
    this.enqueue({ parts: [...this.rawParts] });
  }
  addToolResult(part: unknown): void { this.addPart(part); }
  replacePart(index: number, part: unknown): void {
    this.assertActive(); if (this.previewFrozen) return;
    if (!Number.isInteger(index) || index < 0 || index >= this.rawParts.length) {
      throw new Error("retained part index invalid");
    }
    this.rawParts = this.rawParts.map((old, position) => position === index ? part : old);
    this.enqueue({ parts: [...this.rawParts] });
  }
  finalize(options?: { stopReason?: string }): Promise<RuntimeChatRetainedSnapshot> {
    return this.terminate("completed", options);
  }
  fail(options?: { body?: string; stopReason?: string }): Promise<RuntimeChatRetainedSnapshot> {
    return this.terminate("failed", options);
  }
  refuse(options?: { stopReason?: string }): Promise<RuntimeChatRetainedSnapshot> {
    return this.terminate("refused", options);
  }
  cancel(options?: { stopReason?: string }): Promise<RuntimeChatRetainedSnapshot> {
    return this.terminate("cancelled", options);
  }
  private async terminate(state: string, options?: { body?: string; stopReason?: string }):
    Promise<RuntimeChatRetainedSnapshot> {
    this.assertActive(); this.terminated = true;
    await this.chain; this.lease();
    if (options?.body !== undefined) this.body = options.body;
    let mutationError: unknown;
    try { await this.write({ state, ...options }); }
    catch (error) { mutationError = error; }
    // Even successful HTTP acknowledgement is not a durable terminal receipt.
    const observed = await this.getSnapshot();
    const stateMatch = observed.state === state;
    let bodyMatch: boolean | "not_evaluated" = "not_evaluated";
    let stopReasonMatch: boolean | "not_evaluated" = "not_evaluated";
    let partsStringMatch: boolean | "not_evaluated" = "not_evaluated";
    if (stateMatch) bodyMatch = observed.body === this.body;
    if (bodyMatch === true) stopReasonMatch = observed.stopReason === options?.stopReason;
    if (stopReasonMatch === true) {
      partsStringMatch = JSON.stringify(retainedParts(observed.rawText)) ===
        JSON.stringify(this.rawParts);
    }
    if (stateMatch !== true || bodyMatch !== true || stopReasonMatch !== true ||
        partsStringMatch !== true) {
      if (mutationError) throw mutationError;
      throw new Error("retained reply terminal outcome unknown " +
        `stateMatch=${stateMatch} bodyMatch=${bodyMatch} ` +
        `stopReasonMatch=${stopReasonMatch} partsStringMatch=${partsStringMatch}`);
    }
    this.snapshot = observed;
    return observed;
  }
  private enqueue(patch: Partial<Parameters<RuntimeChatHttpPort["patchMessage"]>[1]>): void {
    this.chain = this.chain.then(async () => {
      if (this.previewFrozen) return;
      try { await this.write(patch); }
      catch (error) {
        const check = this.input.binding.assertPreparedClear;
        if (!(error instanceof RuntimeChatPreparedClearError) || !check) throw error;
        this.lease(); check(error.proof);
        const original = await this.getSnapshot();
        this.lease(); check(error.proof);
        // Only confirmed original-row refusal is reconciled; no 403/409 catch-all.
        this.snapshot = original;
        this.body = original.body;
        this.rawParts = retainedParts(original.rawText);
        this.previewFrozen = true;
      }
    });
    void this.chain.catch(() => {});
  }
  private async write(patch: Partial<Parameters<RuntimeChatHttpPort["patchMessage"]>[1]>):
    Promise<void> {
    const operationKey = this.input.binding.mutationKey(++this.sequence);
    if (!operationKey) throw new Error("retained mutation key required");
    const lease = this.lease();
    await this.input.http.patchMessage(lease, { ...patch,
      conversationId: this.input.binding.conversationId, messageId: this.id, operationKey });
    this.lease();
  }
  private async getSnapshot(): Promise<RuntimeChatRetainedSnapshot> {
    const lease = this.lease();
    const row = await this.read(lease, {
      conversationId: this.input.binding.conversationId, messageId: this.id });
    this.lease();
    if (row.messageId !== this.id || row.conversationId !== this.input.binding.conversationId ||
        row.type !== "agent_reply" || row.replyTo !== this.input.binding.taskRequestMessageId) {
      throw new Error("retained reply request binding mismatch");
    }
    return row;
  }
  private lease(): RuntimeChatLease {
    this.input.binding.assertAttempt();
    const lease = this.input.authority.currentLease();
    if (!lease || ["instanceId", "handlerIdentity", "runtimeEpoch", "leaseId"].some(
      key => lease[key as keyof RuntimeChatLease] !== this.captured[key as keyof RuntimeChatLease]) ||
      !lease.runtimeLeaseCredential || !Number.isFinite(Date.parse(lease.leaseExpiresAt)) ||
      Date.parse(lease.leaseExpiresAt) <= (this.input.now ?? Date.now)()) {
      throw new Error("retained reply lease fence changed or expired");
    }
    return lease;
  }
  private assertActive(): void {
    this.lease();
    if (this.terminated) throw new Error("retained reply terminated");
  }
}
