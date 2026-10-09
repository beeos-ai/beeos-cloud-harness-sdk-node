import { createHash, randomBytes } from "node:crypto";
import type {
  RuntimeCapability,
  RuntimeHeartbeatRequest,
  RuntimeHeartbeatResult,
  RuntimeInvocationMethod,
  RuntimeRegistrationHashPayload,
  RuntimeRegistrationRequest,
  RuntimeRegistrationResult,
  RuntimeHeartbeatHashPayload,
} from "@beeos-ai/beeos-types/runtime";
import {
  RUNTIME_HASH_DOMAINS,
  RUNTIME_RPC_PROTOCOL_VERSION,
  canonicalizeJcs,
  runtimeHashPreimage,
  runtimeIdentitySignatureMessage,
  validateRuntimeInternalRegistrationResult,
} from "@beeos-ai/beeos-types/runtime";
import { RuntimeTransportFailurePolicy } from "./runtime-transport-failure-policy.js";
import { delayForAttempt, type BackoffPolicy } from "./retry.js";
import type { AgentGatewayClient } from "./gateway-client.js";
import { operationPath } from "./generated/operations.gen.js";
import type { MSConnectionStatus } from "@beeos-ai/beeos-types/runtime";

export interface RuntimeRegistrationTransport {
  register(request: RuntimeRegistrationRequest): Promise<RuntimeRegistrationResult>;
  heartbeat(request: RuntimeHeartbeatRequest): Promise<RuntimeHeartbeatResult>;
}

export interface RuntimeActiveLease {
  instanceId: string;
  leaseId: string;
  runtimeEpoch: `${bigint}`;
  runtimeLeaseCredential: string;
  leaseExpiresAt: string;
}

/** Ephemeral authority view for lease-bound data planes. Credentials must never be journaled or logged. */
export interface RuntimeRegistrationAuthorityLease extends RuntimeActiveLease {
  handlerIdentity: string;
}

export interface RuntimeRegistrationAuthoritySource {
  readonly authorityLease: RuntimeRegistrationAuthorityLease | null;
  subscribeAuthority(listener: (lease: RuntimeRegistrationAuthorityLease | null) => void): () => void;
}

export interface RuntimeRegistrationConfig {
  registrationId: string;
  handlerIdentity: string;
  contractRevision: string;
  runtimeMethods: readonly RuntimeInvocationMethod[];
  capabilities: readonly RuntimeCapability[];
  manifestDigest: RuntimeRegistrationHashPayload["manifestDigest"];
  journalStoreId: string;
  journalGeneration: `${bigint}`;
  instanceIdentityKeyId: string;
  /** Explicit Cloud workload target; absent preserves Legacy proof bytes. */
  targetInstanceId?: string;
  /** Key custody stays with the host; sign the canonical Ed25519 proof bytes. */
  signIdentityProof(message: Uint8Array): Uint8Array;
  /**
   * A `fenced` answer is recoverable: the authority may fence a registration
   * while a predecessor or the control plane settles, so the coordinator keeps
   * re-registering the same intent on this backoff. `false` restores a terminal
   * fence. Default: 30 s doubling to 5 min, unbounded.
   */
  fencedRetry?: BackoffPolicy | false;
  getMSConnectionStatus?: (lease: RuntimeActiveLease) => MSConnectionStatus;
}

const DEFAULT_FENCED_RETRY: BackoffPolicy = Object.freeze({
  maxAttempts: Number.POSITIVE_INFINITY, initialDelayMs: 30_000, maxDelayMs: 300_000, factor: 2,
});

export class RuntimeRegistrationLeaseExpiredError extends Error {
  constructor() { super("Instance authority returned an expired active lease");
    this.name = "RuntimeRegistrationLeaseExpiredError"; }
}

/** Prior lease still owns the instance; wait for leaseTTL then re-register the same intent. */
export class RuntimeRegistrationHandoffRequiredError extends Error {
  readonly retryAfterMs: number;
  constructor(retryAfterMs = 65_000) {
    super("Instance authority requires waiting for the active runtime lease to expire");
    this.name = "RuntimeRegistrationHandoffRequiredError";
    this.retryAfterMs = retryAfterMs;
  }
}

/**
 * Backend refuses to honor the requested journal store/lineage without an
 * explicit owner reset authorization. This is a terminal fence, not a
 * handoff-wait: retrying the same registration intent loops forever, so
 * callers must surface it for operator intervention instead of scheduling
 * another attempt.
 */
export class RuntimeRegistrationOwnerResetRequiredError extends Error {
  constructor() {
    super("Instance authority requires owner reset authorization after a journal store change");
    this.name = "RuntimeRegistrationOwnerResetRequiredError";
  }
}

/**
 * Lease already expired: register must use Current.journalGeneration+1.
 * Callers must roll durable identity (exactly once) before retrying — never
 * treat this as a lease-handoff wait with the same generation.
 */
export class RuntimeRegistrationJournalAdvanceRequiredError extends Error {
  /** Authority Current.journalGeneration when the conflict body exposes it. */
  readonly currentJournalGeneration?: `${bigint}`;
  /** Required next generation (= Current+1) when the conflict body exposes it. */
  readonly expectedJournalGeneration?: `${bigint}`;
  constructor(detail?: {
    currentJournalGeneration?: `${bigint}`;
    expectedJournalGeneration?: `${bigint}`;
  }) {
    super("Instance authority requires journal generation to advance exactly once after lease expiry");
    this.name = "RuntimeRegistrationJournalAdvanceRequiredError";
    this.currentJournalGeneration = detail?.currentJournalGeneration;
    this.expectedJournalGeneration = detail?.expectedJournalGeneration;
  }
}

/** Owns runtime registration and stops mutation claims immediately on lease loss. */
export class RuntimeRegistrationCoordinator {
  private active: RuntimeActiveLease | null = null;
  private timer?: ReturnType<typeof setTimeout>;
  private expiryTimer?: ReturnType<typeof setTimeout>;
  private stopped = false;
  private fencedAttempts = 0;
  /** Fences every async registration/heartbeat continuation across stop/start. */
  private generation = 0;
  /**
   * Transient Agent Gateway / ALB 5xx must not immediately drop the local lease.
   * Clearing on first 502 while the control-plane lease is still active produces
   * register→409 handoff thrash and leaves Gateway accepting 202s nobody claims
   * (P1 model-switch: 202 without conversation.model_changed).
   */
  private readonly heartbeatTransportFailurePolicy = new RuntimeTransportFailurePolicy();
  private readonly authorityListeners = new Set<(lease: RuntimeRegistrationAuthorityLease | null) => void>();

  constructor(
    private readonly transport: RuntimeRegistrationTransport,
    private readonly config: RuntimeRegistrationConfig,
    private readonly onLeaseLost: (reason: "fenced" | "expired" | "heartbeat_failed", cause?: unknown) => void,
    private readonly now: () => number = Date.now,
  ) {}

  get lease(): RuntimeActiveLease | null {
    if (!this.active || Date.parse(this.active.leaseExpiresAt) <= this.now()) return null;
    return this.active;
  }

  get canClaimMutations(): boolean { return this.lease !== null; }

  get authorityLease(): RuntimeRegistrationAuthorityLease | null {
    const lease = this.lease;
    return lease ? Object.freeze({ ...lease, handlerIdentity: this.config.handlerIdentity }) : null;
  }

  subscribeAuthority(listener: (lease: RuntimeRegistrationAuthorityLease | null) => void): () => void {
    this.authorityListeners.add(listener);
    return () => { this.authorityListeners.delete(listener); };
  }

  async start(): Promise<RuntimeActiveLease | null> {
    const generation = ++this.generation;
    this.stopped = false;
    return await this.registerOnce(generation);
  }

  private async registerOnce(generation: number): Promise<RuntimeActiveLease | null> {
    if (!this.isCurrent(generation)) return null;
    this.timer = undefined;
    if (this.active) {
      this.clearLeaseExpiry();
      this.active = null;
      this.notifyAuthority();
    }
    const payload: RuntimeRegistrationHashPayload = {
      registrationId: this.config.registrationId,
      handlerIdentity: this.config.handlerIdentity,
      contractRevision: this.config.contractRevision,
      runtimeRpcProtocolVersion: RUNTIME_RPC_PROTOCOL_VERSION,
      runtimeMethods: this.config.runtimeMethods,
      capabilities: this.config.capabilities,
      manifestDigest: this.config.manifestDigest,
      journalStoreId: this.config.journalStoreId,
      journalGeneration: this.config.journalGeneration,
    };
    let result: RuntimeRegistrationResult;
    try { result = await this.transport.register(this.signProof("runtime.register", payload)); }
    catch (error) {
      if (!this.isCurrent(generation)) return null;
      // Journal-advance and lease-expired both require a durable roll before any
      // retry — scheduling registerOnce with the same journalGeneration loops forever.
      // Owner-reset is a terminal fence for the same reason: no roll or backoff
      // recovers it, so it must never fall through to scheduleRegistration below.
      if (error instanceof RuntimeRegistrationLeaseExpiredError ||
          error instanceof RuntimeRegistrationJournalAdvanceRequiredError ||
          error instanceof RuntimeRegistrationOwnerResetRequiredError) {
        this.onLeaseLost(
          error instanceof RuntimeRegistrationLeaseExpiredError ? "expired" : "heartbeat_failed",
          error,
        );
        return null;
      }
      this.onLeaseLost("heartbeat_failed", error);
      const retryAfterMs = error instanceof RuntimeRegistrationHandoffRequiredError
        ? error.retryAfterMs
        : 5_000;
      this.scheduleRegistration(retryAfterMs, generation);
      return null;
    }
    // stop() may race an in-flight registration request.  Never resurrect an
    // authority lease (or its heartbeat timers) from that late response.
    if (!this.isCurrent(generation)) return null;
    if (result.status !== "active") {
      if (result.status === "fenced") {
        this.onLeaseLost("fenced");
        this.scheduleFencedRegistration(generation);
      } else this.scheduleRegistration(result.retryAfterMs, generation);
      return null;
    }
    if (this.config.targetInstanceId !== undefined &&
        result.instanceId !== this.config.targetInstanceId) {
      this.onLeaseLost("fenced", new Error("Instance authority returned a different Cloud instance"));
      return null;
    }
    this.active = {
      instanceId: result.instanceId,
      leaseId: result.leaseId,
      runtimeEpoch: result.runtimeEpoch,
      runtimeLeaseCredential: result.runtimeLeaseCredential,
      leaseExpiresAt: result.leaseExpiresAt,
    };
    this.heartbeatTransportFailurePolicy.recordSuccess();
    this.fencedAttempts = 0;
    this.notifyAuthority();
    this.scheduleLeaseExpiry(this.active, generation);
    this.schedule(result.heartbeatIntervalMs, generation);
    return this.active;
  }

  stop(): void {
    this.generation++;
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    if (this.expiryTimer) clearTimeout(this.expiryTimer);
    this.timer = undefined;
    this.expiryTimer = undefined;
    this.active = null;
    this.notifyAuthority();
  }

  private schedule(intervalMs: number, generation: number): void {
    if (!this.isCurrent(generation)) return;
    const active = this.active;
    if (!active) return;
    this.timer = setTimeout(() => { void this.sendHeartbeat(intervalMs, generation, active); }, intervalMs);
    this.timer.unref?.();
  }

  private scheduleFencedRegistration(generation: number): void {
    const policy = this.config.fencedRetry === undefined ? DEFAULT_FENCED_RETRY : this.config.fencedRetry;
    if (policy === false || this.fencedAttempts + 1 >= policy.maxAttempts) return;
    this.fencedAttempts += 1;
    this.scheduleRegistration(delayForAttempt(policy, this.fencedAttempts), generation);
  }

  private scheduleRegistration(intervalMs: number, generation: number): void {
    if (!this.isCurrent(generation)) return;
    // Deliberately not unref'd: this is the handoff/soft-retry heal timer, and an
    // otherwise-idle event loop must not let the process exit (or a bundler GC the
    // timer) mid-backoff — that would strand the Op on a lease that never recovers.
    this.timer = setTimeout(() => { void this.registerOnce(generation); }, intervalMs);
  }

  private async sendHeartbeat(
    intervalMs: number,
    generation: number,
    active: RuntimeActiveLease,
  ): Promise<void> {
    if (!this.isCurrentLease(generation, active)) return;
    const payload: RuntimeHeartbeatHashPayload = {
      leaseId: active.leaseId,
      handlerIdentity: this.config.handlerIdentity,
      runtimeEpoch: active.runtimeEpoch,
    };
    const heartbeatPayload = this.config.getMSConnectionStatus
      ? { ...payload, msConnectionStatus: this.config.getMSConnectionStatus(active) }
      : payload;
    try {
      const result = await this.transport.heartbeat(this.signProof("runtime.heartbeat", heartbeatPayload));
      // A stop/start, registration replacement, or newer heartbeat may have
      // taken ownership while the request was in flight.  Every late outcome
      // is inert, including expired/fenced results.
      if (!this.isCurrentLease(generation, active)) return;
      if (result.status !== "renewed") {
        this.heartbeatTransportFailurePolicy.recordSuccess();
        this.active = null;
        this.clearLeaseExpiry();
        this.notifyAuthority();
        this.onLeaseLost(result.status);
        if (result.status === "fenced") this.scheduleFencedRegistration(generation);
        return;
      }
      this.heartbeatTransportFailurePolicy.recordSuccess();
      this.active = { ...active, runtimeLeaseCredential: result.runtimeLeaseCredential,
        leaseExpiresAt: result.leaseExpiresAt };
      this.notifyAuthority();
      this.scheduleLeaseExpiry(this.active, generation);
      this.schedule(intervalMs, generation);
    } catch (error) {
      if (!this.isCurrentLease(generation, active)) return;
      // Soft-retry transient transport failures (ALB/AG 502/504) while keeping
      // the local lease so claim stays live and we avoid register handoff races
      // against a still-active control-plane lease.
      if (!this.heartbeatTransportFailurePolicy.recordFailure()) {
        this.schedule(Math.min(intervalMs, 5_000), generation);
        return;
      }
      this.active = null;
      this.clearLeaseExpiry();
      this.notifyAuthority();
      this.onLeaseLost("heartbeat_failed", error);
      this.scheduleRegistration(5_000, generation);
    }
  }

  private scheduleLeaseExpiry(lease: RuntimeActiveLease, generation: number): void {
    this.clearLeaseExpiry();
    const expected = `${lease.leaseId}\0${lease.runtimeEpoch}\0${lease.leaseExpiresAt}`;
    const schedule = () => {
      const remaining = Date.parse(lease.leaseExpiresAt) - this.now();
      if (remaining > 0) {
        this.expiryTimer = setTimeout(schedule, Math.min(2_147_483_647, remaining));
        this.expiryTimer.unref?.();
        return;
      }
      const active = this.active;
      if (!this.isCurrentLease(generation, lease) || !active ||
          `${active.leaseId}\0${active.runtimeEpoch}\0${active.leaseExpiresAt}` !== expected) return;
      this.active = null;
      this.expiryTimer = undefined;
      this.notifyAuthority();
      this.onLeaseLost("expired");
    };
    schedule();
  }

  private clearLeaseExpiry(): void {
    if (this.expiryTimer) clearTimeout(this.expiryTimer);
    this.expiryTimer = undefined;
  }

  private isCurrent(generation: number): boolean {
    return !this.stopped && generation === this.generation;
  }

  private isCurrentLease(generation: number, lease: RuntimeActiveLease): boolean {
    return this.isCurrent(generation) && this.active === lease;
  }

  private notifyAuthority(): void {
    const snapshot = this.authorityLease;
    for (const listener of this.authorityListeners) {
      try { listener(snapshot); } catch { /* observers cannot alter registration authority state */ }
    }
  }

  private signProof<TPurpose extends "runtime.register" | "runtime.heartbeat", TPayload extends object>(
    purpose: TPurpose,
    payload: TPayload,
  ): TPayload & {
    signaturePurpose: TPurpose; instanceIdentityKeyId: string; nonce: string; signedAt: string;
    payloadHash: `${string}`; signature: string; targetInstanceId?: string;
  } {
    const signedAt = new Date(this.now()).toISOString();
    const nonce = randomBytes(18).toString("base64url");
    const domain = purpose === "runtime.register" ? RUNTIME_HASH_DOMAINS.registration : RUNTIME_HASH_DOMAINS.heartbeat;
    const payloadHash = createHash("sha256").update(runtimeHashPreimage(domain,
      payload as unknown as Parameters<typeof canonicalizeJcs>[0])).digest("hex") as `${string}`;
    const message = runtimeIdentitySignatureMessage({ purpose,
      instanceIdentityKeyId: this.config.instanceIdentityKeyId,
      targetInstanceId: this.config.targetInstanceId, signedAt, nonce, payloadHash });
    return { ...payload, signaturePurpose: purpose,
      instanceIdentityKeyId: this.config.instanceIdentityKeyId, signedAt, nonce, payloadHash,
      ...(this.config.targetInstanceId === undefined ? {} : { targetInstanceId: this.config.targetInstanceId }),
      signature: Buffer.from(this.config.signIdentityProof(message)).toString("base64url") };
  }
}

/** Agent Gateway transport for the Instance-owned registration authority. */
export class InstanceAuthorityRuntimeRegistrationTransport implements RuntimeRegistrationTransport {
  constructor(private readonly options: { gateway: AgentGatewayClient; registrationId: string }) {}

  async register(request: RuntimeRegistrationRequest): Promise<RuntimeRegistrationResult> {
    if (request.registrationId !== this.options.registrationId) throw new Error("runtime registration id mismatch");
    const result = await this.post(operationPath("runtimeRegister"),
      request) as RuntimeRegistrationResult;
    if (result.status === "active" && Number.isFinite(Date.parse(result.leaseExpiresAt)) &&
        Date.parse(result.leaseExpiresAt) <= Date.now()) throw new RuntimeRegistrationLeaseExpiredError();
    if (validateRuntimeInternalRegistrationResult(result) ||
        (result.status !== "fenced" && (result.journalStoreId !== request.journalStoreId ||
          result.journalGeneration !== request.journalGeneration)) ||
        (result.status === "active" && (!result.instanceId || !result.leaseId ||
          !result.runtimeLeaseCredential || !Number.isInteger(result.heartbeatIntervalMs) ||
          !Number.isFinite(Date.parse(result.leaseExpiresAt)) || Date.parse(result.leaseExpiresAt) <= Date.now()))) {
      throw new Error("Instance authority returned an invalid registration binding");
    }
    return result;
  }

  async heartbeat(request: RuntimeHeartbeatRequest): Promise<RuntimeHeartbeatResult> {
    const result = await this.post(operationPath("runtimeHeartbeat", { registrationId: this.options.registrationId }),
      request) as RuntimeHeartbeatResult;
    if (!result || !["renewed", "fenced", "expired"].includes(result.status) ||
        (result.status === "renewed" && (!result.runtimeLeaseCredential ||
          !Number.isFinite(Date.parse(result.leaseExpiresAt))))) {
      throw new Error("Instance authority returned an invalid heartbeat result");
    }
    return result;
  }

  private async post(path: string, body: RuntimeRegistrationRequest | RuntimeHeartbeatRequest): Promise<unknown> {
    // The coordinator owns retry cadence (5 s / handoff wait); the transport sends exactly once.
    const response = await this.options.gateway.fetch(path, {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body), retry: false,
    });
    if (!response.ok) {
      const detail = await response.text().catch(() => "");
      // Agent Gateway maps authority FailedPrecondition to HTTP 409. When the
      // gateway surfaces the gRPC status message (see backend register handler),
      // classify handoff-wait vs journal-generation advance. Generic conflict
      // bodies remain handoff (legacy) so old gateways keep waiting leaseTTL.
      if (response.status === 409 && path.endsWith("/runtime/registrations")) {
        throw classifyRuntimeRegistrationConflict(detail);
      }
      throw new Error(
        `runtime Instance authority request failed (${response.status} ${response.statusText}) ${path}`
          + (detail ? `: ${detail.slice(0, 240)}` : ""),
      );
    }
    return await response.json();
  }
}

/** Parse register-path 409 bodies into handoff wait vs journal-generation roll vs owner-reset. */
export function classifyRuntimeRegistrationConflict(detail: string):
RuntimeRegistrationHandoffRequiredError | RuntimeRegistrationJournalAdvanceRequiredError |
RuntimeRegistrationOwnerResetRequiredError {
  const message = extractConflictMessage(detail);
  if (/journal store change requires owner reset authorization/i.test(message)) {
    return new RuntimeRegistrationOwnerResetRequiredError();
  }
  if (/journal generation must advance exactly once/i.test(message)) {
    const current = message.match(/\bcurrent=(\d+)\b/i)?.[1];
    const expected = message.match(/\bexpected=(\d+)\b/i)?.[1];
    return new RuntimeRegistrationJournalAdvanceRequiredError({
      currentJournalGeneration: current && /^[1-9][0-9]*$/.test(current)
        ? current as `${bigint}` : undefined,
      expectedJournalGeneration: expected && /^[1-9][0-9]*$/.test(expected)
        ? expected as `${bigint}` : undefined,
    });
  }
  if (/active runtime lease requires explicit handoff/i.test(message) ||
      /waiting for the active runtime lease/i.test(message)) {
    return new RuntimeRegistrationHandoffRequiredError();
  }
  // Legacy Agent Gateway: generic "conflicts with current state" — preserve
  // handoff backoff. Post-expiry journal conflicts then surface after wait as
  // the journal-advance message once the gateway forwards authority text.
  return new RuntimeRegistrationHandoffRequiredError();
}

function extractConflictMessage(detail: string): string {
  if (!detail) return "";
  try {
    const parsed = JSON.parse(detail) as { error?: { message?: unknown }; message?: unknown };
    const fromError = parsed.error?.message;
    if (typeof fromError === "string") return fromError;
    if (typeof parsed.message === "string") return parsed.message;
  } catch { /* plain-text body */ }
  return detail;
}
