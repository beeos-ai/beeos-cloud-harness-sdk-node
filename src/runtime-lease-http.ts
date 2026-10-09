import { operationPath, type OperationId } from "./generated/operations.gen.js";
import { TRANSIENT_BACKOFF, isTransientError, retryWithBackoff, type BackoffPolicy } from "./retry.js";

/**
 * Lease-authenticated HTTP to the Cloud data plane (Message Service and the
 * Gateway's internal runtime routes). The runtime lease credential is the only
 * authority; it is attached here and nowhere else, always to a fixed origin.
 *
 * Python mirror: `beeos_cloud_harness_sdk.lease_http.RuntimeLeaseHttp`.
 */
export interface RuntimeLeaseCredential {
  runtimeLeaseCredential: string;
  /** Per-operation execution grant when the route requires one. */
  executionGrant?: string;
  /** Message Service shared delivery key (`X-Runtime-Delivery-Key`). Required on session-model and cron. */
  scopedDeliveryKey?: string;
}

const HISTORY_BOUNDARY_OPERATIONS = {
  prepare: "runtimeHistoryBoundaryPrepare",
  resetting: "runtimeHistoryBoundaryResetting",
  reset_done: "runtimeHistoryBoundaryResetDone",
  commit: "runtimeHistoryBoundaryCommit",
  abort: "runtimeHistoryBoundaryAbort",
  reconcile: "runtimeHistoryBoundaryReconcile",
} as const satisfies Record<string, OperationId>;

export type HistoryBoundaryAction = keyof typeof HISTORY_BOUNDARY_OPERATIONS;

/** Lease-authenticated Message Service routes; every path comes from the generated route table. */
export const CLOUD_MESSAGE_ROUTES = Object.freeze({
  deliveriesRead: operationPath("runtimeDeliveriesRead"),
  deliveriesRenew: operationPath("runtimeDeliveriesRenew"),
  deliveriesAck: operationPath("runtimeDeliveriesAck"),
  conversations: operationPath("runtimeConversationSync"),
  operationHistory: (operationId: string) => operationPath("runtimeOperationHistoryGet", { operationId }),
  operationMessages: (operationId: string) => operationPath("runtimeOperationMessageAppend", { operationId }),
  historyBoundary: (conversationId: string, action: HistoryBoundaryAction) =>
    operationPath(HISTORY_BOUNDARY_OPERATIONS[action], { conversationId }),
  metadataModel: (conversationId: string) => operationPath("runtimeConversationMetadataModel", { conversationId }),
  cronDelivery: (conversationId: string) => operationPath("runtimeConversationCronDelivery", { conversationId }),
  claimNext: operationPath("runtimeClaimNext"),
  claimExact: (operationId: string) => operationPath("runtimeClaimExact", { operationId }),
  claimRenew: (operationId: string) => operationPath("runtimeClaimRenew", { operationId }),
  claimRelease: (operationId: string) => operationPath("runtimeClaimRelease", { operationId }),
});

export const CLOUD_GATEWAY_RUNTIME_ROUTES = Object.freeze({
  outputFiles: (operationId: string, stage: "presign" | "confirm") =>
    operationPath(stage === "presign" ? "runtimeOutputFilePresign" : "runtimeOutputFileConfirm", { operationId }),
  inputFilesResolve: (operationId: string) => operationPath("runtimeInputFilesResolve", { operationId }),
});

export interface RuntimeLeaseHttpOptions {
  /** Fixed origin the lease may be sent to (Message Service or Agent Gateway). */
  origin: string;
  /** Per-call retry; defaults to none because append/ack outcomes can be ambiguous. */
  retry?: BackoffPolicy | false;
  fetch?: typeof fetch;
}

export class RuntimeLeaseHttp {
  readonly origin: URL;
  constructor(private readonly options: RuntimeLeaseHttpOptions) {
    this.origin = new URL(options.origin);
    if (!["http:", "https:"].includes(this.origin.protocol) || this.origin.username || this.origin.password) {
      throw new Error("runtime lease origin must be http(s) without credentials");
    }
  }

  /**
   * `retry: true` marks the call idempotent (metadata/model projection, reads);
   * default never replays so an uncertain append/ack is reconciled by the caller.
   */
  async fetch(target: string | URL, credential: RuntimeLeaseCredential, init: RequestInit = {},
    resilience: { retry?: boolean | BackoffPolicy } = {}): Promise<Response> {
    const url = typeof target === "string" && target.startsWith("/")
      ? new URL(target, this.origin) : new URL(target);
    if (url.origin !== this.origin.origin) {
      throw new Error("runtime lease credential cannot be sent to another origin");
    }
    const headers = new Headers(init.headers);
    headers.set("authorization", `Bearer ${credential.runtimeLeaseCredential}`);
    if (credential.executionGrant) headers.set("x-beeos-execution-grant", credential.executionGrant);
    if (credential.scopedDeliveryKey) headers.set("x-runtime-delivery-key", credential.scopedDeliveryKey);
    const send = async () => {
      const response = await (this.options.fetch ?? fetch)(url, { ...init, headers, redirect: "error" });
      if (response.url && new URL(response.url).origin !== this.origin.origin) {
        throw new Error("runtime lease response origin changed");
      }
      return response;
    };
    const policy = resilience.retry === true ? this.options.retry || TRANSIENT_BACKOFF
      : resilience.retry || this.options.retry || false;
    if (!policy) return await send();
    return await retryWithBackoff(async () => {
      const response = await send();
      if ([502, 503, 504].includes(response.status)) throw new TransientLeaseAnswer(response);
      return response;
    }, { policy, signal: init.signal ?? undefined,
      isRetryable: (error) => error instanceof TransientLeaseAnswer || isTransientError(error) })
      .catch((error: unknown) => {
        if (error instanceof TransientLeaseAnswer) return error.response;
        throw error;
      });
  }

  /** JSON POST helper that returns the Response so the host maps its own domain errors. */
  async postJson(path: string, credential: RuntimeLeaseCredential, body: unknown,
    resilience?: { retry?: boolean | BackoffPolicy }, signal?: AbortSignal): Promise<Response> {
    return await this.fetch(path, credential, {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body), signal,
    }, resilience);
  }
}

class TransientLeaseAnswer extends Error {
  constructor(readonly response: Response) { super(`transient HTTP ${response.status}`); }
}
