/**
 * Invocation claim lifecycle from the harness spec.
 * Message Service does not register these routes yet (`x-beeos-route-gap`);
 * the paths are the beeos-types contracts so a harness does not hard-code them.
 *
 * Python mirror: `beeos_cloud_harness_sdk.claims`.
 */
import { CLOUD_MESSAGE_ROUTES, RuntimeLeaseHttp, type RuntimeLeaseCredential } from "./runtime-lease-http.js";
import { HarnessProtocolError } from "./runtime-message-plane.js";

const NEXT_STATUS = ["claimed", "empty", "fenced"] as const;
const EXACT_STATUS = ["claimed", "already_claimed", "fenced", "epoch_handoff_required", "terminal"] as const;
const RENEW_STATUS = ["renewed", "fenced", "expired", "terminal"] as const;

export interface RuntimeClaimNextRequest {
  claimRequestId: string;
  instanceId: string;
  handlerIdentity: string;
  runtimeEpoch: string;
  journalStoreId: string;
  journalGeneration: string;
  runtimeLeaseCredential: string;
}

export interface RuntimeClaimExactRequest extends RuntimeClaimNextRequest {
  operationId: string;
  requestMessageId: string;
  dispatchEpoch: string;
}

export interface RuntimeClaimFence {
  claimId: string;
  handlerIdentity: string;
  runtimeEpoch: string;
  fencingToken: string;
  journalStoreId: string;
  journalGeneration: string;
  operationId: string;
  requestMessageId: string;
  runtimeLeaseCredential: string;
}

export interface RuntimeClaimRenewRequest extends RuntimeClaimFence {}

export interface RuntimeClaimReleaseRequest extends RuntimeClaimFence {
  reason: "completed" | "cancelled" | "worker_shutdown" | "claim_rejected";
}

async function readJson(response: Response, what: string): Promise<{ status: string }> {
  const text = await response.text();
  let body: unknown;
  try {
    body = text ? JSON.parse(text) : undefined;
  } catch {
    body = text;
  }
  if (!response.ok) {
    throw new HarnessProtocolError(`${what} failed (${response.status})`, response.status,
      typeof body === "string" ? body : JSON.stringify(body));
  }
  if (!body || typeof body !== "object" || typeof (body as { status?: unknown }).status !== "string") {
    throw new HarnessProtocolError(`${what} returned an invalid result`);
  }
  return body as { status: string };
}

export class RuntimeClaimClient {
  constructor(private readonly http: RuntimeLeaseHttp) {}

  async claimNext(credential: RuntimeLeaseCredential, body: RuntimeClaimNextRequest): Promise<{ status: string }> {
    const result = await readJson(await this.http.postJson(CLOUD_MESSAGE_ROUTES.claimNext, credential, body),
      "claim next");
    if (!NEXT_STATUS.includes(result.status as typeof NEXT_STATUS[number])) {
      throw new HarnessProtocolError("claim next returned an invalid status");
    }
    return result;
  }

  async claimExact(credential: RuntimeLeaseCredential, body: RuntimeClaimExactRequest): Promise<{ status: string }> {
    const result = await readJson(
      await this.http.postJson(CLOUD_MESSAGE_ROUTES.claimExact(body.operationId), credential, body), "claim exact");
    if (!EXACT_STATUS.includes(result.status as typeof EXACT_STATUS[number])) {
      throw new HarnessProtocolError("claim exact returned an invalid status");
    }
    return result;
  }

  async renewClaim(credential: RuntimeLeaseCredential, body: RuntimeClaimRenewRequest): Promise<{ status: string }> {
    const result = await readJson(
      await this.http.postJson(CLOUD_MESSAGE_ROUTES.claimRenew(body.operationId), credential, body), "claim renew");
    if (!RENEW_STATUS.includes(result.status as typeof RENEW_STATUS[number])) {
      throw new HarnessProtocolError("claim renew returned an invalid status");
    }
    return result;
  }

  async releaseClaim(credential: RuntimeLeaseCredential, body: RuntimeClaimReleaseRequest): Promise<void> {
    const response = await this.http.postJson(CLOUD_MESSAGE_ROUTES.claimRelease(body.operationId), credential, body);
    if (response.status !== 204) {
      const text = await response.text();
      throw new HarnessProtocolError(`claim release failed (${response.status})`, response.status, text);
    }
  }
}
