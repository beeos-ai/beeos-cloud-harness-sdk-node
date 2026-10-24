import { createHash } from "node:crypto";
import { RUNTIME_HASH_DOMAINS, runtimeHashPreimage, type JcsValue } from "@beeos-ai/beeos-types/runtime";
import type { RuntimeRegistrationAuthorityLease } from "./runtime-registration.js";
import { RuntimeLeaseHttp } from "./runtime-lease-http.js";

export const RUNTIME_ORIGINAL_INTENT_ROUTE = "/api/v1/runtime/execution/original-intent";
export interface RuntimeOriginalIntentLocator {
  operation_id: string; request_message_id: string; native_request_hash: string;
}
/** Mirrors the actual owner PreparedOriginalIntentEnvelope; credentials are transient. */
export interface PreparedOriginalIntentEnvelope {
  request: Readonly<Record<string, unknown>>;
  execution_grant: string; execution_grant_expires_at: string;
  public_request_hash: string; native_request_hash: string;
  lease: { instance_id: string; lease_id: string; handler_identity: string; runtime_epoch: string };
}
export class RuntimeOriginalIntentHttpError extends Error {
  constructor(readonly status: number, readonly code: string) {
    super(`original execution authority unavailable (${status})`); this.name = "RuntimeOriginalIntentHttpError";
  }
}
const object = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const nonempty = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const digest = (value: unknown): value is string => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);

/** Lease-only original read/renewal. The native owner must verify the returned JWT before using it. */
export class RuntimeOriginalIntent {
  constructor(private readonly http: RuntimeLeaseHttp) {}

  async read(lease: RuntimeRegistrationAuthorityLease, locator: RuntimeOriginalIntentLocator,
    signal?: AbortSignal): Promise<PreparedOriginalIntentEnvelope> {
    if (!nonempty(lease.runtimeLeaseCredential) || !Number.isFinite(Date.parse(lease.leaseExpiresAt)) || Date.parse(lease.leaseExpiresAt) <= Date.now() ||
        !nonempty(locator.operation_id) || !nonempty(locator.request_message_id) || !digest(locator.native_request_hash)) {
      throw new Error("current lease and exact original execution locator are required");
    }
    const body = { operation_id: locator.operation_id, request_message_id: locator.request_message_id,
      native_request_hash: locator.native_request_hash };
    const response = await this.http.postJson(RUNTIME_ORIGINAL_INTENT_ROUTE,
      { runtimeLeaseCredential: lease.runtimeLeaseCredential }, body, { retry: false }, signal);
    let raw: unknown;
    try { raw = await response.json(); } catch { throw new Error("original execution authority returned invalid JSON"); }
    if (!response.ok) {
      const detail = object(raw) && object(raw.error) ? raw.error : {};
      throw new RuntimeOriginalIntentHttpError(response.status, nonempty(detail.code) ? detail.code : "original_intent_unavailable");
    }
    const r = object(raw) && object(raw.request) ? raw.request : {};
    const actual = object(raw) && object(raw.lease) ? raw.lease : {};
    const rpc = object(r.request) ? r.request : {};
    const hash = createHash("sha256").update(runtimeHashPreimage(RUNTIME_HASH_DOMAINS.request, rpc as JcsValue)).digest("hex");
    if (!object(raw) || !nonempty(raw.execution_grant) || !digest(raw.public_request_hash) ||
        raw.native_request_hash !== locator.native_request_hash || r.requestHash !== raw.native_request_hash ||
        r.operationId !== locator.operation_id || r.requestMessageId !== locator.request_message_id ||
        r.schemaVersion !== 2 || r.authorityRealm !== "cloud" || r.method !== "session/prompt" ||
        rpc.jsonrpc !== "2.0" || rpc.method !== "session/prompt" || hash !== locator.native_request_hash ||
        actual.instance_id !== lease.instanceId || actual.lease_id !== lease.leaseId ||
        actual.handler_identity !== lease.handlerIdentity || actual.runtime_epoch !== lease.runtimeEpoch ||
        r.instanceId !== lease.instanceId || r.leaseId !== lease.leaseId ||
        r.runtimeHandlerIdentity !== lease.handlerIdentity || r.dispatchEpoch !== lease.runtimeEpoch ||
        !object(r.cloudSubject) || r.cloudSubject.instanceId !== lease.instanceId ||
        !nonempty(raw.execution_grant_expires_at) || !Number.isFinite(Date.parse(raw.execution_grant_expires_at)) ||
        Date.parse(raw.execution_grant_expires_at) <= Date.now() ||
        Date.parse(raw.execution_grant_expires_at) > Date.parse(lease.leaseExpiresAt) ||
        !nonempty(r.executionDeadlineAt) || !Number.isFinite(Date.parse(r.executionDeadlineAt)) || Date.parse(raw.execution_grant_expires_at) > Date.parse(r.executionDeadlineAt)) {
      throw new Error("original execution authority changed the retained request or current lease");
    }
    return raw as unknown as PreparedOriginalIntentEnvelope;
  }
}
