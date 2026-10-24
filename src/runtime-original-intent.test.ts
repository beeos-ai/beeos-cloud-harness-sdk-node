import { createHash } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { RUNTIME_HASH_DOMAINS, runtimeHashPreimage } from "@beeos-ai/beeos-types/runtime";
import { RuntimeLeaseHttp } from "./runtime-lease-http.js";
import { RuntimeOriginalIntent, type PreparedOriginalIntentEnvelope } from "./runtime-original-intent.js";
import type { RuntimeRegistrationAuthorityLease } from "./runtime-registration.js";

function fixture() {
  const lease: RuntimeRegistrationAuthorityLease = { instanceId: "instance", leaseId: "lease", runtimeEpoch: "11",
    handlerIdentity: "handler", runtimeLeaseCredential: "current-token", leaseExpiresAt: new Date(Date.now() + 120_000).toISOString(),
    heartbeatIntervalMs: 15_000 };
  const rpc = { jsonrpc: "2.0", method: "session/prompt", params: { opaque: "原始\u0000bytes" } };
  const hash = createHash("sha256").update(runtimeHashPreimage(RUNTIME_HASH_DOMAINS.request, rpc)).digest("hex");
  const locator = { operation_id: "op", request_message_id: "request", native_request_hash: hash };
  const envelope: PreparedOriginalIntentEnvelope = { request: { schemaVersion: 2, authorityRealm: "cloud", method: "session/prompt", request: rpc,
    operationId: "op", requestMessageId: "request", requestHash: hash, instanceId: "instance", leaseId: "lease",
    runtimeHandlerIdentity: "handler", dispatchEpoch: "11", cloudSubject: { instanceId: "instance" }, executionDeadlineAt: lease.leaseExpiresAt },
    native_request_hash: hash, public_request_hash: "a".repeat(64), execution_grant: "transient-new-grant",
    execution_grant_expires_at: new Date(Date.now() + 25_000).toISOString(),
    lease: { instance_id: "instance", lease_id: "lease", handler_identity: "handler", runtime_epoch: "11" } };
  const send = vi.fn(async (_url: URL, _init?: RequestInit) => Response.json(envelope));
  const client = new RuntimeOriginalIntent(new RuntimeLeaseHttp({ origin: "https://gateway.test", fetch: send as typeof fetch,
    retry: { maxAttempts: 3, initialDelayMs: 1, maxDelayMs: 1, factor: 1 } }));
  return { client, send, envelope, lease, locator, rpc };
}

describe("current original execution authority", () => {
  it("uses lease-only auth and retains exact original RPC plus transient outer grant", async () => {
    const f = fixture(); const original = structuredClone(f.envelope);
    expect(await f.client.read(f.lease, f.locator)).toEqual(original);
    expect(f.send.mock.calls[0]![0].pathname).toBe("/api/v1/runtime/execution/original-intent");
    const request = f.send.mock.calls[0]![1]!;
    expect(JSON.parse(String(request.body))).toEqual(f.locator);
    expect(new Headers(request.headers).get("authorization")).toBe("Bearer current-token");
    expect(new Headers(request.headers).has("x-beeos-execution-grant")).toBe(false);
    expect(request.redirect).toBe("error"); expect(f.envelope).toEqual(original);
  });
  it.each(["rpc", "lease", "locator", "expiry", "deadline"])("rejects changed %s without retry", async kind => {
    const f = fixture();
    if (kind === "rpc") f.rpc.params.opaque = "changed";
    if (kind === "lease") f.envelope.lease.runtime_epoch = "12";
    if (kind === "locator") (f.envelope.request as Record<string, unknown>).operationId = "other";
    if (kind === "expiry") f.envelope.execution_grant_expires_at = "invalid";
    if (kind === "deadline") (f.envelope.request as Record<string, unknown>).executionDeadlineAt = "invalid";
    await expect(f.client.read(f.lease, f.locator)).rejects.toThrow("changed"); expect(f.send).toHaveBeenCalledOnce();
  });
  it("never sends an invalid current lease and preserves owner rejection once", async () => {
    const f = fixture(); f.lease.leaseExpiresAt = "invalid";
    await expect(f.client.read(f.lease, f.locator)).rejects.toThrow("current lease"); expect(f.send).not.toHaveBeenCalled();
    const g = fixture(); g.send.mockImplementation(async () => Response.json({ error: { code: "original_revoked" } }, { status: 503 }));
    await expect(g.client.read(g.lease, g.locator)).rejects.toMatchObject({ status: 503, code: "original_revoked" });
    expect(g.send).toHaveBeenCalledOnce();
  });
});
