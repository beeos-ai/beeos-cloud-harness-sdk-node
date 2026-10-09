import { generateKeyPairSync, sign, verify } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { runtimeIdentitySignatureMessage, type Sha256HexDigest } from "@beeos-ai/beeos-types/runtime";
import { RuntimeRegistrationCoordinator } from "./runtime-registration.js";

const digest = "d5db2c5e7c78cee73e5672a3c18a8e56179b28888f6fbb660d98ec021ca9de9b" as Sha256HexDigest;
afterEach(() => vi.useRealTimers());

function config(privateKey: ReturnType<typeof generateKeyPairSync>["privateKey"]) {
  return {
    registrationId: "registration-1", handlerIdentity: "handler-1", contractRevision: "revision-1",
    runtimeMethods: ["models/list"] as const, capabilities: ["models"] as const,
    manifestDigest: digest, journalStoreId: "journal-1", journalGeneration: "1" as const,
    instanceIdentityKeyId: "key-1", targetInstanceId: "inst_123",
    signIdentityProof: (message: Uint8Array) => sign(null, message, privateKey),
  };
}

const active = {
  status: "active" as const, instanceId: "inst_123", runtimeEpoch: "1" as const,
  journalStoreId: "journal-1", journalGeneration: "1" as const,
  leaseId: "lease-1", issuedAt: "2026-09-23T00:00:00.000Z",
  leaseExpiresAt: "2099-01-01T00:00:00.000Z", heartbeatIntervalMs: 20_000,
  runtimeLeaseCredential: "lease-jwt",
};

describe("Cloud runtime registration authority", () => {
  it("signs canonical target-bound registration and heartbeat without owning the key", async () => {
    vi.useFakeTimers();
    const { privateKey, publicKey } = generateKeyPairSync("ed25519");
    const register = vi.fn(async (request: any) => {
      const message = runtimeIdentitySignatureMessage({
        purpose: request.signaturePurpose, instanceIdentityKeyId: request.instanceIdentityKeyId,
        targetInstanceId: request.targetInstanceId, signedAt: request.signedAt,
        nonce: request.nonce, payloadHash: request.payloadHash,
      });
      expect(verify(null, message, publicKey, Buffer.from(request.signature, "base64url"))).toBe(true);
      return active;
    });
    const heartbeat = vi.fn(async () => ({ status: "expired" as const }));
    const lost = vi.fn();
    const coordinator = new RuntimeRegistrationCoordinator({ register, heartbeat },
      config(privateKey), lost, () => Date.parse("2026-09-23T00:00:00.000Z"));
    await coordinator.start();
    expect(coordinator.canClaimMutations).toBe(true);
    await vi.advanceTimersByTimeAsync(20_000);
    expect(heartbeat).toHaveBeenCalledOnce();
    expect(coordinator.canClaimMutations).toBe(false);
    coordinator.stop();
  });

  it("ignores a late registration after stop and rejects another target instance", async () => {
    const { privateKey } = generateKeyPairSync("ed25519");
    let complete!: (value: typeof active) => void;
    const register = vi.fn(() => new Promise<typeof active>((resolve) => { complete = resolve; }));
    const lost = vi.fn();
    const coordinator = new RuntimeRegistrationCoordinator({ register,
      heartbeat: async () => ({ status: "expired" }) }, config(privateKey), lost);
    const starting = coordinator.start();
    coordinator.stop();
    complete(active);
    expect(await starting).toBeNull();
    expect(coordinator.canClaimMutations).toBe(false);
    const wrong = new RuntimeRegistrationCoordinator({ register: async () =>
      ({ ...active, instanceId: "inst_other" }), heartbeat: async () => ({ status: "expired" }) },
    config(privateKey), lost);
    await wrong.start();
    expect(wrong.canClaimMutations).toBe(false);
    expect(lost).toHaveBeenCalledWith("fenced", expect.any(Error));
    wrong.stop();
  });

  it("keeps re-registering after a fenced answer instead of fencing forever", async () => {
    vi.useFakeTimers();
    const { privateKey } = generateKeyPairSync("ed25519");
    const register = vi.fn()
      .mockResolvedValueOnce({ status: "fenced" })
      .mockResolvedValueOnce({ status: "fenced" })
      .mockResolvedValue(active);
    const lost = vi.fn();
    const coordinator = new RuntimeRegistrationCoordinator({ register,
      heartbeat: async () => ({ status: "expired" }) }, config(privateKey), lost);
    expect(await coordinator.start()).toBeNull();
    await vi.advanceTimersByTimeAsync(30_000);
    expect(register).toHaveBeenCalledTimes(2);
    expect(coordinator.canClaimMutations).toBe(false);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(register).toHaveBeenCalledTimes(3);
    expect(coordinator.canClaimMutations).toBe(true);
    expect(lost).toHaveBeenCalledTimes(2);
    coordinator.stop();
  });

  it("retries a refused connection while the control plane starts", async () => {
    vi.useFakeTimers();
    const { privateKey } = generateKeyPairSync("ed25519");
    const register = vi.fn()
      .mockRejectedValueOnce(new TypeError("fetch failed"))
      .mockResolvedValue(active);
    const coordinator = new RuntimeRegistrationCoordinator({ register,
      heartbeat: async () => ({ status: "expired" }) }, config(privateKey), vi.fn());
    await coordinator.start();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(coordinator.canClaimMutations).toBe(true);
    coordinator.stop();
  });

  it("can opt out of fenced recovery", async () => {
    vi.useFakeTimers();
    const { privateKey } = generateKeyPairSync("ed25519");
    const register = vi.fn(async () => ({ status: "fenced" as const }));
    const coordinator = new RuntimeRegistrationCoordinator({ register,
      heartbeat: async () => ({ status: "expired" }) }, { ...config(privateKey), fencedRetry: false }, vi.fn());
    await coordinator.start();
    await vi.advanceTimersByTimeAsync(600_000);
    expect(register).toHaveBeenCalledOnce();
    coordinator.stop();
  });
});
