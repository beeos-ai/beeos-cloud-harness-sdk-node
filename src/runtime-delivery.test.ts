import { afterEach, describe, expect, it, vi } from "vitest";
import { NodeRuntimeDeliveryPort, type RuntimeDeliveryAuthorityLease } from "./runtime-delivery.js";

const lease: RuntimeDeliveryAuthorityLease = {
  instanceId: "inst_123", handlerIdentity: "handler-1", runtimeEpoch: "1",
  leaseId: "lease-1", leaseExpiresAt: "2099-01-01T00:00:00.000Z",
  journalStoreId: "journal-1", journalGeneration: "1",
  runtimeLeaseCredential: "current-lease",
};

afterEach(() => vi.unstubAllGlobals());

describe("Cloud runtime durable transport", () => {
  it("reads with the current lease, delivers once, and acknowledges through the sole reader", async () => {
    const seen: string[] = [];
    let readCount = 0;
    const requests: Array<{ path: string; auth: string | null; deliveryKey: string | null }> = [];
    vi.stubGlobal("fetch", vi.fn(async (input: URL, init: RequestInit) => {
      const path = new URL(input).pathname;
      const headers = new Headers(init.headers);
      requests.push({ path, auth: headers.get("authorization"),
        deliveryKey: headers.get("x-runtime-delivery-key") });
      if (path.endsWith("/read")) {
        if (readCount++ === 0) return Response.json({ status: "deliveries", deliveries:
          [{ deliveryId: "d1", redelivered: false, idleMs: 0, message: { operationId: "op1" } }] });
        return await new Promise<Response>((_resolve, reject) =>
          init.signal?.addEventListener("abort", () => reject(new Error("stopped")), { once: true }));
      }
      if (path.endsWith("/ack")) return Response.json({ status: "acknowledged", deliveryIds: ["d1"] });
      throw new Error("unexpected route");
    }));
    const consumer = new NodeRuntimeDeliveryPort({ serviceOrigin: async () => "https://message.example" },
      { currentLease: () => lease }).consume({
      idleDelayMs: 5,
      onDelivery: async (delivery) => {
        seen.push(delivery.deliveryId);
        await consumer.acknowledge([delivery.deliveryId]);
      },
    });
    consumer.start();
    await vi.waitFor(() => expect(seen).toEqual(["d1"]));
    await consumer.stop();
    expect(requests.map((request) => request.path)).toContain("/api/v1/runtime/deliveries/ack");
    expect(requests.every((request) => request.auth === "Bearer current-lease" &&
      request.deliveryKey === null)).toBe(true);
  });

  it("fences a changed lease after resolving the origin before issuing HTTP", async () => {
    let current = lease;
    const fetcher = vi.fn();
    vi.stubGlobal("fetch", fetcher);
    const consumer = new NodeRuntimeDeliveryPort({ serviceOrigin: async () => {
      current = { ...lease, leaseId: "lease-2" };
      return "https://message.example";
    } }, { currentLease: () => current }).consume({ onDelivery: async () => {} });
    await expect(consumer.history("op1")).rejects.toThrow("lease changed");
    expect(fetcher).not.toHaveBeenCalled();
  });

  it.each(["rotated", "revoked"] as const)(
    "discards a late read when the lease is %s before delivery",
    async (change) => {
      let current: RuntimeDeliveryAuthorityLease | null = lease;
      let release!: () => void;
      let reads = 0;
      const seen = vi.fn(async () => {});
      vi.stubGlobal("fetch", vi.fn(async (input: URL, init: RequestInit) => {
        if (!new URL(input).pathname.endsWith("/read")) throw new Error("unexpected route");
        if (reads++ === 0) {
          await new Promise<void>((resolve) => { release = resolve; });
          return Response.json({ status: "deliveries", deliveries: [
            { deliveryId: "old", redelivered: false, idleMs: 0, message: { operationId: "op1" } },
          ] });
        }
        return await new Promise<Response>((_resolve, reject) =>
          init.signal?.addEventListener("abort", () => reject(new Error("stopped")), { once: true }));
      }));
      const consumer = new NodeRuntimeDeliveryPort({ serviceOrigin: async () => "https://message.example" },
        { currentLease: () => current }).consume({ onDelivery: seen, idleDelayMs: 5 });
      consumer.start();
      await vi.waitFor(() => expect(reads).toBeGreaterThan(0));
      current = change === "rotated" ? { ...lease, leaseId: "lease-2" } : null;
      release();
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(seen).not.toHaveBeenCalled();
      await consumer.stop();
    },
  );

  it("aborts an active delivery on the first authoritative renewal rejection", async () => {
    let reads = 0;
    let workerAborted = false;
    vi.stubGlobal("fetch", vi.fn(async (input: URL, init: RequestInit) => {
      const path = new URL(input).pathname;
      if (path.endsWith("/renew")) return Response.json({ message: "fenced" }, { status: 401 });
      if (path.endsWith("/read") && reads++ === 0) return Response.json({
        status: "deliveries", deliveries: [
          { deliveryId: "d1", redelivered: false, idleMs: 0, message: { operationId: "op1" } },
        ],
      });
      return await new Promise<Response>((_resolve, reject) =>
        init.signal?.addEventListener("abort", () => reject(new Error("stopped")), { once: true }));
    }));
    const consumer = new NodeRuntimeDeliveryPort({ serviceOrigin: async () => "https://message.example" },
      { currentLease: () => lease }).consume({ renewIntervalMs: 5,
      onDelivery: async (_delivery, context) => {
        await new Promise<void>((resolve) => context.signal.addEventListener("abort", () => {
          workerAborted = true; resolve();
        }, { once: true }));
      } });
    consumer.start();
    await vi.waitFor(() => expect(workerAborted).toBe(true));
    await consumer.stop();
  });

  it("aborts an active worker immediately when authority revokes the lease", async () => {
    let current: RuntimeDeliveryAuthorityLease | null = lease;
    let notify: (() => void) | undefined;
    let reads = 0;
    let aborted = false;
    vi.stubGlobal("fetch", vi.fn(async (input: URL, init: RequestInit) => {
      if (new URL(input).pathname.endsWith("/read") && reads++ === 0) {
        return Response.json({ status: "deliveries", deliveries: [
          { deliveryId: "d1", redelivered: false, idleMs: 0, message: { operationId: "op1" } },
        ] });
      }
      return await new Promise<Response>((_resolve, reject) =>
        init.signal?.addEventListener("abort", () => reject(new Error("stopped")), { once: true }));
    }));
    const consumer = new NodeRuntimeDeliveryPort({ serviceOrigin: async () => "https://message.example" },
      { currentLease: () => current, subscribeAuthority: (listener) => {
        notify = listener; return () => { notify = undefined; };
      } }).consume({ onDelivery: async (_delivery, context) =>
        await new Promise<void>((resolve) => context.signal.addEventListener("abort", () => {
          aborted = true; resolve();
        }, { once: true })) });
    consumer.start();
    await vi.waitFor(() => expect(reads).toBeGreaterThan(0));
    current = null;
    notify?.();
    await vi.waitFor(() => expect(aborted).toBe(true));
    await consumer.stop();
  });

  it("ignores a late renewal response from a stopped generation after restart", async () => {
    let reads = 0;
    let renewStarted = false;
    let releaseRenew!: () => void;
    let renewCalls = 0;
    let secondAborted = false;
    const started: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: URL, init: RequestInit) => {
      const path = new URL(input).pathname;
      if (path.endsWith("/renew")) {
        renewStarted = true;
        if (renewCalls++ > 0) return await new Promise<Response>((_resolve, reject) =>
          init.signal?.addEventListener("abort", () => reject(new Error("stopped")), { once: true }));
        await new Promise<void>((resolve) => { releaseRenew = resolve; });
        return Response.json({ status: "renewed", renewed: [], notPending: ["d2"] });
      }
      if (path.endsWith("/read") && (reads === 0 || reads === 2)) {
        const id = reads++ === 0 ? "d1" : "d2";
        return Response.json({ status: "deliveries", deliveries: [
          { deliveryId: id, redelivered: false, idleMs: 0, message: { operationId: id } },
        ] });
      }
      reads++;
      return await new Promise<Response>((_resolve, reject) =>
        init.signal?.addEventListener("abort", () => reject(new Error("stopped")), { once: true }));
    }));
    const consumer = new NodeRuntimeDeliveryPort({ serviceOrigin: async () => "https://message.example" },
      { currentLease: () => lease }).consume({ renewIntervalMs: 5,
      onDelivery: async (delivery, context) => {
        started.push(delivery.deliveryId);
        await new Promise<void>((resolve) => context.signal.addEventListener("abort", () => {
          if (delivery.deliveryId === "d2") secondAborted = true;
          resolve();
        }, { once: true }));
      } });
    consumer.start();
    await vi.waitFor(() => expect(renewStarted).toBe(true));
    await consumer.stop();
    consumer.start();
    await vi.waitFor(() => expect(started).toContain("d2"));
    releaseRenew();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(secondAborted).toBe(false);
    await consumer.stop();
  });

  it("does not abort a new lease's worker when an old lease renewal rejects late", async () => {
    let current: RuntimeDeliveryAuthorityLease = lease;
    let notify: (() => void) | undefined;
    let releaseRead!: () => void;
    let releaseRenew!: () => void;
    let renewStarted = false;
    let d2Started = false;
    let d2Aborted = false;
    let l1Reads = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: URL, init: RequestInit) => {
      const path = new URL(input).pathname;
      const auth = new Headers(init.headers).get("authorization");
      if (path.endsWith("/renew") && auth === "Bearer current-lease") {
        renewStarted = true;
        await new Promise<void>((resolve) => { releaseRenew = resolve; });
        return Response.json({ message: "old lease fenced" }, { status: 401 });
      }
      if (path.endsWith("/read") && auth === "Bearer current-lease") {
        if (l1Reads++ === 0) return Response.json({ status: "deliveries", deliveries: [
          { deliveryId: "d1", redelivered: false, idleMs: 0, message: { operationId: "op1" } },
        ] });
        await new Promise<void>((resolve) => { releaseRead = resolve; });
        return Response.json({ status: "deliveries", deliveries: [] });
      }
      if (path.endsWith("/read") && auth === "Bearer next-lease" && !d2Started) {
        return Response.json({ status: "deliveries", deliveries: [
          { deliveryId: "d2", redelivered: false, idleMs: 0, message: { operationId: "op2" } },
        ] });
      }
      return await new Promise<Response>((_resolve, reject) =>
        init.signal?.addEventListener("abort", () => reject(new Error("stopped")), { once: true }));
    }));
    const consumer = new NodeRuntimeDeliveryPort({ serviceOrigin: async () => "https://message.example" },
      { currentLease: () => current, subscribeAuthority: (listener) => {
        notify = listener; return () => { notify = undefined; };
      } }).consume({ renewIntervalMs: 5,
      onDelivery: async (delivery, context) => {
        if (delivery.deliveryId === "d2") d2Started = true;
        await new Promise<void>((resolve) => context.signal.addEventListener("abort", () => {
          if (delivery.deliveryId === "d2") d2Aborted = true;
          resolve();
        }, { once: true }));
      } });
    consumer.start();
    await vi.waitFor(() => expect(renewStarted && l1Reads >= 2).toBe(true));
    current = { ...lease, leaseId: "lease-2", runtimeLeaseCredential: "next-lease" };
    notify?.();
    releaseRead();
    await vi.waitFor(() => expect(d2Started).toBe(true));
    releaseRenew();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(d2Aborted).toBe(false);
    await consumer.stop();
  });

  it("reconciles an ambiguous append through history without retrying the POST", async () => {
    const requests: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: URL) => {
      const path = new URL(input).pathname;
      requests.push(path);
      if (path.endsWith("/messages")) throw new Error("connection dropped");
      if (path.endsWith("/history")) return Response.json({ terminal: false, messages: [] });
      throw new Error("unexpected route");
    }));
    const consumer = new NodeRuntimeDeliveryPort({ serviceOrigin: async () => "https://message.example" },
      { currentLease: () => lease }).consume({ onDelivery: async () => {} });
    expect(await consumer.append("op1", "final", { status: "completed" })).toMatchObject({
      outcome: "outcome_unknown",
    });
    expect(requests.filter((path) => path.endsWith("/messages"))).toHaveLength(1);
    expect(requests.filter((path) => path.endsWith("/history"))).toHaveLength(1);
  });
});
