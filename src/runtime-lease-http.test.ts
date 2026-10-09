import { afterEach, describe, expect, it, vi } from "vitest";
import { CLOUD_MESSAGE_ROUTES, RuntimeLeaseHttp } from "./runtime-lease-http.js";

const lease = { runtimeLeaseCredential: "lease-credential", executionGrant: "grant" };
afterEach(() => vi.unstubAllGlobals());

describe("RuntimeLeaseHttp", () => {
  it("attaches the lease and grant only to its own origin", async () => {
    const spy = vi.fn(async (_u: URL, _i?: RequestInit) => new Response("{}", { status: 200 }));
    const http = new RuntimeLeaseHttp({ origin: "https://ms.test", fetch: spy as unknown as typeof fetch });
    await http.postJson(CLOUD_MESSAGE_ROUTES.historyBoundary("c/1", "prepare"), lease, { a: 1 });
    const [url, init] = spy.mock.calls[0]!;
    expect(url.toString()).toBe("https://ms.test/api/v1/runtime/conversations/c%2F1/history-boundary/prepare");
    const headers = new Headers(init!.headers);
    expect(headers.get("authorization")).toBe("Bearer lease-credential");
    expect(headers.get("x-beeos-execution-grant")).toBe("grant");
    expect(headers.get("x-runtime-delivery-key")).toBeNull();
    expect(init!.redirect).toBe("error");
    await expect(http.fetch("https://other.test/x", lease)).rejects.toThrow("another origin");
  });

  it("never replays an append-like call by default but retries an idempotent projection through 503", async () => {
    let calls = 0;
    const spy = vi.fn(async () => (++calls < 3 ? new Response("", { status: 503 }) : new Response("{}", { status: 200 })));
    const http = new RuntimeLeaseHttp({ origin: "https://ms.test", fetch: spy as unknown as typeof fetch });
    expect((await http.postJson("/a", lease, {})).status).toBe(503);
    expect(spy).toHaveBeenCalledOnce();
    calls = 0; spy.mockClear();
    const retried = await http.postJson("/b", lease, {}, { retry: { maxAttempts: 3, initialDelayMs: 1, maxDelayMs: 1, factor: 1 } });
    expect(retried.status).toBe(200);
    expect(spy).toHaveBeenCalledTimes(3);
  });

  it("sends the delivery key only when the caller has one", async () => {
    const spy = vi.fn(async () => new Response("{}", { status: 200 }));
    const http = new RuntimeLeaseHttp({ origin: "https://ms.test", fetch: spy as unknown as typeof fetch });
    await http.postJson("/k", { runtimeLeaseCredential: "lease", scopedDeliveryKey: "delivery-key" }, {});
    expect(new Headers(spy.mock.calls[0]![1].headers).get("x-runtime-delivery-key")).toBe("delivery-key");
  });
});
