import { describe, expect, it, vi } from "vitest";
import {
  CloudHttpError, STARTUP_BACKOFF, TRANSIENT_BACKOFF, delayForAttempt, isTransientError, retryWithBackoff,
} from "./retry.js";

const noSleep = async () => undefined;

describe("retryWithBackoff", () => {
  it("retries a refused connection until the control plane answers", async () => {
    let calls = 0;
    const delays: number[] = [];
    const result = await retryWithBackoff(async () => {
      calls += 1;
      if (calls < 4) throw new TypeError("fetch failed");
      return "ready";
    }, { policy: STARTUP_BACKOFF, sleep: noSleep, onRetry: (info) => delays.push(info.delayMs) });
    expect(result).toBe("ready");
    expect(delays).toEqual([1_000, 2_000, 4_000]);
  });

  it("does not retry an authoritative 4xx", async () => {
    const op = vi.fn(async () => { throw new CloudHttpError("POST", "/x", 403, "{\"error\":{\"code\":\"denied\"}}"); });
    await expect(retryWithBackoff(op, { policy: TRANSIENT_BACKOFF, sleep: noSleep })).rejects.toMatchObject({
      status: 403, code: "denied",
    });
    expect(op).toHaveBeenCalledOnce();
  });

  it("gives up when the wall-clock budget cannot cover the next wait", async () => {
    let clock = 0;
    const op = vi.fn(async () => { throw new TypeError("fetch failed"); });
    await expect(retryWithBackoff(op, {
      policy: { ...STARTUP_BACKOFF, budgetMs: 2_500 }, now: () => clock, sleep: async (ms) => { clock += ms; },
    })).rejects.toBeInstanceOf(TypeError);
    expect(op).toHaveBeenCalledTimes(2);
  });

  it("stops at the abort signal", async () => {
    const controller = new AbortController();
    const op = vi.fn(async () => { throw new TypeError("fetch failed"); });
    const pending = retryWithBackoff(op, {
      policy: STARTUP_BACKOFF, signal: controller.signal,
      sleep: async () => { controller.abort(new Error("stopped")); },
    });
    await expect(pending).rejects.toThrow("stopped");
  });

  it("classifies transport failures and gateway statuses as transient", () => {
    expect(isTransientError(new TypeError("fetch failed"))).toBe(true);
    expect(isTransientError(new CloudHttpError("GET", "/x", 503, ""))).toBe(true);
    expect(isTransientError(new CloudHttpError("GET", "/x", 404, ""))).toBe(false);
    expect(delayForAttempt(STARTUP_BACKOFF, 10)).toBe(15_000);
  });
});
