import { describe, expect, it, vi } from "vitest";
import { warmGrantJwks } from "./grant-jwks.js";

describe("warmGrantJwks", () => {
  it("rides out a control plane that is not serving yet and reports ready", async () => {
    let calls = 0;
    const refresh = vi.fn(async () => { if (++calls < 3) throw new Error("JWKS fetch failed: HTTP 503"); });
    const retries: number[] = [];
    const ready = await warmGrantJwks({ refresh }, {
      policy: { maxAttempts: 10, initialDelayMs: 1, maxDelayMs: 1, factor: 1 }, onRetry: (i) => retries.push(i.attempt),
    });
    expect(ready).toBe(true);
    expect(retries).toEqual([1, 2]);
  });

  it("returns false (not a thrown fence) when the budget is spent", async () => {
    const refresh = vi.fn(async () => { throw new Error("JWKS fetch failed: HTTP 503"); });
    expect(await warmGrantJwks({ refresh }, { policy: { maxAttempts: 2, initialDelayMs: 1, maxDelayMs: 1, factor: 1 } })).toBe(false);
    expect(refresh).toHaveBeenCalledTimes(2);
  });
});
