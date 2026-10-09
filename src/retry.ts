/**
 * One retry/backoff vocabulary for every Cloud control-plane call a harness
 * makes. Hosts never write their own sleep loops: they pick a policy.
 *
 * The Python SDK mirrors these names and numbers (`beeos_cloud_harness_sdk.retry`).
 */

export interface BackoffPolicy {
  /** Total attempts including the first one. `Infinity` retries until `budgetMs`/`signal`. */
  readonly maxAttempts: number;
  readonly initialDelayMs: number;
  readonly maxDelayMs: number;
  readonly factor: number;
  /** Total wall-clock budget; the last attempt may start before it, never after. */
  readonly budgetMs?: number;
}

/** Brief ALB/Agent-Gateway blips on ordinary calls (matches the former Claw edge: 3 attempts, 200/500 ms). */
export const TRANSIENT_BACKOFF: BackoffPolicy = Object.freeze({
  maxAttempts: 3, initialDelayMs: 200, maxDelayMs: 500, factor: 2.5,
});

/**
 * Control plane not ready yet (pod started while Cloud was rolling out). Waits
 * up to two minutes with 1 s → 15 s exponential backoff instead of giving up on
 * the first refused connection.
 */
export const STARTUP_BACKOFF: BackoffPolicy = Object.freeze({
  maxAttempts: Number.POSITIVE_INFINITY, initialDelayMs: 1_000, maxDelayMs: 15_000, factor: 2,
  budgetMs: 120_000,
});

/** Non-HTTP failures and 5xx gateway statuses worth another attempt. */
const TRANSIENT_STATUSES: ReadonlySet<number> = new Set([408, 425, 429, 500, 502, 503, 504]);

export function isTransientStatus(status: number): boolean {
  return TRANSIENT_STATUSES.has(status);
}

/** Error for any non-2xx Cloud answer; `body` is truncated and never holds credentials. */
export class CloudHttpError extends Error {
  readonly status: number;
  readonly body: string;
  /** `error.code` / `code` from a JSON error envelope when present. */
  readonly code?: string;
  constructor(readonly method: string, readonly path: string, status: number, body: string) {
    super(`${method} ${path} failed: HTTP ${status}${body ? ` ${body.slice(0, 240)}` : ""}`);
    this.name = "CloudHttpError";
    this.status = status;
    this.body = body.slice(0, 2_048);
    this.code = errorCodeOf(body);
  }
  get transient(): boolean { return isTransientStatus(this.status); }
}

function errorCodeOf(body: string): string | undefined {
  try {
    const parsed = JSON.parse(body) as { code?: unknown; error?: { code?: unknown } };
    const code = typeof parsed.error?.code === "string" ? parsed.error.code : parsed.code;
    return typeof code === "string" && /^[A-Za-z0-9_.:-]{1,128}$/.test(code) ? code : undefined;
  } catch { return undefined; }
}

export function isTransientError(error: unknown): boolean {
  if (error instanceof CloudHttpError) return error.transient;
  if (error instanceof DOMException && (error.name === "AbortError")) return false;
  // fetch() rejects with TypeError for refused/reset/DNS failures; timeouts surface as TimeoutError.
  return error instanceof TypeError || (error instanceof DOMException && error.name === "TimeoutError");
}

export interface RetryOptions {
  policy: BackoffPolicy;
  isRetryable?: (error: unknown) => boolean;
  signal?: AbortSignal;
  onRetry?: (info: { attempt: number; delayMs: number; error: unknown }) => void;
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
  now?: () => number;
}

export function delayForAttempt(policy: BackoffPolicy, attempt: number): number {
  return Math.min(policy.maxDelayMs, Math.round(policy.initialDelayMs * policy.factor ** (attempt - 1)));
}

export function abortableSleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) { reject(signal.reason); return; }
    const timer = setTimeout(() => { signal?.removeEventListener("abort", onAbort); resolve(); }, ms);
    const onAbort = () => { clearTimeout(timer); reject(signal!.reason); };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

/** Run `operation` until it succeeds, a non-retryable error occurs, or the policy is exhausted. */
export async function retryWithBackoff<T>(
  operation: (attempt: number) => Promise<T>,
  options: RetryOptions,
): Promise<T> {
  const { policy, signal } = options;
  const isRetryable = options.isRetryable ?? isTransientError;
  const sleep = options.sleep ?? abortableSleep;
  const now = options.now ?? Date.now;
  const startedAt = now();
  for (let attempt = 1; ; attempt += 1) {
    signal?.throwIfAborted();
    try {
      return await operation(attempt);
    } catch (error) {
      if (!isRetryable(error) || attempt >= policy.maxAttempts) throw error;
      const delayMs = delayForAttempt(policy, attempt);
      if (policy.budgetMs !== undefined && now() - startedAt + delayMs > policy.budgetMs) throw error;
      options.onRetry?.({ attempt, delayMs, error });
      await sleep(delayMs, signal);
    }
  }
}
