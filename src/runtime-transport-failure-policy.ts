/**
 * One consecutive-failure classifier shared by authority and delivery loops that must tell a
 * transient transport error (ALB/AG 502/504) apart from an authoritative
 * fence: registration heartbeat (#220), claim renew (#222), and the
 * message-channel delivery renew loop. Folding the threshold into one module
 * keeps their tolerance from drifting apart (ADR-0026 D3) — an authoritative
 * (non-transport) response must still fence immediately by calling the
 * caller's own fence path; this class only governs the soft-retry window
 * around transport failures.
 */
export class RuntimeTransportFailurePolicy {
  private consecutiveFailures = 0;

  constructor(private readonly threshold = 3) {
    if (!Number.isSafeInteger(threshold) || threshold <= 0) {
      throw new Error("transport failure threshold must be a positive integer");
    }
  }

  /** Any authoritative (non-transport) outcome resets the tolerance window. */
  recordSuccess(): void {
    this.consecutiveFailures = 0;
  }

  /**
   * Records one transport failure. Returns false while still within the
   * soft-retry tolerance (caller keeps the claim/lease live); returns true
   * once the threshold is crossed, at which point the counter resets and the
   * caller must treat this as an authoritative fence.
   */
  recordFailure(): boolean {
    this.consecutiveFailures += 1;
    if (this.consecutiveFailures < this.threshold) return false;
    this.consecutiveFailures = 0;
    return true;
  }
}
