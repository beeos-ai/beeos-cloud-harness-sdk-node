import { createHash, createPublicKey, verify, type JsonWebKey as CryptoJsonWebKey,
  type KeyObject } from "node:crypto";
import type {
  AnyRuntimeExecutionGrantClaims,
  CoreExecutionAuthorizationVerifier,
  RuntimeExecutionBoundaryAssertionClaims,
  RuntimeExecutionGrantHeader,
  VerifiedDecodedExecutionBoundary,
  VerifiedDecodedExecutionGrant,
  RuntimeInvocationValidationContext,
  RuntimeInvocationFence,
} from "@beeos-ai/beeos-types/runtime";
import { validateRuntimeExecutionGrant, CLOUD_RUNTIME_EXECUTION_GRANT_DOMAIN } from "@beeos-ai/beeos-types/runtime";
import { STARTUP_BACKOFF, retryWithBackoff, type BackoffPolicy } from "./retry.js";

interface RuntimeJsonWebKey extends CryptoJsonWebKey {
  kid?: string; alg?: string; kty?: string; use?: string;
}
interface JsonWebKeySet {
  issuer?: string;
  audience?: string;
  purpose?: string;
  maxClaimTtlSeconds?: number;
  keys: RuntimeJsonWebKey[];
}

export interface RuntimeJwksDocumentProvenance {
  sourceUrl: string;
  etag: string;
  issuer: string;
  audience: string;
  purpose: string;
  maxClaimTtlSeconds: number;
}

export interface RuntimeJwksKeyMaterial {
  jwkThumbprint: string;
  spkiSha256: string;
}

export type RuntimeJwksFetch = (url: string | URL, init?: RequestInit) => Promise<Response>;

/** RS256-only JWKS verifier with bounded cache lifetime and explicit refresh. */
export class RuntimeJwksVerifier implements CoreExecutionAuthorizationVerifier {
  private keys = new Map<string, KeyObject>();
  private expiresAt = 0;
  private inflight?: Promise<void>;
  private provenance?: RuntimeJwksDocumentProvenance;
  private keyMaterials: readonly RuntimeJwksKeyMaterial[] = Object.freeze([]);
  readonly keysetUrl: string;

  constructor(
    private readonly jwksUrl: string,
    private readonly fetcher: RuntimeJwksFetch = fetch,
    private readonly ttlMs = 5 * 60_000,
    private readonly now: () => number = Date.now,
  ) { this.keysetUrl = new URL(jwksUrl).toString(); }

  async refresh(force = false): Promise<void> {
    if (!force && this.keys.size > 0 && this.now() < this.expiresAt) return;
    if (this.inflight) return await this.inflight;
    this.inflight = this.load();
    try { await this.inflight; } finally { this.inflight = undefined; }
  }

  hasKid(kid: string): boolean { return this.keys.has(kid); }
  trustedKids(): readonly string[] { return Object.freeze([...this.keys.keys()].sort()); }
  trustedKeyMaterials(): readonly RuntimeJwksKeyMaterial[] { return this.keyMaterials; }
  documentProvenance(): RuntimeJwksDocumentProvenance | undefined { return this.provenance; }

  verifyAndDecodeGrant(compactJwt: string): VerifiedDecodedExecutionGrant | null {
    const decoded = this.verifyCompact(compactJwt);
    if (!decoded) return null;
    return { header: decoded.header, claims: decoded.claims as AnyRuntimeExecutionGrantClaims,
      compactHash: sha256(compactJwt) };
  }

  /**
   * The only grant entry point runtime authorities may use. Signature success
   * alone is not authorization: the frozen validator binds every caller,
   * request/hash/target/deadline/journal/fence field to the persisted
   * invocation context.
   */
  verifyAndValidateGrant(compactJwt: string, expected: RuntimeInvocationValidationContext & {
    activeFence: RuntimeInvocationFence;
  }): VerifiedDecodedExecutionGrant | null {
    const decoded = this.verifyAndDecodeGrant(compactJwt);
    if (!decoded) return null;
    return validateRuntimeExecutionGrant(decoded.claims, expected).length === 0 ? decoded : null;
  }

  verifyAndDecodeBoundary(compactJwt: string): VerifiedDecodedExecutionBoundary | null {
    const decoded = this.verifyCompact(compactJwt);
    if (!decoded) return null;
    return { header: decoded.header, claims: decoded.claims as RuntimeExecutionBoundaryAssertionClaims,
      compactHash: sha256(compactJwt) };
  }

  private async load(): Promise<void> {
    const response = await this.fetcher(this.jwksUrl, {
      signal: AbortSignal.timeout(10_000), redirect: "error",
    });
    if (!response.ok) throw new Error(`JWKS fetch failed: HTTP ${response.status}`);
    const body = await response.json() as JsonWebKeySet;
    const next = new Map<string, KeyObject>();
    const materials: RuntimeJwksKeyMaterial[] = [];
    for (const jwk of body.keys ?? []) {
      if (jwk.kty !== "RSA" || jwk.use !== "sig" || jwk.alg !== "RS256" ||
          typeof jwk.kid !== "string" || !jwk.kid) continue;
      if (next.has(jwk.kid)) throw new Error(`JWKS contains duplicate kid: ${jwk.kid}`);
      const key = createPublicKey({ key: jwk, format: "jwk" });
      if ((key.asymmetricKeyDetails?.modulusLength ?? 0) < 2048) continue;
      const normalized = key.export({ format: "jwk" });
      if (normalized.kty !== "RSA" || typeof normalized.n !== "string" ||
          typeof normalized.e !== "string") continue;
      next.set(jwk.kid, key);
      materials.push(Object.freeze({
        jwkThumbprint: sha256(JSON.stringify({ e: normalized.e, kty: "RSA", n: normalized.n })),
        spkiSha256: createHash("sha256").update(
          key.export({ format: "der", type: "spki" }),
        ).digest("hex"),
      }));
    }
    if (next.size === 0) throw new Error("JWKS contains no trusted RS256 keys");
    const sourceUrl = response.url ? new URL(response.url).toString() : this.keysetUrl;
    if (sourceUrl !== this.keysetUrl) throw new Error("JWKS response provenance URL mismatch");
    this.keys = next;
    this.keyMaterials = Object.freeze(materials.sort((a, b) =>
      a.jwkThumbprint.localeCompare(b.jwkThumbprint) || a.spkiSha256.localeCompare(b.spkiSha256)));
    this.provenance = Object.freeze({ sourceUrl, etag: response.headers.get("etag") ?? "",
      issuer: typeof body.issuer === "string" ? body.issuer : "",
      audience: typeof body.audience === "string" ? body.audience : "",
      purpose: typeof body.purpose === "string" ? body.purpose : "",
      maxClaimTtlSeconds: typeof body.maxClaimTtlSeconds === "number"
        ? body.maxClaimTtlSeconds : Number.NaN });
    this.expiresAt = this.now() + this.ttlMs;
  }

  private verifyCompact(compactJwt: string): { header: RuntimeExecutionGrantHeader; claims: object } | null {
    const parts = compactJwt.split(".");
    if (parts.length !== 3) return null;
    try {
      if (!parts.every(isCanonicalBase64Url)) return null;
      const header = JSON.parse(decodeBase64Url(parts[0]!)) as RuntimeExecutionGrantHeader;
      if (header.alg !== "RS256" || header.typ !== "JWT" || typeof header.kid !== "string") return null;
      const key = this.keys.get(header.kid);
      if (!key) return null;
      const signed = Buffer.from(`${parts[0]}.${parts[1]}`);
      const signature = Buffer.from(parts[2]!, "base64url");
      if (!verify("RSA-SHA256", signed, key, signature)) return null;
      const claims = JSON.parse(decodeBase64Url(parts[1]!)) as unknown;
      if (claims === null || typeof claims !== "object" || Array.isArray(claims)) return null;
      return { header, claims };
    } catch { return null; }
  }
}

function isCanonicalBase64Url(value: string): boolean {
  if (!value || !/^[A-Za-z0-9_-]+$/.test(value)) return false;
  const decoded = Buffer.from(value, "base64url");
  return decoded.toString("base64url") === value;
}

function decodeBase64Url(value: string): string {
  return Buffer.from(value, "base64url").toString("utf8");
}

function sha256(value: string): `${string}` {
  return createHash("sha256").update(value).digest("hex") as `${string}`;
}

// --- purpose-bound verifiers (formerly Claw split-runtime-authorization-verifier) ---
const GATEWAY_GRANT_PROVENANCE = Object.freeze({ issuer: "beeos-gateway",
  audience: "beeos-runtime-command", purpose: "runtime-execution-grant",
  maxClaimTtlSeconds: 60 });
const CLOUD_GRANT_PROVENANCE = Object.freeze({ issuer: CLOUD_RUNTIME_EXECUTION_GRANT_DOMAIN.issuer,
  audience: CLOUD_RUNTIME_EXECUTION_GRANT_DOMAIN.audience,
  purpose: CLOUD_RUNTIME_EXECUTION_GRANT_DOMAIN.purpose, maxClaimTtlSeconds: 60 });
const MESSAGE_BOUNDARY_PROVENANCE = Object.freeze({ issuer: "message-service",
  audience: "beeos-runtime-execution-boundary", purpose: "runtime-execution-boundary",
  maxClaimTtlSeconds: 60 });

export class GatewayGrantJwksVerifier {
  readonly purpose = "gateway-runtime-grant" as const;
  readonly keysetProvenance: string;
  constructor(private readonly verifier: RuntimeJwksVerifier) {
    this.keysetProvenance = verifier.keysetUrl;
  }
  async refresh(force = false): Promise<void> {
    await this.verifier.refresh(force);
    assertResponseProvenance(this.verifier, GATEWAY_GRANT_PROVENANCE);
  }
  trustedKeyMaterials() { return this.verifier.trustedKeyMaterials(); }
  responseProvenance() { return this.verifier.documentProvenance(); }
  verifyAndDecodeGrant(jwt: string): VerifiedDecodedExecutionGrant | null {
    const decoded = this.verifier.verifyAndDecodeGrant(jwt);
    return decoded?.claims.iss === "beeos-gateway" &&
      decoded.claims.aud === "beeos-runtime-command" &&
      (decoded.claims as unknown as { purpose?: unknown }).purpose === "runtime-execution-grant"
      ? decoded : null;
  }
}

/** Cloud commands have a separate signing purpose; legacy user grants cannot cross this boundary. */
export class CloudGatewayGrantJwksVerifier {
  readonly keysetProvenance: string;
  constructor(private readonly verifier: RuntimeJwksVerifier) {
    this.keysetProvenance = verifier.keysetUrl;
  }
  async refresh(force = false): Promise<void> {
    await this.verifier.refresh(force);
    assertResponseProvenance(this.verifier, CLOUD_GRANT_PROVENANCE);
  }
  verifyAndDecodeGrant(jwt: string): VerifiedDecodedExecutionGrant | null {
    const decoded = this.verifier.verifyAndDecodeGrant(jwt);
    const claims = decoded?.claims as unknown as { iss?: unknown; aud?: unknown; purpose?: unknown } | undefined;
    return claims?.iss === CLOUD_RUNTIME_EXECUTION_GRANT_DOMAIN.issuer &&
      claims.aud === CLOUD_RUNTIME_EXECUTION_GRANT_DOMAIN.audience &&
      claims.purpose === CLOUD_RUNTIME_EXECUTION_GRANT_DOMAIN.purpose
      ? decoded : null;
  }
}

export class MessageServiceBoundaryJwksVerifier {
  readonly purpose = "message-service-execution-boundary" as const;
  readonly keysetProvenance: string;
  constructor(private readonly verifier: RuntimeJwksVerifier) {
    this.keysetProvenance = verifier.keysetUrl;
  }
  async refresh(force = false): Promise<void> {
    await this.verifier.refresh(force);
    assertResponseProvenance(this.verifier, MESSAGE_BOUNDARY_PROVENANCE);
  }
  trustedKeyMaterials() { return this.verifier.trustedKeyMaterials(); }
  responseProvenance() { return this.verifier.documentProvenance(); }
  verifyAndDecodeBoundary(jwt: string): VerifiedDecodedExecutionBoundary | null {
    const decoded = this.verifier.verifyAndDecodeBoundary(jwt);
    return decoded?.claims.iss === "message-service" &&
      decoded.claims.aud === "beeos-runtime-execution-boundary" ? decoded : null;
  }
}

/** One Core verifier composed from two purpose-bound, provenance-distinct keysets. */
export class SplitIssuerRuntimeAuthorizationVerifier implements CoreExecutionAuthorizationVerifier {
  constructor(readonly grant: GatewayGrantJwksVerifier,
    readonly boundary: MessageServiceBoundaryJwksVerifier) {
    if (grant.keysetProvenance === boundary.keysetProvenance) {
      throw new Error("grant and boundary JWKS keysets require distinct provenance");
    }
  }
  async refresh(force = false): Promise<void> {
    await Promise.all([this.grant.refresh(force), this.boundary.refresh(force)]);
    const grantProvenance = this.grant.responseProvenance();
    const boundaryProvenance = this.boundary.responseProvenance();
    if (!grantProvenance || !boundaryProvenance ||
        grantProvenance.sourceUrl === boundaryProvenance.sourceUrl ||
        grantProvenance.etag === boundaryProvenance.etag) {
      throw new Error("grant and boundary JWKS responses require distinct provenance");
    }
    const boundaryMaterial = new Set(this.boundary.trustedKeyMaterials().flatMap((key) =>
      [key.jwkThumbprint, key.spkiSha256]));
    if (this.grant.trustedKeyMaterials().some((key) =>
      boundaryMaterial.has(key.jwkThumbprint) || boundaryMaterial.has(key.spkiSha256))) {
      throw new Error("grant and boundary JWKS keysets reuse public-key material");
    }
  }
  verifyAndDecodeGrant(jwt: string) { return this.grant.verifyAndDecodeGrant(jwt); }
  verifyAndDecodeBoundary(jwt: string) { return this.boundary.verifyAndDecodeBoundary(jwt); }
}

function assertResponseProvenance(verifier: RuntimeJwksVerifier,
  expected: { issuer: string; audience: string; purpose: string; maxClaimTtlSeconds: number }): void {
  const actual = verifier.documentProvenance();
  if (!actual || !actual.etag || actual.sourceUrl !== verifier.keysetUrl ||
      actual.issuer !== expected.issuer || actual.audience !== expected.audience ||
      actual.purpose !== expected.purpose ||
      actual.maxClaimTtlSeconds !== expected.maxClaimTtlSeconds) {
    throw new Error(`JWKS response provenance mismatch for ${expected.purpose}`);
  }
}

/**
 * Pull a JWKS-backed verifier into its first loaded state, tolerating a control
 * plane that is still rolling out. Resolves `true` once keys are loaded and
 * `false` when the budget is spent; callers keep working either way because
 * every verification path calls `refresh()` first. A cold control plane is
 * therefore a delay, never a permanent capability fence.
 */
export async function warmGrantJwks(
  verifier: { refresh(force?: boolean): Promise<void> },
  options: { policy?: BackoffPolicy; signal?: AbortSignal;
    onRetry?: (info: { attempt: number; delayMs: number; error: unknown }) => void } = {},
): Promise<boolean> {
  try {
    await retryWithBackoff(async () => await verifier.refresh(true), {
      policy: options.policy ?? STARTUP_BACKOFF, signal: options.signal,
      isRetryable: () => true, onRetry: options.onRetry,
    });
    return true;
  } catch (error) {
    if (options.signal?.aborted) throw error;
    return false;
  }
}

/** The Cloud grant keyset every managed harness trusts: Agent Gateway's purpose-bound JWKS proxy. */
export function createCloudGrantVerifier(agentGatewayUrl: string, fetcher?: RuntimeJwksFetch): CloudGatewayGrantJwksVerifier {
  const base = new URL(agentGatewayUrl).toString().replace(/\/+$/, "");
  return new CloudGatewayGrantJwksVerifier(new RuntimeJwksVerifier(
    `${base}/.well-known/runtime-execution-grant-jwks.json`, fetcher));
}
