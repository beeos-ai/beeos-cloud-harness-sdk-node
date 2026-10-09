import { createHash, randomUUID } from "node:crypto";

/** The host owns the private key; the SDK owns the exact Gateway v2 preimage. */
export interface AgentRequestIdentity {
  publicKey: Uint8Array;
  sign(message: Uint8Array): Uint8Array;
}

export const EMPTY_BODY_SHA256 =
  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

export function bodyHashHex(body?: string | Uint8Array | null): string {
  const bytes = body == null ? Buffer.alloc(0)
    : typeof body === "string" ? Buffer.from(body, "utf8") : Buffer.from(body);
  return createHash("sha256").update(bytes).digest("hex");
}

export function agentRequestAuthHeaders(
  method: string,
  path: string,
  identity: AgentRequestIdentity,
  body?: string | Uint8Array | null,
  now: () => number = Date.now,
  nonce: () => string = randomUUID,
): Record<string, string> {
  const timestamp = Math.floor(now() / 1000).toString();
  const requestNonce = nonce();
  const preimage = `${method.toUpperCase()}|${path}||${bodyHashHex(body)}|${timestamp}|${requestNonce}`;
  return {
    "X-Agent-Public-Key": Buffer.from(identity.publicKey).toString("base64"),
    "X-Agent-Signature": Buffer.from(identity.sign(Buffer.from(preimage))).toString("base64"),
    "X-Agent-Timestamp": timestamp,
    "X-Agent-Nonce": requestNonce,
  };
}

export async function authenticatedAgentFetch(
  input: string | URL,
  identity: AgentRequestIdentity,
  init?: RequestInit,
): Promise<Response> {
  const url = typeof input === "string" ? new URL(input) : input;
  const method = init?.method?.toUpperCase() ?? "GET";
  let body: string | Uint8Array | undefined;
  if (init?.body != null) {
    if (typeof init.body !== "string" && !(init.body instanceof Uint8Array)) {
      throw new Error("agent-authenticated request body must be string or Uint8Array for exact signing");
    }
    body = init.body;
  }
  const headers = new Headers(init?.headers);
  const auth = agentRequestAuthHeaders(method, decodeURIComponent(url.pathname), identity, body);
  for (const [key, value] of Object.entries(auth)) headers.set(key, value);
  headers.delete("X-Agent-Body-SHA256");
  const response = await fetch(url, { ...init, method, headers, redirect: "error" });
  if (response.url && new URL(response.url).origin !== url.origin) {
    throw new Error("agent-authenticated response origin changed");
  }
  return response;
}
