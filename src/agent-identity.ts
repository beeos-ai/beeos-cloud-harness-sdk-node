import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { agentRequestAuthHeaders, type AgentRequestIdentity } from "./agent-request-signing.js";

/** Raw Ed25519 key material of a harness instance (32-byte public + 32-byte seed). */
export interface AgentKeyPair {
  publicKey: Uint8Array;
  privateKey: Uint8Array;
}

const ED25519_PKCS8_PREFIX = Buffer.from("302e020100300506032b657004220420", "hex");

function privateKeyObject(raw: Uint8Array): crypto.KeyObject {
  const der = raw.length === 32 ? Buffer.concat([ED25519_PKCS8_PREFIX, Buffer.from(raw)]) : Buffer.from(raw);
  return crypto.createPrivateKey({ key: der, format: "der", type: "pkcs8" });
}

function derivePublicKey(privateKey: Uint8Array): Uint8Array {
  const spki = crypto.createPublicKey(privateKeyObject(privateKey)).export({ type: "spki", format: "der" }) as Buffer;
  return new Uint8Array(spki.length === 44 ? spki.subarray(12) : spki);
}

/**
 * Load the instance key file; creates it (0600) when absent. Accepts the JSON
 * `{publicKey, privateKey}` form or a bare base64 32-byte seed.
 */
export function loadAgentKeyPair(filePath: string): AgentKeyPair {
  if (!fs.existsSync(filePath)) return generateAgentKeyPair(filePath);
  const raw = fs.readFileSync(filePath, "utf-8").trim();
  try {
    const data = JSON.parse(raw) as { publicKey?: string; privateKey?: string };
    if (data.publicKey && data.privateKey) {
      return {
        publicKey: new Uint8Array(Buffer.from(data.publicKey, "base64")),
        privateKey: new Uint8Array(Buffer.from(data.privateKey, "base64")),
      };
    }
  } catch { /* not JSON: bare base64 seed */ }
  const seed = new Uint8Array(Buffer.from(raw, "base64"));
  if (seed.length === 32) return { publicKey: derivePublicKey(seed), privateKey: seed };
  throw new Error(`Invalid key file format at ${filePath}`);
}

function generateAgentKeyPair(saveTo: string): AgentKeyPair {
  const kp = crypto.generateKeyPairSync("ed25519", {
    publicKeyEncoding: { type: "spki", format: "der" },
    privateKeyEncoding: { type: "pkcs8", format: "der" },
  });
  const pub = kp.publicKey as unknown as Buffer;
  const priv = kp.privateKey as unknown as Buffer;
  const publicKey = new Uint8Array(pub.length === 44 ? pub.subarray(12) : pub);
  const privateKey = new Uint8Array(priv.length === 48 ? priv.subarray(16) : priv);
  fs.mkdirSync(path.dirname(saveTo), { recursive: true });
  fs.writeFileSync(saveTo, JSON.stringify({
    publicKey: Buffer.from(publicKey).toString("base64"),
    privateKey: Buffer.from(privateKey).toString("base64"),
  }, null, 2), { mode: 0o600 });
  return { publicKey, privateKey };
}

export function signAgentMessage(message: string | Uint8Array, privateKey: Uint8Array): Uint8Array {
  return new Uint8Array(crypto.sign(null, Buffer.from(message), privateKeyObject(privateKey)));
}

/** Adapter used by every signed Gateway request and by registration proofs. */
export function agentIdentityFromKeyPair(keys: AgentKeyPair): AgentRequestIdentity {
  return { publicKey: keys.publicKey, sign: (message) => signAgentMessage(message, keys.privateKey) };
}

/** Gateway v2 auth headers for a request the caller sends itself (e.g. a WebSocket upgrade). */
export function agentAuthHeaders(
  method: string,
  urlPath: string,
  keys: AgentKeyPair,
  body?: string | Uint8Array | null,
): Record<string, string> {
  return agentRequestAuthHeaders(method, urlPath, agentIdentityFromKeyPair(keys), body);
}
