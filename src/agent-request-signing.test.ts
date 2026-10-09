import { generateKeyPairSync, sign, verify } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { agentRequestAuthHeaders, authenticatedAgentFetch, bodyHashHex } from
  "./agent-request-signing.js";

describe("Gateway body-bound Ed25519 signing", () => {
  it("signs exact method/path/body bytes without exposing private keys to the SDK", () => {
    const keys = generateKeyPairSync("ed25519");
    const identity = { publicKey: keys.publicKey.export({ format: "der", type: "spki" }).subarray(12),
      sign: (message: Uint8Array) => sign(null, message, keys.privateKey) };
    const headers = agentRequestAuthHeaders("post", "/api/v1/agents/sync", identity,
      '{"agents":[]}', () => 1_700_000_000_000, () => "nonce-1");
    const preimage = `POST|/api/v1/agents/sync||${bodyHashHex('{"agents":[]}')}|1700000000|nonce-1`;
    expect(verify(null, Buffer.from(preimage), keys.publicKey,
      Buffer.from(headers["X-Agent-Signature"]!, "base64"))).toBe(true);
    expect(verify(null, Buffer.from(preimage.replace("agents/sync", "agents/other")),
      keys.publicKey, Buffer.from(headers["X-Agent-Signature"]!, "base64"))).toBe(false);
  });

  it("signs decoded path, rejects non-byte body, and never follows an origin redirect", async () => {
    const keys = generateKeyPairSync("ed25519");
    const signed: Uint8Array[] = [];
    const identity = { publicKey: new Uint8Array(32),
      sign: (message: Uint8Array) => { signed.push(message); return sign(null, message, keys.privateKey); } };
    const original = globalThis.fetch;
    try {
      globalThis.fetch = vi.fn(async (_input, init) => {
        expect(init?.redirect).toBe("error");
        return new Response("{}", { status: 200 });
      });
      await authenticatedAgentFetch("https://gateway.test/api/v1/%61gents/sync", identity,
        { method: "POST", body: "{}" });
      expect(Buffer.from(signed[0]!).toString("utf8")).toContain("/api/v1/agents/sync");
      await expect(authenticatedAgentFetch("https://gateway.test/route", identity,
        { method: "POST", body: new Blob(["unsafe"]) })).rejects.toThrow("exact signing");
    } finally {
      globalThis.fetch = original;
    }
  });
});
