import { afterEach, describe, expect, it } from "vitest";
import { createServer, type Server } from "node:http";
import { bindExistingRuntimeChatReply } from "./runtime-chat-retained.js";
import { createRuntimeChatHttpPort } from "./runtime-chat-http.js";
import type { RuntimeChatLease } from "./runtime-chat.js";

const servers: Server[] = [];
afterEach(async () => { await Promise.all(servers.splice(0).map(server =>
  new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve())))); });

const proof = { clearIntentId: "clear", activeRunRegistrationId: "task",
  historyGeneration: "3", runtimeEpoch: "7" };
const seed = '[{"type":"custom","n":9007199254740993,"decimal":1.234567890123456789,"minus":-0}]';
async function fixture(errorBody: unknown = { error: { code: "history_clear_in_progress", metadata: proof } }, status = 409) {
  let lease: RuntimeChatLease = { instanceId: "instance", handlerIdentity: "handler", runtimeEpoch: "7",
    leaseId: "lease", runtimeLeaseCredential: "fixture-only", leaseExpiresAt: "2099-01-01T00:00:00Z" };
  let lineage = "journal:2";
  let clearing = false;
  let wrongReply = false;
  let row = { body: "你🙂", state: "streaming", stopReason: undefined as string | undefined, parts: seed };
  let onRequest: ((method: string) => void) | undefined;
  const writes: string[] = [];
  const auth: string[] = [];
  const server = createServer(async (req, res) => {
    const method = req.method!;
    auth.push(String(req.headers.authorization));
    let body = ""; for await (const bytes of req) body += bytes;
    onRequest?.(method);
    if (method === "PATCH") {
      writes.push(body);
      const patch = JSON.parse(body);
      if (!patch.state && clearing) {
        res.writeHead(status, { "content-type": "application/json" });
        res.end(JSON.stringify(errorBody)); return;
      }
      if (row.state !== "streaming" && (patch.state !== row.state ||
          patch.stop_reason !== row.stopReason || (patch.body !== undefined && patch.body !== row.body))) {
        res.writeHead(409); res.end('{"error":{"code":"conflict"}}'); return;
      }
      if (patch.body_append !== undefined) {
        expect(patch.body_from).toBe(new TextEncoder().encode(row.body).byteLength);
        row.body += patch.body_append;
      }
      if (patch.body !== undefined) row.body = patch.body;
      if (patch.state) { row.state = patch.state; row.stopReason = patch.stop_reason; }
    }
    res.setHeader("content-type", "application/json");
    res.end(`{"id":"reply","conversation_id":"conversation","type":"agent_reply","sender":"instance","reply_to":"${wrongReply ? "other-request" : "task-request"}","body":${JSON.stringify(row.body)},"state":${JSON.stringify(row.state)},"parts":${row.parts},"created_at":"2026-09-30T00:00:00Z"${row.stopReason ? `,"stop_reason":${JSON.stringify(row.stopReason)}` : ""}}`);
  });
  servers.push(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const http = createRuntimeChatHttpPort({ baseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}` });
  const stream = await bindExistingRuntimeChatReply({ authority: { currentLease: () => lease }, http,
    binding: { conversationId: "conversation", messageId: "reply", taskRequestMessageId: "task-request",
      mutationKey: n => `attempt:${n}`,
      assertAttempt() { if (lineage !== "journal:2") throw new Error("original lineage changed"); },
      assertPreparedClear(p) { if (p.activeRunRegistrationId !== proof.activeRunRegistrationId ||
        p.historyGeneration !== proof.historyGeneration || p.runtimeEpoch !== proof.runtimeEpoch) {
        throw new Error("original registered attempt mismatch");
      } } } });
  return { stream, http, writes, auth, clear: () => { clearing = true; },
    changeLease: (patch: Partial<RuntimeChatLease>) => { lease = { ...lease, ...patch }; },
    changeLineage: () => { lineage = "journal:3"; }, wrongReply: () => { wrongReply = true; },
    afterRequest: (callback: (method: string) => void) => { onRequest = callback; },
    setRow: (patch: Partial<typeof row>) => { row = { ...row, ...patch }; }, get row() { return row; } };
}

describe("prepared native clear: actual localhost HTTP parser and retained chain", () => {
  it("reconciles only typed exact proof, stops queued previews and permits original terminal", async () => {
    const f = await fixture({ error: { code: "history_clear_in_progress", metadata: proof,
      type: "api", message: "clearing", request_id: "diagnostic" } });
    f.clear(); f.stream.appendBody("discard-one"); f.stream.appendBody("discard-two");
    await f.stream.flush(); f.stream.appendBody("discard-three");
    await f.stream.finalize({ stopReason: "end_turn" });
    expect(f.writes).toHaveLength(2); expect(f.row.body).toBe("你🙂"); expect(f.row.parts).toBe(seed);
    expect(f.writes[1]).not.toContain("body_append"); expect(f.writes[1]).not.toContain('"parts"');
  });
  it.each([
    ["ordinary same code", { error: { code: "history_clear_in_progress" } }, 409],
    ["missing metadata field", { error: { code: "history_clear_in_progress", metadata: { ...proof, runtimeEpoch: undefined } } }, 409],
    ["extra metadata field", { error: { code: "history_clear_in_progress", metadata: { ...proof, extra: "not-proof" } } }, 409],
    ["number metadata", { error: { code: "history_clear_in_progress", metadata: { ...proof, runtimeEpoch: 7 } } }, 409],
    ["data instead of metadata", { error: { code: "history_clear_in_progress", data: proof } }, 409],
    ["ordinary 403", { error: { code: "history_clear_in_progress", metadata: proof } }, 403],
    ["different code", { error: { code: "conflict", metadata: proof } }, 409],
  ] as const)("does not clear failed chain for %s", async (_label, body, status) => {
    const f = await fixture(body, status); f.clear(); f.stream.appendBody("discard");
    await expect(f.stream.flush()).rejects.toThrow("runtime chat request failed");
    await expect(f.stream.finalize({ stopReason: "end_turn" })).rejects.toThrow("runtime chat request failed");
    expect(f.writes).toHaveLength(1); expect(f.row.state).toBe("streaming");
  });
  it("a later full prepared proof cannot clear an unrelated prior failed chain", async () => {
    const body = { error: { code: "conflict", metadata: proof } };
    const f = await fixture(body); f.clear(); f.stream.appendBody("first");
    await expect(f.stream.flush()).rejects.toThrow("runtime chat request failed");
    body.error.code = "history_clear_in_progress";
    f.stream.appendBody("second");
    await expect(f.stream.finalize({ stopReason: "end_turn" })).rejects.toThrow("runtime chat request failed");
    expect(f.writes).toHaveLength(1); expect(f.row.state).toBe("streaming");
  });
  it.each(["activeRunRegistrationId", "historyGeneration", "runtimeEpoch"] as const)("rejects changed proof %s", async key => {
    const f = await fixture({ error: { code: "history_clear_in_progress", metadata: { ...proof, [key]: "999" } } });
    f.clear(); f.stream.appendBody("discard"); await expect(f.stream.flush()).rejects.toThrow("registered attempt mismatch");
    expect(f.writes).toHaveLength(1);
  });
  it("requires the same original reply from authoritative GET", async () => {
    const f = await fixture(); f.clear(); f.wrongReply(); f.stream.appendBody("discard");
    await expect(f.stream.flush()).rejects.toThrow("binding mismatch"); expect(f.writes).toHaveLength(1);
  });
  it.each(["instanceId", "handlerIdentity", "runtimeEpoch", "leaseId"] as const)("rejects %s swap across refusal await", async key => {
    const f = await fixture(); f.clear(); f.afterRequest(method => { if (method === "PATCH") f.changeLease({ [key]: "changed" }); });
    f.stream.appendBody("discard"); await expect(f.stream.flush()).rejects.toThrow("lease fence");
    expect(f.writes).toHaveLength(1);
  });
  it("rejects original journal lineage drift", async () => {
    const f = await fixture(); f.clear(); f.afterRequest(method => { if (method === "PATCH") f.changeLineage(); });
    f.stream.appendBody("discard"); await expect(f.stream.flush()).rejects.toThrow("lineage changed");
  });
  it("rejects expired current lease before a preview fetch", async () => {
    const f = await fixture(); f.changeLease({ leaseExpiresAt: "2000-01-01T00:00:00Z" });
    expect(() => f.stream.appendBody("discard")).toThrow("lease fence"); expect(f.writes).toHaveLength(0);
  });
  it("allows same-identity credential renewal during refusal and original GET", async () => {
    const f = await fixture(); f.clear(); f.afterRequest(method => {
      if (method === "PATCH") f.changeLease({ runtimeLeaseCredential: "renewed", leaseExpiresAt: "2099-02-01T00:00:00Z" });
    });
    f.stream.appendBody("discard"); await f.stream.flush(); await f.stream.finalize({ stopReason: "end_turn" });
    expect(f.auth.slice(-2)).toEqual(["Bearer renewed", "Bearer renewed"]);
  });
  it("preserves an already recovery-failed original body/parts and only permits its exact terminal", async () => {
    const f = await fixture(); f.clear();
    // The prepared refusal is generated while streaming; recovery wins only
    // after that response, before the authoritative reconciliation GET.
    f.afterRequest(method => { if (method === "GET") f.setRow({ state: "failed", stopReason: "error", body: "recovery failed 原内容" }); });
    f.stream.appendBody("discard"); await f.stream.flush();
    expect((await f.stream.fail({ stopReason: "error" })).state).toBe("failed");
    expect(f.row.body).toBe("recovery failed 原内容"); expect(f.row.parts).toBe(seed);
  });
  it("terminal comparison ignores unrelated format/metadata but preserves raw numeric parts", async () => {
    const f = await fixture(); const row = await f.stream.finalize({ stopReason: "end_turn" });
    f.stream.assertTerminalSnapshot({ ...row, rawText: ` { "parts" : ${seed}, "unrelated":true } ` });
    expect(() => f.stream.assertTerminalSnapshot({ ...row, rawText: '{"parts":[{"n":9007199254740992}]}' })).toThrow("terminal snapshot changed");
    expect(() => f.stream.assertTerminalSnapshot({ ...row, replyTo: "wrong" })).toThrow("terminal snapshot changed");
  });
});
