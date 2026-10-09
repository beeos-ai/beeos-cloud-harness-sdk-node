import { describe, expect, it } from "vitest";
import { bindExistingRuntimeChatReply } from "./runtime-chat-retained.js";
import { createRuntimeChatHttpPort } from "./runtime-chat-http.js";
import type { RuntimeChatLease } from "./runtime-chat.js";

function fixture() {
  let lease: RuntimeChatLease = { instanceId: "instance", handlerIdentity: "handler",
    runtimeEpoch: "epoch1", leaseId: "lease1", runtimeLeaseCredential: "credential1",
    leaseExpiresAt: "2099-01-01T00:00:00Z" };
  let attemptValid = true;
  let body = "你🙂";
  let state = "streaming";
  let stopReason: string | undefined;
  let parts = '[{"type":"custom","nested":{"large":9007199254740993,"decimal":1.234567890123456789,"minus":-0,"exp":1e+30}}]';
  const calls: { method: string; body?: Record<string, unknown>; wire?: string; auth: string | null }[] = [];
  let afterRequest: ((method: string) => void) | undefined;
  let dropTerminal = false;
  let persistTerminal = true;
  let wrongRequest = false;
  let dropTerminalParts = false;
  const http = createRuntimeChatHttpPort({ baseUrl: "https://local.invalid",
    fetchImpl: async (_url, init) => {
      const method = init?.method ?? "GET";
      const wire = typeof init?.body === "string" ? init.body : undefined;
      const patch = wire ? JSON.parse(wire) : undefined;
      calls.push({ method, body: patch, wire, auth: new Headers(init?.headers).get("authorization") });
      if (method === "PATCH") {
        if (patch.body_append !== undefined) {
          expect(patch.body_from).toBe(new TextEncoder().encode(body).byteLength);
          body += patch.body_append;
        }
        if (patch.body !== undefined) body = patch.body;
        if (patch.parts !== undefined) parts = wire!.slice(wire!.indexOf('"parts":') + 8, -1);
        if (patch.state && persistTerminal) {
          state = patch.state; stopReason = patch.stop_reason;
          if (dropTerminalParts) parts = "[]";
        }
      }
      afterRequest?.(method);
      if (method === "PATCH" && patch.state && dropTerminal) throw new Error("response lost");
      return new Response(`{"id":"reply","conversation_id":"conversation","type":"agent_reply","sender":"instance","created_at":"2026-09-30T00:00:00.123456789Z","reply_to":"${wrongRequest ? "wrong" : "task-request"}","body":${JSON.stringify(body)},"state":${JSON.stringify(state)},${stopReason ? `"stop_reason":${JSON.stringify(stopReason)},` : ""}"parts":${parts}}`, { status: 200 });
    } });
  const bind = () => bindExistingRuntimeChatReply({ authority: { currentLease: () => lease }, http,
    binding: { conversationId: "conversation", messageId: "reply", taskRequestMessageId: "task-request",
      assertAttempt: () => { if (!attemptValid) throw new Error("journal attempt changed"); },
      mutationKey: n => `attempt:${n}` } });
  return { bind, http, calls, get lease() { return lease; },
    changeLease: (patch: Partial<RuntimeChatLease>) => { lease = { ...lease, ...patch }; },
    changeAttempt: () => { attemptValid = false; },
    afterRequest: (hook: (method: string) => void) => { afterRequest = hook; },
    dropTerminal: (persist: boolean) => { dropTerminal = true; persistTerminal = persist; },
    wrongRequest: () => { wrongRequest = true; },
    dropTerminalParts: () => { dropTerminalParts = true; } };
}

describe("retained reply binding", () => {
  it("GET seeds UTF8 offset, only PATCH writes, unchanged parts omitted", async () => {
    const f = fixture(); const stream = await f.bind();
    stream.appendBody("!"); await stream.flush(); await stream.finalize({ stopReason: "end_turn" });
    expect(f.calls.map(c => c.method)).toEqual(["GET", "PATCH", "PATCH", "GET"]);
    expect(f.calls[1].body?.body_from).toBe(7);
    expect(f.calls.filter(c => c.method === "PATCH").every(c => !("parts" in c.body!))).toBe(true);
  });
  it("parts modification preserves all original numeric lexemes", async () => {
    const f = fixture(); const stream = await f.bind();
    stream.addPart({ type: "text", text: "new" }); await stream.flush();
    expect(f.calls[1].wire).toContain('"large":9007199254740993');
    expect(f.calls[1].wire).toContain('"decimal":1.234567890123456789');
    expect(f.calls[1].wire).toContain('"minus":-0');
    expect(f.calls[1].wire).toContain('"exp":1e+30');
  });
  it("permits same-fence credential renewal", async () => {
    const f = fixture(); const stream = await f.bind();
    f.changeLease({ runtimeLeaseCredential: "credential2", leaseExpiresAt: "2099-02-01T00:00:00Z" });
    stream.appendBody("!"); await stream.flush();
    expect(f.calls[1].auth).toBe("Bearer credential2");
  });
  for (const key of ["instanceId", "handlerIdentity", "runtimeEpoch", "leaseId"] as const) {
    it(`rejects replacement ${key} before effect`, async () => {
      const f = fixture(); const stream = await f.bind(); f.changeLease({ [key]: "replacement" });
      expect(() => stream.appendBody("!")).toThrow("lease fence");
      expect(f.calls).toHaveLength(1);
    });
  }
  it("rejects epoch swap across initial GET await", async () => {
    const f = fixture(); f.afterRequest(() => f.changeLease({ runtimeEpoch: "epoch2" }));
    await expect(f.bind()).rejects.toThrow("lease fence"); expect(f.calls).toHaveLength(1);
  });
  it("rejects journal attempt change across PATCH await", async () => {
    const f = fixture(); const stream = await f.bind();
    f.afterRequest(() => f.changeAttempt()); stream.appendBody("!");
    await expect(stream.flush()).rejects.toThrow("journal attempt");
    await expect(stream.finalize()).rejects.toThrow("journal attempt");
    expect(f.calls).toHaveLength(2);
  });
  it("lost terminal response proves only the same durable row, never POST", async () => {
    const f = fixture(); const stream = await f.bind(); f.dropTerminal(true);
    expect((await stream.cancel({ stopReason: "user_stop" })).state).toBe("cancelled");
    expect(f.calls.map(c => c.method)).toEqual(["GET", "PATCH", "GET"]);
  });
  it("lost terminal response without persistence is unknown and never reexecutes", async () => {
    const f = fixture(); const stream = await f.bind(); f.dropTerminal(false);
    await expect(stream.finalize({ stopReason: "end_turn" })).rejects.toThrow("response lost");
    expect(f.calls.map(c => c.method)).toEqual(["GET", "PATCH", "GET"]);
  });
  it("wrong task request binding prevents any mutation", async () => {
    const f = fixture(); f.wrongRequest(); await expect(f.bind()).rejects.toThrow("binding mismatch");
    expect(f.calls).toHaveLength(1);
  });
  it("terminal row with lost original or appended parts is not success", async () => {
    const f = fixture(); const stream = await f.bind();
    stream.addPart({ type: "text", text: "answer" }); await stream.flush();
    f.dropTerminalParts();
    await expect(stream.finalize({ stopReason: "end_turn" })).rejects.toThrow("outcome unknown");
    expect(f.calls.map(c => c.method)).toEqual(["GET", "PATCH", "PATCH", "GET"]);
  });
  it("capability absence is explicit, with no history fallback", async () => {
    const f = fixture(); const { getMessage: _unused, ...legacy } = f.http;
    await expect(bindExistingRuntimeChatReply({ authority: { currentLease: () => f.lease }, http: legacy,
      binding: { conversationId: "conversation", messageId: "reply", taskRequestMessageId: "task-request",
        assertAttempt() {}, mutationKey: n => `${n}` } })).rejects.toThrow("single snapshot capability");
    expect(f.calls).toHaveLength(0);
  });
});
