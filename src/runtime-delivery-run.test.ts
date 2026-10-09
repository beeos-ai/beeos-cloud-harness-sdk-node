import { beforeAll, afterAll, afterEach, describe, expect, it, vi } from "vitest";
import { NodeRuntimeDeliveryPort } from "./runtime-delivery.js";
// Pure transport fixture: no Go corpus, environment, signature or admission authority is claimed.
const clock = 1790800174687;
const request = { operationId: "00000000-0000-4000-8000-000000000001",
  requestMessageId: "synthetic-operation-request", command: { context: { historyGeneration: "5" } } };
const p = { runtimeRunId: request.operationId };
const lease = { instanceId: "synthetic-instance", handlerIdentity: "synthetic-handler",
  runtimeEpoch: "7", leaseId: "synthetic-lease", runtimeLeaseCredential: "local-port-only",
  leaseExpiresAt: new Date(clock + 120000).toISOString(), journalStoreId: "synthetic-journal", journalGeneration: "2" };
beforeAll(() => vi.spyOn(Date, "now").mockReturnValue(clock));
afterAll(() => vi.restoreAllMocks());
afterEach(() => vi.unstubAllGlobals());
function runInput() { return { operationId: request.operationId, requestMessageId: request.requestMessageId,
  conversationId: "conv/汉字", historyGeneration: request.command.context.historyGeneration, runtimeRunId: p.runtimeRunId,
  lease: { leaseId: lease.leaseId, handlerIdentity: lease.handlerIdentity, runtimeEpoch: lease.runtimeEpoch,
    runtimeLeaseCredential: lease.runtimeLeaseCredential } }; }
describe("concrete runtime run transport", () => {
  it("sends only the configured two POST routes and returns Response to the mapper", async () => {
    const f = vi.fn(async () => Response.json({ status: "registered", activeRunRegistrationId: "reg", historyGeneration: request.command.context.historyGeneration }));
    vi.stubGlobal("fetch", f);
    const port = new NodeRuntimeDeliveryPort({ serviceOrigin: async () => "https://message.test" }, { currentLease: () => lease });
    const start = await port.startConversationRun(runInput());
    await expect(start.json()).resolves.toMatchObject({ status: "registered" });
    f.mockImplementationOnce(async () => Response.json({ status: "idempotent", activeRunRegistrationId: "reg" }));
    const finish = await port.finishConversationRun({ ...runInput(), activeRunRegistrationId: "reg", terminalState: "cancelled", source: "recovery" });
    await expect(finish.json()).resolves.toMatchObject({ status: "idempotent" });
    expect(f.mock.calls.map(call => String(call[0]))).toEqual([
      "https://message.test/api/v1/runtime/conversations/conv%2F%E6%B1%89%E5%AD%97/runs/start",
      "https://message.test/api/v1/runtime/conversations/conv%2F%E6%B1%89%E5%AD%97/runs/finish" ]);
    const init = f.mock.calls[0][1] as RequestInit;
    expect(init.redirect).toBe("error"); expect(new Headers(init.headers).get("authorization")).toBe("Bearer local-port-only");
    expect(JSON.parse(String(init.body))).toEqual(runInput()); expect(init.signal).toBeTruthy();
  });
  it("rejects mismatched input lease before fetch", async () => {
    const f = vi.fn(); vi.stubGlobal("fetch", f);
    const port = new NodeRuntimeDeliveryPort({ serviceOrigin: async () => "https://message.test" }, { currentLease: () => lease });
    await expect((port as any).startConversationRun({ ...runInput(), lease: { ...runInput().lease, runtimeEpoch: "100" } })).rejects.toThrow();
    expect(f).not.toHaveBeenCalled();
  });
  it.each(["instanceId", "handlerIdentity", "runtimeEpoch", "leaseId", "journalStoreId", "journalGeneration"])("rejects %s change while resolving origin before fetch", async field => {
    let current = { ...lease }; const f = vi.fn(); vi.stubGlobal("fetch", f);
    const port = new NodeRuntimeDeliveryPort({ serviceOrigin: async () => { (current as any)[field] = "changed"; return "https://message.test"; } }, { currentLease: () => current });
    await expect((port as any).startConversationRun(runInput())).rejects.toThrow(); expect(f).not.toHaveBeenCalled();
  });
  it.each(["instanceId", "handlerIdentity", "runtimeEpoch", "leaseId", "journalStoreId", "journalGeneration"])("rejects %s change across fetch await", async field => {
    let current = { ...lease }; const f = vi.fn(async () => { current = { ...current, [field]: "changed" }; return Response.json({}); }); vi.stubGlobal("fetch", f);
    const port = new NodeRuntimeDeliveryPort({ serviceOrigin: async () => "https://message.test" }, { currentLease: () => current });
    await expect((port as any).finishConversationRun({ ...runInput(), activeRunRegistrationId: "reg", terminalState: "completed", source: "runtime" })).rejects.toThrow(); expect(f).toHaveBeenCalledTimes(1);
  });
  it("permits same-identity credential renewal without borrowing another attempt", async () => {
    let current = { ...lease }; const f = vi.fn(async (_url: any, init: RequestInit) => {
      expect(new Headers(init.headers).get("authorization")).toBe("Bearer renewed");
      expect(JSON.parse(String(init.body)).lease.runtimeLeaseCredential).toBe("renewed");
      return Response.json({}); }); vi.stubGlobal("fetch", f);
    const port = new NodeRuntimeDeliveryPort({ serviceOrigin: async () => { current = { ...current, runtimeLeaseCredential: "renewed", leaseExpiresAt: new Date(clock + 180000).toISOString() }; return "https://message.test"; } }, { currentLease: () => current });
    await expect((port as any).startConversationRun(runInput())).resolves.toBeInstanceOf(Response);
  });
  it("rejects a response that escaped the configured origin", async () => {
    const response = Response.json({}); Object.defineProperty(response, "url", { value: "https://other.test/result" });
    vi.stubGlobal("fetch", async () => response);
    const port = new NodeRuntimeDeliveryPort({ serviceOrigin: async () => "https://message.test" }, { currentLease: () => lease });
    await expect((port as any).startConversationRun(runInput())).rejects.toThrow("origin");
  });
});
