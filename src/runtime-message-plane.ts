/**
 * Lease-authenticated Message Service and internal file routes.
 * Paths come only from the generated route table.
 *
 * Python mirror: `beeos_cloud_harness_sdk.runtime_plane`.
 */
import {
  CLOUD_GATEWAY_RUNTIME_ROUTES,
  CLOUD_MESSAGE_ROUTES,
  RuntimeLeaseHttp,
  type HistoryBoundaryAction,
  type RuntimeLeaseCredential,
} from "./runtime-lease-http.js";

export class HarnessProtocolError extends Error {
  constructor(message: string, readonly status?: number, readonly body?: string) {
    super(message);
    this.name = "HarnessProtocolError";
  }
}

async function responseValue(response: Response): Promise<unknown> {
  const text = await response.text();
  if (!text) return undefined;
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return text;
  }
}

async function expectJson(response: Response, what: string): Promise<unknown> {
  const body = await responseValue(response);
  if (!response.ok) {
    throw new HarnessProtocolError(
      `${what} failed (${response.status})`,
      response.status,
      typeof body === "string" ? body : JSON.stringify(body),
    );
  }
  return body;
}

function requireDeliveryKey(credential: RuntimeLeaseCredential, route: string): void {
  if (!credential.scopedDeliveryKey) {
    throw new HarnessProtocolError(`${route} requires X-Runtime-Delivery-Key`);
  }
}

/** Read, renew, ack, history, and conversation-authority calls on one Message Service origin. */
export class RuntimeMessagePlane {
  constructor(private readonly http: RuntimeLeaseHttp) {}

  async readDeliveries(credential: RuntimeLeaseCredential, input: { maxCount: number; blockMs: number },
    signal?: AbortSignal): Promise<unknown> {
    const response = await this.http.postJson(CLOUD_MESSAGE_ROUTES.deliveriesRead, credential,
      { schemaVersion: 1, maxCount: input.maxCount, blockMs: input.blockMs }, undefined, signal);
    return await expectJson(response, "runtime delivery read");
  }

  async renewDeliveries(credential: RuntimeLeaseCredential, deliveryIds: readonly string[],
    signal?: AbortSignal): Promise<unknown> {
    const response = await this.http.postJson(CLOUD_MESSAGE_ROUTES.deliveriesRenew, credential,
      { schemaVersion: 1, deliveryIds: [...deliveryIds] }, undefined, signal);
    return await expectJson(response, "runtime delivery renew");
  }

  async ackDeliveries(credential: RuntimeLeaseCredential, deliveryIds: readonly string[],
    signal?: AbortSignal): Promise<unknown> {
    const response = await this.http.postJson(CLOUD_MESSAGE_ROUTES.deliveriesAck, credential,
      { schemaVersion: 1, deliveryIds: [...deliveryIds] }, undefined, signal);
    return await expectJson(response, "runtime delivery ack");
  }

  async operationHistory(credential: RuntimeLeaseCredential, operationId: string,
    signal?: AbortSignal): Promise<unknown> {
    const response = await this.http.fetch(CLOUD_MESSAGE_ROUTES.operationHistory(operationId), credential,
      { method: "GET", signal });
    return await expectJson(response, "operation history");
  }

  async appendOperationMessage(credential: RuntimeLeaseCredential, operationId: string,
    body: { type: string; payload: unknown }, signal?: AbortSignal): Promise<unknown> {
    const response = await this.http.postJson(CLOUD_MESSAGE_ROUTES.operationMessages(operationId), credential,
      { schemaVersion: 1, type: body.type, payload: body.payload }, undefined, signal);
    return await expectJson(response, "operation message append");
  }

  async historyBoundary(credential: RuntimeLeaseCredential, conversationId: string, action: HistoryBoundaryAction,
    body: unknown, signal?: AbortSignal): Promise<unknown> {
    const response = await this.http.postJson(
      CLOUD_MESSAGE_ROUTES.historyBoundary(conversationId, action), credential, body, { retry: true }, signal);
    return await expectJson(response, "history boundary");
  }

  async projectSessionModel(credential: RuntimeLeaseCredential, conversationId: string, body: unknown,
    signal?: AbortSignal): Promise<unknown> {
    requireDeliveryKey(credential, "session model");
    const response = await this.http.postJson(
      CLOUD_MESSAGE_ROUTES.metadataModel(conversationId), credential, body, { retry: true }, signal);
    return await expectJson(response, "session model");
  }

  async deliverCron(credential: RuntimeLeaseCredential, conversationId: string, body: unknown,
    signal?: AbortSignal): Promise<unknown> {
    requireDeliveryKey(credential, "cron delivery");
    const response = await this.http.postJson(
      CLOUD_MESSAGE_ROUTES.cronDelivery(conversationId), credential, body, { retry: true }, signal);
    return await expectJson(response, "cron delivery");
  }
}

/** Internal operation input resolve and output presign/confirm. Origin is the Agent Gateway, not Message Service. */
export class RuntimeOperationFiles {
  constructor(private readonly http: RuntimeLeaseHttp) {}

  async resolveInput(credential: RuntimeLeaseCredential, operationId: string, body: unknown,
    signal?: AbortSignal): Promise<unknown> {
    const response = await this.http.postJson(
      CLOUD_GATEWAY_RUNTIME_ROUTES.inputFilesResolve(operationId), credential, body, { retry: true }, signal);
    return await expectJson(response, "input file resolve");
  }

  async presignOutput(credential: RuntimeLeaseCredential, operationId: string, body: unknown,
    signal?: AbortSignal): Promise<unknown> {
    const response = await this.http.postJson(
      CLOUD_GATEWAY_RUNTIME_ROUTES.outputFiles(operationId, "presign"), credential, body, { retry: true }, signal);
    return await expectJson(response, "output file presign");
  }

  async confirmOutput(credential: RuntimeLeaseCredential, operationId: string, body: unknown,
    signal?: AbortSignal): Promise<unknown> {
    const response = await this.http.postJson(
      CLOUD_GATEWAY_RUNTIME_ROUTES.outputFiles(operationId, "confirm"), credential, body, { retry: true }, signal);
    return await expectJson(response, "output file confirm");
  }
}

/** PUT bytes to a presigned upload URL. The lease is not attached. */
export async function putPresignedUpload(input: {
  url: string;
  allowedOrigin: string;
  body: Uint8Array;
  method?: string;
  requiredHeaders?: Record<string, string>;
  signal?: AbortSignal;
  fetchImpl?: typeof fetch;
}): Promise<Response> {
  const target = new URL(input.url);
  const allowed = new URL(input.allowedOrigin);
  if (target.protocol !== "https:" || target.username || target.password || target.origin !== allowed.origin) {
    throw new HarnessProtocolError("presigned upload origin is not the allowed https origin");
  }
  const headers = new Headers(input.requiredHeaders);
  return await (input.fetchImpl ?? fetch)(target, {
    method: input.method ?? "PUT",
    headers,
    body: input.body as BufferSource,
    redirect: "error",
    signal: input.signal,
  });
}
