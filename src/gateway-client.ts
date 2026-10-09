import { operationPath } from "./generated/operations.gen.js";
import { agentRequestAuthHeaders, authenticatedAgentFetch, type AgentRequestIdentity } from "./agent-request-signing.js";
import {
  CloudHttpError, TRANSIENT_BACKOFF, isTransientError, retryWithBackoff, type BackoffPolicy,
} from "./retry.js";

/**
 * Agent Gateway client: the only place a harness (Claw, Hermes, custom agents)
 * learns Gateway routes, signs requests, or decides what is worth retrying.
 *
 * Python mirror: `beeos_cloud_harness_sdk.gateway.AgentGatewayClient`.
 */
export interface AgentGatewayClientOptions {
  /** Agent Gateway origin, e.g. `https://agent-gateway.beeos.example`. */
  baseUrl: string;
  identity: AgentRequestIdentity;
  /** Default retry for idempotent calls; `false` disables. Defaults to {@link TRANSIENT_BACKOFF}. */
  retry?: BackoffPolicy | false;
  onRetry?: (info: { method: string; path: string; attempt: number; delayMs: number; error: unknown }) => void;
}

export interface GatewayRequestInit extends Omit<RequestInit, "body"> {
  body?: string | Uint8Array;
  /** Override the client default; `false` disables retries for this call. */
  retry?: BackoffPolicy | false;
  /** Allow retries of a non-GET call that the Gateway deduplicates. */
  idempotent?: boolean;
}

export interface GatewayFileRef { fileId: string; fileName: string; mimeType?: string; size?: number; source?: string }
export interface GatewayPresign { fileId: string; uploadUrl: string }
export interface GatewayFileResolve {
  fileId?: string; fileName?: string; mimeType?: string; size?: number; downloadUrl?: string;
  [key: string]: unknown;
}
export interface GatewayUploadedFile { fileId: string; uri: string; fileName: string; mimeType: string }
export interface GatewayCanvasToken { token: string; relayUrl: string; expiresAt: number }
export interface GatewayBridgeConfig { url: string; region?: string }
export interface GatewayA2aRpcResult {
  ok: boolean; status: number; body: string; rpc: Record<string, unknown>;
}

const JSON_HEADERS = { "content-type": "application/json" } as const;

export class AgentGatewayClient {
  readonly baseUrl: URL;
  readonly files: GatewayFiles;
  readonly canvas: GatewayCanvas;
  readonly agents: GatewayAgents;
  readonly channels: GatewayChannels;
  readonly a2a: GatewayA2a;
  readonly events: GatewayEvents;
  readonly bridge: GatewayBridge;
  readonly connectors: GatewayConnectors;
  readonly automations: GatewayAutomations;
  readonly runtime: GatewayRuntime;

  constructor(private readonly options: AgentGatewayClientOptions) {
    this.baseUrl = new URL(options.baseUrl);
    if (!["http:", "https:"].includes(this.baseUrl.protocol) || this.baseUrl.username || this.baseUrl.password) {
      throw new Error("Agent Gateway base URL must be http(s) without credentials");
    }
    this.files = new GatewayFiles(this);
    this.canvas = new GatewayCanvas(this);
    this.agents = new GatewayAgents(this);
    this.channels = new GatewayChannels(this);
    this.a2a = new GatewayA2a(this);
    this.events = new GatewayEvents(this);
    this.bridge = new GatewayBridge(this);
    this.connectors = new GatewayConnectors(this);
    this.automations = new GatewayAutomations(this);
    this.runtime = new GatewayRuntime(this);
  }

  /** Signed request. Network errors and 502/503/504 are retried for GET (or `idempotent`) per policy. */
  async fetch(target: string | URL, init: GatewayRequestInit = {}): Promise<Response> {
    const url = typeof target === "string" && target.startsWith("/")
      ? new URL(target, this.baseUrl) : new URL(target);
    if (url.origin !== this.baseUrl.origin) throw new Error("Agent Gateway credentials cannot be sent to another origin");
    const { retry, idempotent, ...rest } = init;
    const method = (rest.method ?? "GET").toUpperCase();
    const policy = retry === undefined ? this.options.retry ?? TRANSIENT_BACKOFF : retry;
    const mayRetry = policy !== false && (method === "GET" || method === "HEAD" || idempotent === true || retry !== undefined);
    const attempt = async (): Promise<Response> => {
      const response = await authenticatedAgentFetch(url, this.options.identity, { ...rest, method } as RequestInit);
      if (mayRetry && TRANSIENT_STATUSES.has(response.status)) throw new TransientAnswer(response);
      return response;
    };
    if (!mayRetry) return await attempt().catch(settleTransient);
    try {
      return await retryWithBackoff(attempt, {
        policy: policy as BackoffPolicy, signal: rest.signal ?? undefined,
        isRetryable: (error) => error instanceof TransientAnswer || isTransientError(error),
        onRetry: ({ attempt: n, delayMs, error }) => {
          if (error instanceof TransientAnswer) void error.response.body?.cancel().catch(() => undefined);
          this.options.onRetry?.({ method, path: url.pathname, attempt: n, delayMs, error });
        },
      });
    } catch (error) { return settleTransient(error); }
  }

  /** Signed JSON call; throws {@link CloudHttpError} for any non-2xx answer. */
  async json<T>(method: string, path: string, body?: unknown, init: GatewayRequestInit = {}): Promise<T> {
    const headers = new Headers(init.headers);
    if (body !== undefined) headers.set("content-type", "application/json");
    const response = await this.fetch(path, {
      ...init, method, headers,
      ...(body === undefined ? {} : { body: typeof body === "string" ? body : JSON.stringify(body) }),
    });
    const text = await response.text();
    if (!response.ok) throw new CloudHttpError(method, path.split("?")[0]!, response.status, text);
    return (text ? JSON.parse(text) : undefined) as T;
  }

  /** Gateway v2 headers for a socket the caller opens itself, such as the canvas relay upgrade. */
  upgradeHeaders(method: string, path: string, body?: string | Uint8Array | null): Record<string, string> {
    return agentRequestAuthHeaders(method, path, this.options.identity, body);
  }

  /** A `(url, init)` fetch bound to this client, for ports that only need a signed transport. */
  asFetch(): (url: string | URL, init?: RequestInit) => Promise<Response> {
    return (url, init) => this.fetch(url, init as GatewayRequestInit | undefined);
  }
}

const TRANSIENT_STATUSES: ReadonlySet<number> = new Set([408, 425, 429, 500, 502, 503, 504]);

/** Carries the last transient answer out of the retry loop so exhaustion still returns a real Response. */
class TransientAnswer extends Error {
  constructor(readonly response: Response) { super(`transient HTTP ${response.status}`); }
}

function settleTransient(error: unknown): Response {
  if (error instanceof TransientAnswer) return error.response;
  throw error;
}

class GatewayRuntime {
  constructor(private readonly gw: AgentGatewayClient) {}
  /** Registration routes; satisfy the SDK registration transport. Never replayed: the coordinator owns retry. */
  async register<T = unknown>(body: unknown, init?: GatewayRequestInit): Promise<T> {
    return await this.gw.json("POST", operationPath("runtimeRegister"), body, { retry: false, ...init });
  }
  async heartbeat<T = unknown>(registrationId: string, body: unknown, init?: GatewayRequestInit): Promise<T> {
    return await this.gw.json("POST", operationPath("runtimeHeartbeat", { registrationId }),
      body, { retry: false, ...init });
  }
  /** Redeem an operation resource. Returns the raw Response so the host can verify digests and map failures. */
  async redeemResource(operationId: string, kind: "skill" | "mcp", resourceRef: string, body: unknown,
    init?: GatewayRequestInit): Promise<Response> {
    return await this.gw.fetch(
      operationPath("runtimeResourceRedeem", { operationId, kind, resourceRef }),
      { retry: false, ...init, method: "POST", headers: JSON_HEADERS, body: JSON.stringify(body) });
  }
}

class GatewayFiles {
  constructor(private readonly gw: AgentGatewayClient) {}
  async list(init?: GatewayRequestInit): Promise<{ files?: GatewayFileRef[] }> {
    return await this.gw.json("GET", operationPath("agentFilesList"), undefined, init);
  }
  async presign(input: { fileName: string; mimeType: string }, init?: GatewayRequestInit): Promise<GatewayPresign> {
    return await this.gw.json("POST", operationPath("agentFilePresign"), input, init);
  }
  async confirm(input: { fileId: string }, init?: GatewayRequestInit): Promise<GatewayFileRef> {
    return await this.gw.json("POST", operationPath("agentFileConfirm"), input, init);
  }
  async resolve(fileId: string, init?: GatewayRequestInit): Promise<GatewayFileResolve> {
    return await this.gw.json("GET", operationPath("agentFileResolve", { fileId }), undefined, init);
  }
  /** presign → PUT → confirm under one caller-owned deadline. */
  async upload(input: { fileName: string; mimeType: string; data: Uint8Array; signal?: AbortSignal },
    fetchImpl: typeof fetch = fetch): Promise<GatewayUploadedFile> {
    const { signal } = input;
    const presign = await this.presign({ fileName: input.fileName, mimeType: input.mimeType }, { signal });
    const put = await fetchImpl(presign.uploadUrl, {
      method: "PUT", headers: { "Content-Type": input.mimeType },
      body: new Blob([input.data as BlobPart], { type: input.mimeType }), signal,
    });
    if (!put.ok) throw new CloudHttpError("PUT", "object-storage", put.status, "");
    const confirmed = await this.confirm({ fileId: presign.fileId }, { signal });
    return {
      fileId: confirmed.fileId, uri: `beeos-file://${confirmed.fileId}`,
      fileName: confirmed.fileName || input.fileName, mimeType: confirmed.mimeType || input.mimeType,
    };
  }
  /** Download a presigned URL returned by {@link resolve}. No Gateway signature is attached. */
  async download(downloadUrl: string, signal?: AbortSignal, fetchImpl: typeof fetch = fetch): Promise<Uint8Array> {
    const response = await fetchImpl(downloadUrl, { method: "GET", signal });
    if (!response.ok) throw new CloudHttpError("GET", "object-storage", response.status, "");
    return new Uint8Array(await response.arrayBuffer());
  }
}

class GatewayCanvas {
  constructor(private readonly gw: AgentGatewayClient) {}
  /** The connection token is not bound to one agent; association travels on each canvas message. */
  async token(init?: GatewayRequestInit): Promise<GatewayCanvasToken> {
    const raw = await this.gw.json<Record<string, unknown>>("POST", operationPath("canvasTokenCreate"), {},
      { retry: TRANSIENT_BACKOFF, ...init });
    const body = (raw?.data && typeof raw.data === "object" ? raw.data : raw) as Record<string, unknown>;
    const token = typeof body?.token === "string" ? body.token : "";
    if (!token) throw new CloudHttpError("POST", operationPath("canvasTokenCreate"), 200, "canvas token response missing token");
    return {
      token,
      relayUrl: String(body.relay_url ?? body.relayUrl ?? ""),
      expiresAt: Number(body.expires_at ?? body.expiresAt ?? 0),
    };
  }

  /** Signed headers for `GET /ws/agent` on the relay origin from {@link token}. */
  relayHeaders(path = "/ws/agent"): Record<string, string> {
    return this.gw.upgradeHeaders("GET", path);
  }
}

class GatewayAgents {
  constructor(private readonly gw: AgentGatewayClient) {}
  /** Types4 lease-fenced projection; the caller supplies the exact JSON body that is signed. */
  async sync<T = Record<string, unknown>>(body: string | object, init?: GatewayRequestInit): Promise<T> {
    return await this.gw.json("POST", operationPath("agentSync"), body, init);
  }
  /** Active template-origin report after `agent.applyTemplate` is durably projected. */
  async reportTemplateOrigin(platformAgentId: string, templateId: string, init?: GatewayRequestInit): Promise<void> {
    await this.gw.json("POST", operationPath("agentTemplateOriginReport", { agentId: platformAgentId }),
      { templateId }, init);
  }
}

class GatewayChannels {
  constructor(private readonly gw: AgentGatewayClient) {}
  async create(input: { participants: string[]; metadata: Record<string, string> },
    init?: GatewayRequestInit): Promise<{ id?: string }> {
    return await this.gw.json("POST", operationPath("channelCreate"), input, init);
  }
}

class GatewayA2a {
  constructor(private readonly gw: AgentGatewayClient) {}
  async discover(input: { query?: string; limit?: number }, init?: GatewayRequestInit): Promise<Record<string, unknown>> {
    const qs = new URLSearchParams({ limit: String(input.limit ?? 50) });
    if (input.query) qs.set("query", input.query);
    return await this.gw.json("GET", `${operationPath("a2aDiscover")}?${qs}`, undefined, init);
  }
  /** JSON-RPC to a peer agent. Never throws on HTTP/RPC errors: the caller maps status (401/403 → auth required). */
  async rpc(agentId: string, method: string, params: Record<string, unknown>,
    init?: GatewayRequestInit & { requestId?: string }): Promise<GatewayA2aRpcResult> {
    const { requestId, ...rest } = init ?? {};
    const response = await this.gw.fetch(operationPath("a2aJsonRpc", { agentId }), {
      ...rest, method: "POST", headers: JSON_HEADERS,
      body: JSON.stringify({ jsonrpc: "2.0", id: requestId ?? `harness-${Date.now()}`, method, params }),
    });
    const body = await response.text();
    let rpc: Record<string, unknown> = {};
    try { rpc = JSON.parse(body) as Record<string, unknown>; } catch { /* non-JSON error body */ }
    return { ok: response.ok && !rpc.error, status: response.status, body, rpc };
  }
  async completeTask(taskId: string, input: { result: string; error: string }, init?: GatewayRequestInit): Promise<void> {
    await this.gw.json("POST", operationPath("a2aTaskComplete", { taskId }), input, init);
  }
}

class GatewayEvents {
  constructor(private readonly gw: AgentGatewayClient) {}
  async publish(event: { type: string; data: unknown }, init?: GatewayRequestInit): Promise<void> {
    await this.gw.json("POST", operationPath("agentEventPublish"), event, init);
  }
}

class GatewayBridge {
  constructor(private readonly gw: AgentGatewayClient) {}
  /** Bridge discovery; `undefined` when the Gateway is unreachable so the caller can fall back to its cache. */
  async config(region?: string, init?: GatewayRequestInit): Promise<GatewayBridgeConfig | undefined> {
    try {
      const data = await this.gw.json<{ url?: string; region?: string }>("GET",
        `${operationPath("bridgeConfigGet")}${region ? `?region=${encodeURIComponent(region)}` : ""}`, undefined, init);
      return data?.url ? { url: data.url, region: data.region } : undefined;
    } catch { return undefined; }
  }
}

class GatewayConnectors {
  constructor(private readonly gw: AgentGatewayClient) {}
  /** Signed pass-through to the connector MCP endpoint; streaming bodies are returned unread. */
  async mcp(init: GatewayRequestInit): Promise<Response> {
    return await this.gw.fetch(operationPath("agentConnectorMcp"), { retry: false, ...init });
  }
  /** One JSON-RPC call (e.g. `tools/call`) answered as JSON; throws {@link CloudHttpError} on non-2xx. */
  async callJson<T = unknown>(rpc: Record<string, unknown>, init?: GatewayRequestInit): Promise<T> {
    return await this.gw.json("POST", operationPath("agentConnectorMcp"), rpc,
      { retry: false, ...init, headers: { accept: "application/json", ...(init?.headers as Record<string, string> | undefined) } });
  }
}

/** Automations CRUD/run routes. Responses are returned as the Gateway sent them; unwrap `data` in the host. */
class GatewayAutomations {
  constructor(private readonly gw: AgentGatewayClient) {}
  private static query(values?: Record<string, string | number | undefined>): string {
    const qs = new URLSearchParams();
    for (const [key, value] of Object.entries(values ?? {})) if (value !== undefined && value !== "") qs.set(key, String(value));
    return qs.size ? `?${qs}` : "";
  }
  async list<T = unknown>(query?: Record<string, string | number | undefined>, init?: GatewayRequestInit): Promise<T> {
    return await this.gw.json("GET", `${operationPath("automationList")}${GatewayAutomations.query(query)}`, undefined, init);
  }
  async create<T = unknown>(body: Record<string, unknown>, init?: GatewayRequestInit): Promise<T> {
    return await this.gw.json("POST", operationPath("automationCreate"), body, init);
  }
  async createWebhook<T = unknown>(body: Record<string, unknown>, init?: GatewayRequestInit): Promise<T> {
    return await this.gw.json("POST", operationPath("automationWebhookCreate"), body, init);
  }
  async get<T = unknown>(automationId: string, init?: GatewayRequestInit): Promise<T> {
    return await this.gw.json("GET", operationPath("automationGet", { automationId }), undefined, init);
  }
  async run<T = unknown>(automationId: string, body: Record<string, unknown>, idempotencyKey: string,
    init?: GatewayRequestInit): Promise<T> {
    return await this.gw.json("POST", operationPath("automationRunCreate", { automationId }), body,
      { ...init, headers: { "idempotency-key": idempotencyKey } });
  }
  async runs<T = unknown>(automationId: string, query?: Record<string, string | number | undefined>,
    init?: GatewayRequestInit): Promise<T> {
    return await this.gw.json("GET",
      `${operationPath("automationRunsList", { automationId })}${GatewayAutomations.query(query)}`, undefined, init);
  }
  async runDetail<T = unknown>(automationId: string, runId: string, init?: GatewayRequestInit): Promise<T> {
    return await this.gw.json("GET",
      operationPath("automationRunGet", { automationId, runId }), undefined, init);
  }
}

export type { GatewayFiles, GatewayCanvas, GatewayAgents, GatewayChannels, GatewayA2a, GatewayEvents, GatewayBridge, GatewayConnectors, GatewayAutomations, GatewayRuntime };
