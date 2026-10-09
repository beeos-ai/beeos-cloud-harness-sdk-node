/**
 * Mint a Message Service Centrifugo connection token and subscribe with the
 * server-owned channel list. The client must not call `newSubscription`.
 *
 * Python mirror: `beeos_cloud_harness_sdk.realtime.mint_runtime_realtime_token`.
 */
import { operationPath } from "./generated/operations.gen.js";
import { createCentrifugoRealtimePort, type CentrifugeRealtimeFactory } from "./runtime-chat-centrifuge.js";
import { RuntimeLeaseHttp, type RuntimeLeaseCredential } from "./runtime-lease-http.js";
import { HarnessProtocolError } from "./runtime-message-plane.js";

export interface RuntimeRealtimeToken {
  token: string;
  centrifugoUrl: string;
  channels: readonly string[];
  principalId: string;
  expiresAt: number;
}

export async function mintRuntimeRealtimeToken(http: RuntimeLeaseHttp, credential: RuntimeLeaseCredential,
  signal?: AbortSignal): Promise<RuntimeRealtimeToken> {
  const response = await http.postJson(operationPath("runtimeRealtimeTokenCreate"), credential, {}, { retry: true }, signal);
  const text = await response.text();
  if (!response.ok) throw new HarnessProtocolError(`realtime token failed (${response.status})`, response.status, text);
  const body = JSON.parse(text) as Record<string, unknown>;
  const token = typeof body.token === "string" ? body.token : "";
  const centrifugoUrl = typeof body.centrifugo_url === "string" ? body.centrifugo_url : "";
  const channels = Array.isArray(body.channels) ? body.channels.filter((item): item is string => typeof item === "string") : [];
  if (!token || !centrifugoUrl || channels.length === 0) {
    throw new HarnessProtocolError("realtime token response is incomplete");
  }
  return {
    token,
    centrifugoUrl,
    channels,
    principalId: typeof body.principal_id === "string" ? body.principal_id : "",
    expiresAt: typeof body.expires_at === "number" ? body.expires_at : 0,
  };
}

export interface RuntimeRealtimeSubscription {
  close(): void;
  token: RuntimeRealtimeToken;
}

/** Lease-authenticated token plus the Centrifugo connection publication stream. */
export async function subscribeRuntimeRealtime(input: {
  origin: string;
  credential: RuntimeLeaseCredential;
  onEvent: (event: { type: string; data?: unknown }) => void;
  onState?: (state: "connecting" | "connected" | "disconnected" | "failed") => void;
  onError?: (error: unknown) => void;
  fetch?: typeof fetch;
  factory?: CentrifugeRealtimeFactory;
  signal?: AbortSignal;
}): Promise<RuntimeRealtimeSubscription> {
  const http = new RuntimeLeaseHttp({ origin: input.origin, fetch: input.fetch });
  const token = await mintRuntimeRealtimeToken(http, input.credential, input.signal);
  const port = createCentrifugoRealtimePort({ factory: input.factory });
  const handle = port.open({
    url: token.centrifugoUrl,
    token: token.token,
    channel: token.channels[0]!,
    onEvent: input.onEvent,
    onState: (state) => input.onState?.(state),
    onError: (error) => input.onError?.(error),
  });
  return { token, close: () => handle.close() };
}
