/**
 * Canvas relay WebSocket. The agent dials `{relay}/ws/agent` and joins canvases
 * with text frames. Binary frames are `[canvasIdLen:uint8][canvasId][payload]`.
 *
 * Python mirror: `beeos_cloud_harness_sdk.canvas_relay`.
 */
import WebSocket from "ws";
import { agentAuthHeaders, type AgentKeyPair } from "./agent-identity.js";

export const CANVAS_RELAY_PATH = "/ws/agent";

/** `{relay}/ws/agent`. An existing path prefix is kept. Never `/ws/{canvasId}`. */
export function canvasRelayWebSocketUrl(relayUrl: string, options: { agentId?: string; token?: string } = {}): string {
  let base = relayUrl.trim().replace(/\/+$/, "");
  if (!base) throw new Error("empty canvas relay url");
  if (base.startsWith("https://")) base = `wss://${base.slice("https://".length)}`;
  else if (base.startsWith("http://")) base = `ws://${base.slice("http://".length)}`;
  else if (!base.startsWith("ws://") && !base.startsWith("wss://")) base = `wss://${base}`;
  const url = new URL(base.endsWith(CANVAS_RELAY_PATH) ? base : `${base}${CANVAS_RELAY_PATH}`);
  if (options.agentId) url.searchParams.set("agentId", options.agentId);
  if (options.token) url.searchParams.set("token", options.token);
  return url.toString();
}

export function canvasRelayUpgradeHeaders(keys: AgentKeyPair): Record<string, string> {
  return agentAuthHeaders("GET", CANVAS_RELAY_PATH, keys);
}

export function canvasRelayJoinFrame(canvasId: string): string {
  return JSON.stringify({ type: "join", canvasId });
}

export function canvasRelayLeaveFrame(canvasId: string): string {
  return JSON.stringify({ type: "leave", canvasId });
}

export function encodeCanvasRelayBinary(canvasId: string, payload: Uint8Array): Uint8Array {
  const id = Buffer.from(canvasId, "utf8");
  if (id.length === 0 || id.length > 255) throw new Error("canvas id length must be 1..255 bytes");
  const out = new Uint8Array(1 + id.length + payload.length);
  out[0] = id.length;
  out.set(id, 1);
  out.set(payload, 1 + id.length);
  return out;
}

export interface CanvasRelaySocket {
  send(data: string | Uint8Array): void;
  close(): void;
  on(event: "open", listener: () => void): void;
  on(event: "message", listener: (data: Buffer | ArrayBuffer | string) => void): void;
  on(event: "error", listener: (error: Error) => void): void;
}

export interface CanvasRelayClientOptions {
  relayUrl: string;
  keys: AgentKeyPair;
  agentId?: string;
  token?: string;
  socketFactory?: (url: string, headers: Record<string, string>) => CanvasRelaySocket;
}

/** Owns the relay upgrade, auth headers, and join/leave frames. Yjs payload bytes stay with the harness. */
export class CanvasRelayClient {
  private socket?: CanvasRelaySocket;
  constructor(private readonly options: CanvasRelayClientOptions) {}

  connect(onMessage?: (data: Uint8Array | string) => void): Promise<void> {
    const headers = canvasRelayUpgradeHeaders(this.options.keys);
    const url = canvasRelayWebSocketUrl(this.options.relayUrl, {
      agentId: this.options.agentId,
      token: this.options.token,
    });
    const socket = (this.options.socketFactory ?? ((target, upgrade) =>
      new WebSocket(target, { headers: upgrade }) as unknown as CanvasRelaySocket))(url, headers);
    this.socket = socket;
    if (onMessage) {
      socket.on("message", (data) => {
        if (typeof data === "string") onMessage(data);
        else if (data instanceof ArrayBuffer) onMessage(new Uint8Array(data));
        else onMessage(new Uint8Array(data));
      });
    }
    return new Promise((resolve, reject) => {
      socket.on("error", reject);
      socket.on("open", () => resolve());
    });
  }

  join(canvasId: string): void {
    this.sendText(canvasRelayJoinFrame(canvasId));
  }

  leave(canvasId: string): void {
    this.sendText(canvasRelayLeaveFrame(canvasId));
  }

  sendBinary(canvasId: string, payload: Uint8Array): void {
    this.socketSend(encodeCanvasRelayBinary(canvasId, payload));
  }

  close(): void {
    this.socket?.close();
    this.socket = undefined;
  }

  private sendText(frame: string): void {
    this.socketSend(frame);
  }

  private socketSend(data: string | Uint8Array): void {
    if (!this.socket) throw new Error("canvas relay is not connected");
    this.socket.send(data);
  }
}
