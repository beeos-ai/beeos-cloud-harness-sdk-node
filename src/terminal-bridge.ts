/**
 * Terminal bridge client. The first text frame is `agent_auth`; the signing
 * string matches cluster-proxy `AuthenticateAgent` for service `terminal`.
 *
 * Python mirror: `beeos_cloud_harness_sdk.terminal`.
 */
import { randomUUID } from "node:crypto";
import WebSocket from "ws";
import { signAgentMessage, type AgentKeyPair } from "./agent-identity.js";
import type { TerminalAgentAuthFrame } from "./generated/types.gen.js";

export interface TerminalAuthLease {
  instanceId: string;
  leaseId: string;
  runtimeEpoch: string;
  runtimeLeaseCredential: string;
  handlerIdentity: string;
}

export function terminalAgentWebSocketUrl(bridgeUrl: string): string {
  const base = bridgeUrl.replace(/\/+$/, "");
  const url = new URL(`${base}/agent`);
  if (url.protocol !== "ws:" && url.protocol !== "wss:") {
    throw new Error("terminal bridge URL must be ws or wss");
  }
  return url.toString();
}

/** UTF-8 preimage. `instance_id` is not included. */
export function terminalAgentAuthSigningMessage(input: {
  publicKey: string;
  timestamp: number;
  nonce: string;
  runtimeLeaseCredential: string;
  handlerIdentity: string;
  runtimeEpoch: string;
  leaseId: string;
}): string {
  return `terminal|${input.publicKey}|${input.timestamp}|${input.nonce}|${input.runtimeLeaseCredential}|` +
    `${input.handlerIdentity}|${input.runtimeEpoch}|${input.leaseId}`;
}

export function createTerminalAgentAuth(input: {
  lease: TerminalAuthLease;
  keys: AgentKeyPair;
  timestamp?: number;
  nonce?: string;
}): TerminalAgentAuthFrame {
  const lease = input.lease;
  if (!lease.instanceId || !lease.leaseId || !lease.handlerIdentity || !lease.runtimeLeaseCredential ||
      !/^[1-9][0-9]*$/.test(lease.runtimeEpoch) || input.keys.publicKey.length !== 32) {
    throw new Error("invalid Terminal runtime lease authority binding");
  }
  const timestamp = input.timestamp ?? Math.floor(Date.now() / 1000);
  const nonce = input.nonce ?? randomUUID();
  const publicKey = Buffer.from(input.keys.publicKey).toString("base64");
  const preimage = terminalAgentAuthSigningMessage({
    publicKey,
    timestamp,
    nonce,
    runtimeLeaseCredential: lease.runtimeLeaseCredential,
    handlerIdentity: lease.handlerIdentity,
    runtimeEpoch: lease.runtimeEpoch,
    leaseId: lease.leaseId,
  });
  return {
    type: "agent_auth",
    instance_id: lease.instanceId,
    service: "terminal",
    public_key: publicKey,
    timestamp,
    nonce,
    signature: Buffer.from(signAgentMessage(preimage, input.keys.privateKey)).toString("base64"),
    runtimeLeaseCredential: lease.runtimeLeaseCredential,
    handlerIdentity: lease.handlerIdentity,
    runtimeEpoch: lease.runtimeEpoch,
    leaseId: lease.leaseId,
  };
}

export interface TerminalSocket {
  send(data: string): void;
  close(): void;
  on(event: "message", listener: (data: Buffer | string) => void): void;
  on(event: "error", listener: (error: Error) => void): void;
  once(event: "open", listener: () => void): void;
}

export interface TerminalBridgeClientOptions {
  bridgeUrl: string;
  keys: AgentKeyPair;
  lease: TerminalAuthLease;
  now?: () => number;
  nonce?: () => string;
  authTimeoutMs?: number;
  socketFactory?: (url: string) => TerminalSocket;
}

/** Opens `GET /agent` and sends the lease-bound `agent_auth` frame. Later frames are raw terminal bytes. */
export class TerminalBridgeClient {
  private socket?: TerminalSocket;
  constructor(private readonly options: TerminalBridgeClientOptions) {}

  async connect(): Promise<TerminalAgentAuthFrame> {
    const frame = createTerminalAgentAuth({
      lease: this.options.lease,
      keys: this.options.keys,
      timestamp: this.options.now ? Math.floor(this.options.now() / 1000) : undefined,
      nonce: this.options.nonce?.(),
    });
    const url = terminalAgentWebSocketUrl(this.options.bridgeUrl);
    const socket = (this.options.socketFactory ?? ((target) => new WebSocket(target) as unknown as TerminalSocket))(url);
    this.socket = socket;
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("Terminal auth timeout")), this.options.authTimeoutMs ?? 10_000);
      const fail = (error: Error) => {
        clearTimeout(timer);
        reject(error);
      };
      socket.on("error", fail);
      socket.on("message", (data) => {
        const text = typeof data === "string" ? data : data.toString("utf8");
        let parsed: { type?: string };
        try {
          parsed = JSON.parse(text) as { type?: string };
        } catch {
          fail(new Error("Terminal auth response was not JSON"));
          return;
        }
        if (parsed.type !== "auth_ok") {
          fail(new Error(`Terminal auth rejected (${parsed.type ?? "unknown"})`));
          return;
        }
        clearTimeout(timer);
        resolve();
      });
      socket.once("open", () => socket.send(JSON.stringify(frame)));
    });
    return frame;
  }

  send(data: string): void {
    if (!this.socket) throw new Error("Terminal bridge is not connected");
    this.socket.send(data);
  }

  close(): void {
    this.socket?.close();
    this.socket = undefined;
  }
}
