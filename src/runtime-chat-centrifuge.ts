/**
 * Real Node Centrifugo binding for the native personal-chat realtime seam.
 *
 * Verified server behavior (Cloud `cf298c60`
 * `services/message/pkg/infrastructure/server/http/proxy_handler.go`):
 * - `HandleConnect` returns the JWT's server-owned `channels`
 *   (`claims.Channels`, sole `personal:instance:<id>`).
 * - `HandleSubscribe` ALWAYS returns 403 "client subscriptions are
 *   disabled".
 *
 * Therefore this binding MUST NOT call `newSubscription`. It uses the
 * connection-level `publication` event that Centrifugo delivers for
 * server-side subscriptions, matching the proven
 * `@beeos-ai/message-sdk` Node factory. Tests must fail if
 * `newSubscription` is ever called.
 *
 * `centrifuge` is a real runtime dependency; tests inject
 * {@link CentrifugeRealtimeFactory} so only the transport is faked while the
 * publication payloads remain the real server-owned event shape.
 */

import { Centrifuge } from "centrifuge";
import WebSocket from "ws";

import type { RuntimeChatRealtimePort } from "./runtime-chat.js";

export interface CentrifugeClientLike {
  on(event: "connected", listener: () => void): void;
  on(event: "connecting", listener: () => void): void;
  on(event: "disconnected", listener: (ctx: { reason?: string }) => void): void;
  on(event: "error", listener: (ctx: { error?: unknown }) => void): void;
  on(event: "publication", listener: (ctx: { data: unknown }) => void): void;
  connect(): void;
  disconnect(): void;
}

export interface CentrifugeRealtimeFactory {
  create(options: { url: string; token: string }): CentrifugeClientLike;
}

const defaultFactory: CentrifugeRealtimeFactory = {
  create: ({ url, token }) =>
    // Node <22 has no global WebSocket and `centrifuge` does not supply one;
    // always hand it the real `ws` implementation (single transport path).
    new Centrifuge(url, {
      token,
      websocket: WebSocket as unknown as typeof globalThis.WebSocket,
    }) as unknown as CentrifugeClientLike,
};

export interface CentrifugeRealtimePortOptions {
  factory?: CentrifugeRealtimeFactory;
}

export function createCentrifugoRealtimePort(
  options: CentrifugeRealtimePortOptions = {},
): RuntimeChatRealtimePort {
  const factory = options.factory ?? defaultFactory;
  return {
    open(input) {
      let closed = false;
      const client = factory.create({ url: input.url, token: input.token });
      // Centrifugo pushes the server-owned subscription event as `ctx.data`;
      // forward it unchanged so nested `data` and unknown fields survive.
      client.on("publication", (ctx) => {
        const event = ctx.data;
        if (
          event !== null &&
          typeof event === "object" &&
          !Array.isArray(event) &&
          typeof (event as { type?: unknown }).type === "string"
        ) {
          input.onEvent(event as { type: string; data?: unknown });
        } else {
          input.onEvent({ type: "message.created", data: event });
        }
      });
      client.on("connecting", () => {
        if (!closed) input.onState("connecting");
      });
      client.on("connected", () => {
        if (!closed) input.onState("connected");
      });
      client.on("disconnected", () => {
        if (!closed) input.onState("disconnected");
      });
      client.on("error", (ctx) => {
        if (!closed) input.onError(ctx.error ?? ctx);
      });
      client.connect();
      return {
        close: () => {
          if (closed) return;
          closed = true;
          client.disconnect();
        },
      };
    },
  };
}
