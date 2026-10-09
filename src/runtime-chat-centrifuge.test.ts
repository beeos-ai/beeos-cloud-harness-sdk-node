import { describe, expect, it, vi } from "vitest";

import {
  createCentrifugoRealtimePort,
  type CentrifugeClientLike,
  type CentrifugeRealtimeFactory,
} from "./runtime-chat-centrifuge.js";

class FakeClient implements CentrifugeClientLike {
  readonly handlers = new Map<string, Array<(ctx: never) => void>>();
  readonly newSubscription = vi.fn(() => {
    throw new Error("client subscriptions are disabled (server-owned channel)");
  });
  connected = false;
  disconnected = false;
  on(event: string, listener: (ctx: never) => void): void {
    const list = this.handlers.get(event) ?? [];
    list.push(listener as (ctx: unknown) => void);
    this.handlers.set(event, list);
  }
  connect(): void {
    this.connected = true;
  }
  disconnect(): void {
    this.disconnected = true;
  }
  emit(event: string, ctx: unknown): void {
    for (const listener of this.handlers.get(event) ?? []) listener(ctx);
  }
}

function build() {
  const client = new FakeClient();
  const factory: CentrifugeRealtimeFactory = { create: () => client };
  const port = createCentrifugoRealtimePort({ factory });
  const events: Array<{ type: string; data?: unknown }> = [];
  const states: string[] = [];
  const errors: unknown[] = [];
  const session = port.open({
    token: "lease-token",
    url: "wss://centrifugo.example/ws",
    channel: "personal:instance:inst-1",
    onEvent: (event) => events.push(event),
    onState: (state) => states.push(state),
    onError: (error) => errors.push(error),
  });
  return { client, session, events, states, errors };
}

describe("Centrifugo realtime binding (server-owned channel)", () => {
  it("never creates a client subscription; consumes connection publications", () => {
    const { client, events, states } = build();
    expect(client.newSubscription).not.toHaveBeenCalled();
    expect(client.connected).toBe(true);

    client.emit("connected", undefined);
    client.emit("connecting", undefined);
    client.emit("publication", {
      data: {
        type: "message.created",
        data: { message: { id: "m1", unknown: "kept" } },
      },
    });

    expect(states).toEqual(["connected", "connecting"]);
    expect(events).toEqual([
      {
        type: "message.created",
        data: { message: { id: "m1", unknown: "kept" } },
      },
    ]);
  });

  it("maps message.terminal publications and tears down on close", () => {
    const { client, session, events, states, errors } = build();
    client.emit("publication", {
      data: {
        type: "message.terminal",
        data: { message_id: "m1", state: "completed" },
      },
    });
    client.emit("disconnected", { reason: "transport closed" });
    client.emit("error", { error: new Error("boom") });
    session.close();

    expect(events[0]).toMatchObject({ type: "message.terminal" });
    expect(states).toContain("disconnected");
    expect((errors[0] as Error).message).toBe("boom");
    expect(client.disconnected).toBe(true);
  });

  it("registers connection-level handlers only and fails on any subscription call", () => {
    const { client, session } = build();
    // Connection-level publication only: no subscription lifecycle handlers.
    expect([...client.handlers.keys()].sort()).toEqual([
      "connected",
      "connecting",
      "disconnected",
      "error",
      "publication",
    ]);
    // The fake factory makes any client-subscription attempt a hard failure.
    expect(client.newSubscription).not.toHaveBeenCalled();
    expect(() => client.newSubscription("personal:instance:inst-1")).toThrow(
      /client subscriptions are disabled/,
    );
    session.close();
  });
});
