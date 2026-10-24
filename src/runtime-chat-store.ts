import { RuntimeLeaseHttp, type RuntimeLeaseCredential } from "./runtime-lease-http.js";
import type { ChatAppendRequest, ChatCommitResult, ChatLoadRequest, ChatLoadResult, ChatReceipt, ChatReceiptRequest } from "./chat-store-contract.js";

/** Internal State routes, shared by the two native drivers; these are not public SDK operations. */
export const RUNTIME_CHAT_STORE_ROUTES = Object.freeze({
  load: "/api/v1/runtime/chat-store/load",
  append: "/api/v1/runtime/chat-store/append",
  receipt: "/api/v1/runtime/chat-store/receipt",
});

export type ChatStoreBody = ChatLoadRequest | ChatAppendRequest | ChatReceiptRequest;

export class ChatStoreHttpError extends Error {
  constructor(readonly status: number, readonly code: string, message: string) {
    super(message);
    this.name = "ChatStoreHttpError";
  }
}

/** The request may have committed. Reconcile the original key/digest; never rerun the model. */
export class ChatStoreOutcomeUnknown extends Error {
  readonly code = "outcome_unknown";
  constructor(readonly request: ChatStoreBody, options?: ErrorOptions) {
    super("central chat append outcome is unknown; query its original receipt", options);
    this.name = "ChatStoreOutcomeUnknown";
  }
}

/** Lossless native payload transport. The State producer and native adapter own the typed codecs. */
export class RuntimeChatStore {
  constructor(private readonly http: RuntimeLeaseHttp) {}

  async load(credential: RuntimeLeaseCredential, body: ChatLoadRequest, signal?: AbortSignal): Promise<ChatLoadResult> {
    return this.request("load", credential, body, signal) as Promise<ChatLoadResult>;
  }

  async append(credential: RuntimeLeaseCredential, body: ChatAppendRequest, signal?: AbortSignal): Promise<ChatReceipt> {
    return this.commitResult(await this.request("append", credential, body, signal), body);
  }

  async receipt(credential: RuntimeLeaseCredential, body: ChatReceiptRequest, signal?: AbortSignal): Promise<ChatReceipt> {
    return this.commitResult(await this.request("receipt", credential, body, signal), body);
  }

  private commitResult(value: unknown, body: ChatAppendRequest | ChatReceiptRequest): ChatReceipt {
    const result = value as Partial<ChatCommitResult>;
    const receipt = result.receipt;
    if (typeof result.replay !== "boolean" || !receipt || typeof receipt !== "object" ||
        !receipt.receipt_id || receipt.idempotency_key !== body.idempotency_key ||
        receipt.payload_sha256 !== body.payload_sha256 || receipt.session_id !== body.session_id ||
        receipt.history_generation !== body.history_generation || receipt.branch_id !== body.branch_id) {
      throw new ChatStoreOutcomeUnknown(body);
    }
    return receipt;
  }

  private async request(operation: keyof typeof RUNTIME_CHAT_STORE_ROUTES, credential: RuntimeLeaseCredential,
    body: ChatStoreBody, signal?: AbortSignal): Promise<unknown> {
    if (!credential.runtimeLeaseCredential || !credential.executionGrant) {
      throw new Error("central chat requires the current runtime lease and execution grant");
    }
    let response: Response;
    try {
      response = await this.http.postJson(RUNTIME_CHAT_STORE_ROUTES[operation], credential, body,
        { retry: false }, signal);
    } catch (cause) {
      if (operation === "append") throw new ChatStoreOutcomeUnknown(body, { cause });
      throw cause;
    }
    let value: unknown;
    try {
      value = await response.json();
    } catch (cause) {
      if (operation === "append") throw new ChatStoreOutcomeUnknown(body, { cause });
      throw new Error("central chat returned invalid JSON", { cause });
    }
    if (!response.ok) {
      const error = value && typeof value === "object" && "error" in value ? value.error : value;
      const detail = error && typeof error === "object" ? error as Record<string, unknown> : {};
      const failure = new ChatStoreHttpError(response.status,
        typeof detail.code === "string" ? detail.code : "chat_store_failed",
        typeof detail.message === "string" ? detail.message : `central chat request failed (${response.status})`);
      if (operation === "append" && response.status >= 500) {
        throw new ChatStoreOutcomeUnknown(body, { cause: failure });
      }
      throw failure;
    }
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      if (operation === "append") throw new ChatStoreOutcomeUnknown(body);
      throw new Error("central chat returned an invalid response object");
    }
    return value;
  }
}
