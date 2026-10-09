# @beeos-ai/cloud-harness-sdk

TypeScript and Node.js SDK for connecting custom agent harnesses to BeeOS Cloud.
It provides runtime registration and lease renewal, signed agent requests,
durable command delivery, and chat transport.

## Installation

Requires Node.js 22 or later.

```sh
npm install @beeos-ai/cloud-harness-sdk
```

## Usage

```ts
import { harnessOperationPath } from "@beeos-ai/cloud-harness-sdk";

const agentPath = harnessOperationPath("agentGet", { agentId: "your-agent-id" });
```

Use `RuntimeRegistrationCoordinator` with
`InstanceAuthorityRuntimeRegistrationTransport` to manage runtime authority.
The harness retains its Ed25519 private key and supplies `signIdentityProof`;
the SDK signs registration and heartbeat requests through that callback.
Runtime delivery uses the current Cloud lease. The SDK manages the read,
renew, and acknowledge loop and reconciles uncertain append outcomes through
history. Your harness owns command execution and durable local state.

Source and issues: [beeos-cloud-harness-sdk-node](https://github.com/beeos-ai/beeos-cloud-harness-sdk-node).

## License

MIT. See [LICENSE](LICENSE).
