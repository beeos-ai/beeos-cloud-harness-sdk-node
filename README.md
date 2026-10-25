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

Use `RuntimeOperationFiles.resolveOutput(credential, operationId, body)` on the
Agent Gateway origin to read a file this run has already confirmed. The body is
`{ requestMessageId, paramsHash, activeRunRegistrationId, fileId }`. Cloud checks
the current lease and the exact tenant, owner, instance, agent and run. Pending
files and files from other runs are refused.

The result includes those run bindings, `file`, `download` and
`transferPolicy.allowedOrigin`. Check all bindings and the requested file ID.
Download only from the allowed HTTPS origin, with redirects disabled and the
returned signed headers. Do not send the runtime lease to object storage.
Check the downloaded size and SHA-256; the signed link lasts five minutes.
Input attachments continue to use `resolveInput`.

## Releasing

Publishing runs in GitHub Actions; do not run `npm publish` locally.

- Push a `harness-sdk-node-v<package version>` tag to a commit on `main` (e.g.
  `harness-sdk-node-v1.0.1-cloud.1`). `.github/workflows/publish.yml` verifies
  the tag is on `main`, matches `package.json` `version`, and that `npm pack`
  contains every entry declared in `files`.
- Prerelease versions (`-cloud.N`) publish under the npm dist tag `cloud`;
  stable versions publish under `latest`.
- To backfill a release, run the `Publish Harness SDK to npm` workflow manually
  (`workflow_dispatch`) with either `tag` or `version`; `version` derives
  `harness-sdk-node-v<version>`, and both must agree with `package.json`.
- The workflow authenticates with the `NPM_TOKEN` repository secret
  (`NODE_AUTH_TOKEN`) and publishes with `--access public`.

Run `npm test` and `npm run build` before release. File-route tests verify the
body, escaped operation path, lease header and redirect policy.

Source and issues: [beeos-cloud-harness-sdk-node](https://github.com/beeos-ai/beeos-cloud-harness-sdk-node).

## License

MIT. See [LICENSE](LICENSE).
