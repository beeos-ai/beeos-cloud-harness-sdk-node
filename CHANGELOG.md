# Changelog

All notable changes to `@beeos-ai/cloud-harness-sdk` are documented here. This
project follows Semantic Versioning.

## [1.0.1-cloud.1] - 2026-10-25

Staging prerelease of 1.0.1, published under the `cloud-staging` dist-tag; `latest`
stays 1.0.0 until 1.0.1 itself is released. Advances past `1.0.0-cloud.7`.
Additive: existing exports keep their signatures.

- Export the runtime original-intent, memory and chat-store contracts for native
  runtimes: `runtime-original-intent`, `runtime-memory`, `runtime-chat-store` and
  `chat-store-contract`.
- `GatewayCanvas.token(init?, sandboxId?)`: a Sandbox v1 guest may name its claimed
  `sandboxId`, so the Gateway signs an Agent-scoped token from the Sandbox Binding.
  Without it the request body is unchanged.
- Runtime lease HTTP: `resilience.retry === false` now disables retries explicitly
  instead of falling back to the default policy.
