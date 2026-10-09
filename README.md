# @beeos-ai/cloud-harness-sdk

Isolated-test TypeScript candidate for managed BeeOS Cloud runtime registration,
lease renewal, and durable command delivery. Source lives in the beeos-cloud-all
super-repository at `sdks/products/harness/typescript`.

The host retains its Ed25519 private key and supplies `signIdentityProof`.
The SDK signs canonical registration/heartbeat requests through that callback
and uses only the current Cloud lease for Message Service delivery. It owns
the single read/renew/ack loop and reconciles ambiguous append outcomes
through history without retrying an uncertain write.

OpenClaw owns command execution, WAL, poison policy and framework adaptation.
Product chat and independent Device Agent identity are not in this candidate.
No legacy Gateway or personal messaging token fallback is provided for the
managed Cloud runtime transport.
