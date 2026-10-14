# Upstream 0.2.16 integration

Target upstream: `4d9ac3e48` (main version synchronization after `v0.2.16`).
Fork baseline: `feb4b0711` (`0.2.11-custom.2`).

## Differences reviewed

Upstream changes touch 565 files; 40 overlap with the 148 files changed by the fork since the common ancestor. New features include Gemini explicit context caches, group localization, recharge bonus tiers, platform catalogs and consolidated upstream protocol routing. Fixes include Responses/Chat fallback, Gemini tool-result images, Haiku 5.5 and GPT-6.1 Sol context/pricing support.

The merge preserves user/group account policies, durable session ownership, model-scoped encrypted compaction checkpoints, bounded session recovery and usage/session tracing. The four existing fork SQL migration files are unchanged. Upstream migration filenames remain distinct despite shared numeric prefixes; existing SQL files were not renamed or rewritten.

Conflict resolutions:

- VERSION follows upstream 0.2.16; no release tag is created by this integration.
- Gemini retains the fork GET forwarding entry point and account access checks alongside the new model endpoints.
- Chat routing uses upstream's unified protocol resolver while retaining fork continuity checks and successful continuity completion. The obsolete OpenCode routing block is replaced by the unified resolver.

## Billing behavior

An unspecified channel billing source and the channel editor default now use `requested`. Requests without a channel source also prioritize the original client model over channel/account mapping for price lookup. Mapping still determines the actual upstream request and remains visible in usage logs.

Explicit `channel_mapped`, `upstream` and `response_model` settings retain their semantics. Existing persisted settings are not rewritten. Existing unpriced-alias fallbacks, composite alias safety checks and media-specific billing remain in place. Admission estimates follow the requested-model default and retain mapped-model fallbacks when the requested alias has no price.

Regression coverage checks actual cost and balance deductions with distinct requested, channel-mapped and account-mapped models, plus existing explicit-source and continuity tests in the full CI suite.

## Validation boundary

Builds and backend/frontend validation run on GitHub Actions; no compilation or production switch occurs on the production host. Local release matrix tests and Compose security/simple-mode checks pass. Production migration execution and paid generation smoke tests are outside this integration task.
