# Upstream 0.2.21 integration

Upstream target: release tag `v0.2.21`, commit `10ba8f9b4dc2e6023d4a9879ab77b21841babd10`.
Fork baseline: `22639ef35` (`0.2.16-custom.1`). The upstream tag still contains
`0.2.20` in VERSION; this integration sets VERSION to `0.2.21` to match the
selected release. It does not include commits after that tag.

## Scope and preserved behavior

The upstream side changes 899 files, with 60 files also changed by the fork
since the common ancestor. New upstream features include scoped administration
keys, management MCP/OAuth, newer model support and pricing, OpenAI cache-write
lifetimes, image billing options and transport/resource safety fixes.

The integration preserves user/group account policies and pins, persistent
model-scoped session ownership, encrypted compaction checkpoints, bounded
session recovery, local error diagnostics and usage/session tracing.

Eight manual conflict resolutions combine both implementations:

- Gateway configuration retains recovery limits and adds upstream minimum
  output-token configuration and validation.
- The migration runner retains both session and API-key-cost index recovery.
- Upstream drain timeout arguments remain supported; an established continuity
  lifetime remains authoritative so disconnected operations cannot outlive the
  session lease grace period.
- Scheduler binding preservation covers continuity, guardian affinity and the
  upstream image-size spillover pass.
- Response handlers retain checkpoint capture after restoring client-visible
  payloads, alongside upstream instruction restoration and timestamp fixes.
- WS replay keeps complete output during continuity recovery and during OAuth
  search activation. Ordinary paths retain upstream replay selection.

Default billing still uses the original requested model before channel/account
mapping. Explicit channel `channel_mapped`, `upstream` and `response_model`
settings remain effective; existing settings are not rewritten. Unpriced alias
fallbacks, composite alias safeguards and media-specific billing remain intact.

## Migrations

All existing SQL migrations, including the four fork migrations, are unchanged.
Six upstream SQL files are added with distinct full filenames (the two `249`
prefixes are intentional). They add scoped admin/MCP tables, image-count billing,
cache-write fields and a concurrent usage-cost index. Migration 246 enables image
generation for existing Gemini/Antigravity groups that previously had it disabled.
Deployment must review this data change and execute all migrations on a restored
production database before switching production, as required by AGENTS.md.

## Verification

The full GitHub CI includes backend unit and integration tests, frontend lint,
type checking and critical component tests, golangci-lint, shell checks and
release-helper tests. Fork regression suites cover policy enforcement, persistent
bindings, model isolation, encrypted checkpoint recovery and requested-model
cost/balance deductions with distinct request/channel/account models.

Additional merge regression coverage checks complete assistant/tool history on
WS reconnect with continuity enabled, recovery defaults alongside new upstream
configuration, and continuity drain enforcement with a longer transport timeout.

Builds and backend/frontend tests run on GitHub-hosted runners. This code merge
does not authorize production migration execution, paid API calls or deployment.
