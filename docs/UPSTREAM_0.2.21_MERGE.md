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

All 300 existing SQL migrations, including the four fork migrations, are unchanged.
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

Local release matrix tests (10 tests), Compose security/simple-mode/gateway-env
checks, runtime resource checks and the Caddy cache/SSE policy check pass.
The only diff whitespace warning is an existing extra blank line in upstream
migration 250; the upstream SQL is retained byte for byte.

At integration time GitHub refused CI dispatch with HTTP 422, "Actions has been
disabled for this repository." Enabling the individual CI workflow did not
remove the repository restriction. The configured token cannot administer
repository Actions permissions (HTTP 403). The Actions page identified a
fork-wide pause due to usage scale; the owner re-enabled workflows. CI now also
supports workflow_dispatch for explicit integration-branch validation.

The first full CI passed frontend, lint and release-helper checks. Its unit test
run passed the service suites, including the fork's billing/continuity tests,
but found three newly added upstream handler tests expecting English error
messages. They now assert the fork's existing local diagnostic codes and Chinese
messages while retaining the upstream status, validation and body-safety checks.
Production behavior was not changed to accommodate these test expectations.

The security scan identified reachable standard-library advisory GO-2026-6859
(`net/http` on Go 1.27.3, fixed in Go 1.27.4). The Go module minimum, both
Docker build defaults and all CI/release version assertions now consistently
use Go 1.27.4. The vulnerability check remains enforced without exceptions.
See https://go.dev/doc/devel/release#go1.27.4.

Builds and backend/frontend tests run on GitHub-hosted runners. This code merge
does not authorize production migration execution, paid API calls or deployment.

## Final validation evidence (2026-11-13)

- Integrated application code at `58058a8a514390efe2ec6d6409dd7e977f50e9b6`:
  all five CI jobs passed, including backend unit/integration tests and the
  macOS Apple-container lifecycle tests:
  https://github.com/hlj11111/sub2api/actions/runs/43403750694.
- Final build-toolchain patch at `1c46c383ccd6fd48c3d5045fcee2bc19b59db329`:
  backend unit/integration tests, frontend checks, golangci-lint and release
  helpers passed:
  https://github.com/hlj11111/sub2api/actions/runs/43404223485.
  GitHub cancelled the repeated macOS shell job at 10:02:22 UTC after it could
  not acquire a hosted runner. The check annotation states: "The job was not
  acquired by Runner of type hosted even after multiple attempts" and reports
  macOS arm64 capacity constraints. Its Apple script, lifecycle tests, fixtures
  and all shell test files are byte-identical to the preceding fully successful
  run. Portable Compose, runtime-resource and Caddy checks were also rerun
  successfully after the toolchain patch. This cancelled repeat is not reported
  as a completed pass; macOS coverage comes from the preceding successful run.
- Both final frontend and backend security scans passed:
  https://github.com/hlj11111/sub2api/actions/runs/43404223534.

Only this validation document changes after the final tested code commit.
No release tag/image was published and production was not switched by this merge.
