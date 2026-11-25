# Option 2 — Engineering Findings (paginated discovery + staged enrichment)

Status: IMPLEMENTATION PR (unmerged, no publication, no production workload).
Scope: grave-indexer `feat/option2-pipeline-hardening` + companion SDK PR in
graveyield-protocol `feat/sdk-last-swap-early-exit` (submodule pin NOT moved).
Companion PR: graveyield-protocol — SDK last-swap early exit (see §8).

This document records the evidence-backed findings the PR implements and the
items that remain owner decisions. M1 invariants (C1–C6, 40/40/20 allocation,
atomic settlement, D-01 sourcing, attestation gates, schema-v2 contract) are
unchanged throughout.

---

## 1. Verified baseline (Phase 0)

| Item | Value |
|---|---|
| Repo HEAD at branch time | `main` @ `8fc81bf8281e1e63d77c59d953d246251f02d793` (PR #5 merge) |
| Submodule pin | `vendor/protocol` @ `2a8aac1` (graveyield-protocol PR #105 merge) |
| Indexer offline suite | 568/568 pass (`pnpm run test:offline`) |
| Protocol SDK suite (pinned) | 179/179 pass |
| Protocol SDK suite (main @ 0fd4abb) | 240/240 pass |
| Provider gPA-V2 capability | Public devnet + mainnet RPC: `-32601 Method not found` (probed 2026-11-23, single bounded request each) |

Material differences from the previous investigation's assumptions:

* The raw enumeration code already uses a 160-byte `dataSlice` identity
  window (NOT the full 752-byte payload) and an optional server-side
  `pc_mint@432` WSOL memcmp filter — the ~180–240 MB response estimate from
  the earlier investigation applied to the UNFILTERED corpus; the
  memcmp-filtered response is smaller but still one unpaginated response.
* `max_pools` (default 500) limits local PARSING only; the RPC response size
  is unaffected (confirmed — the full response is still fetched and decoded
  before the parse loop's cap can stop it).
* Enrichment is sequential across pools with ~4 token-fact calls + 1
  activity derivation per pool, and the activity derivation fetches the pool
  AmmInfo AGAIN (vaults were not passed through) — confirming bottlenecks
  3, 4 and 6.
* The SDK last-swap derivation scans every returned signature (up to
  `scanLimit`, default 1,000) with one `getTransaction` per resolved
  candidate — confirming bottleneck 5.

## 2. Retry-aware transport (Part 2 §1-B) — implemented

`src/rpcGovernance.ts`:

* Bounded retries: attempt cap (`RPC_MAX_RETRIES`, default 3), exponential
  backoff with jitter, backoff ceiling (`RPC_MAX_DELAY_MS`), overall
  wall-clock deadline (`RPC_DEADLINE_MS`).
* `Retry-After` (integer seconds or HTTP-date) honored with priority over
  computed backoff, capped at the backoff ceiling.
* Typed failure states: `RpcRetriesExhaustedError` (carries the ACTUAL
  attempt count), `RpcDeadlineExceededError`, `RpcHttpError` (status +
  parsed Retry-After). A failed operation NEVER yields a fabricated result.
* Retryable: HTTP 429/5xx, JSON-RPC -32005 (provider rate limit), transport
  failures. Non-retryable: other 4xx and RPC errors (deterministic).
* Wired into: classic discovery gPA transport, enrichment token facts
  (per-call path), batch transport, paginated page transport, capability
  probe (single attempt — a probe is a capability question, not a workload).

## 3. Bounded concurrency + rate governance (Part 2 §1-C) — implemented

* `RateGovernor`: process-wide shared token bucket + in-flight cap,
  provider-aware defaults (public/open endpoints: 4 rps / 4 concurrent;
  operator-provisioned endpoints: unlimited unless `RPC_RATE_LIMIT_PER_SEC`
  is set).
* Enrichment now runs under `ENRICHMENT_CONCURRENCY` (default 4, was:
  sequential). `mapWithConcurrency` returns results in INPUT order; all
  accounting (failure records, health, enrichment map) happens in a single
  deterministic post-pass — **artifact byte-stability for identical observed
  inputs is preserved** and is pinned by test.
* Per-pool accounting unchanged: activity failures, gate rejections and
  enrich failures remain per-pool, recorded in the same order as the
  sequential implementation.

## 4. Validated RPC batching (Part 2 §1-D) — implemented, OFF by default

`src/rpcBatch.ts` + opt-in `RPC_BATCH_ENABLED=true`:

* One validated JSON-RPC batch per pool (2 × `getTokenAccountBalance`,
  2 × `getTokenSupply`) replaces 4 logical round-trips.
* Validation: request-id mapping (out-of-order responses are keyed by id),
  per-item errors → UNRESOLVED `item-error`; missing items → UNRESOLVED
  `missing`; duplicate ids → UNRESOLVED `duplicate`; ids outside the
  requested range → the WHOLE batch fails closed (`mapping`); malformed
  elements → UNRESOLVED `malformed`; non-array responses → UNRESOLVED
  `batch`. A missing response is NEVER a negative result — an unresolved
  fact becomes an explicit per-pool enrich failure (pinned by test: the
  artifact then fails enrichment completeness and exits non-zero).
* Batch limits are NOT assumed: the batch size is fixed (4) and the feature
  is off until the owner validates provider batch behavior.

## 5. Vault passthrough (Part 2 §1-A) — implemented

`ActivityIndexerLive.indexActivity` now accepts `coinVault`/`pcVault` and
forwards them to the SDK's `deriveLastSwapEvidenceV4` as the authoritative
vault pair. `cmdConsoleSnapshot` passes the discovery record's parsed AmmInfo
identity. This eliminates the redundant on-chain pool fetch inside EVERY
activity derivation (bottleneck 6) with NO change to identity/ownership
validation: the SDK treats caller-provided vaults as authoritative exactly
as its own fetch, still failing closed (`pool_vaults_unavailable`) when
vaults are unavailable. The C1 test fixtures were updated to carry the REAL
fixture pool's vaults in the discovery identity slice — under passthrough,
discovery and derivation must read the same account, exactly as in
production.

## 6. SDK early exit (Part 2 §1-E) — implemented as a COMPANION PR

The SDK lives in the `vendor/protocol` submodule
(`GraveYield/graveyield-protocol`). Per Part 4 ("modify another repository
only with a reported requirement"), the change is delivered as a separate,
unmerged PR on `feat/sdk-last-swap-early-exit` (base: protocol `main`
@ `0fd4abb`). The grave-indexer submodule pin is NOT moved — that bump is an
owner decision (it also brings the submodule past PR #117).

Mechanism (in `deriveLastSwapEvidenceV4`): stop rule evaluated once per
finalized record — when a verified swap is established AND no NEWER
candidate is unresolved, the loop breaks; older signatures are never
fetched. The scan NEVER stops while a newer candidate is unresolved
(blocking rule absolute; loop continues bounded by `scanLimit`).
`scan.earlyExit` / `scan.candidatesExamined` are additive telemetry;
`candidates` is truncated at the exit point ("not examined", never "not
swaps" — no record is fabricated). Parity suite (10 scenarios, each run
through early-exit ON and OFF): swap at newest signature; verified swap
after newer non-economic candidates; inactive pool with the last swap
inside the window; unresolved newer candidate; transaction-fetch failure;
malformed transaction; no verified swap in the window; bounded scan not
reaching history end; signature ordering; default-option behavior.
Measured fetch savings: 1 vs 3 fetches (swap at newest), 21 vs 100 (swap at
index 20 of 100), identical fetch counts when nothing verifies (no savings,
as expected).

**Historical coverage (Part 2 §4) is NOT changed and NOT implemented:**
authoritative backward paging requires documented provider
archival/retention coverage, which is not established. No empty-page
backward inference exists anywhere; `no_swap_in_scan` still requires
reachedHistoryEnd + every candidate resolved, exactly as before. Early exit
and backward paging remain separate changes.

## 7. Paginated discovery (Part 2 §2, Part 3 Phase 3) — implemented behind an explicit capability gate

`src/discoveryPaginated.ts`:

* `probeGetProgramAccountsV2` — single bounded probe. Verified evidence
  (2026-11-23): public devnet and mainnet endpoints answer `-32601` → the
  gate stays CLOSED there. A transport failure or any unexpected answer
  also keeps the gate closed.
* `makeHeliusGpaV2Provider` — page-based adapter implementing the provider
  contract AS PUBLISHED (`page` param, 1-based; empty page = end of
  enumeration; optional `context.slot`). **CONTRACT STATUS: UNVERIFIED
  against a live provider** (no provider credentials in CI). Cursor
  progression, no-gap/no-duplicate behavior, behavior during concurrent
  account changes, retention and provider limits remain owner verification
  items (Part 2 §2's checklist).
* Page ledger: per page — index, account count, sha256 fingerprint of the
  ordered pubkey list (receiving proof, NOT completeness proof), provider
  context slot, receive time, retries used, end condition, error.
* Cross-page duplicates are counted and kept-first (never silently
  dropped); page-budget exhaustion, page-request failure and the
  undocumented-contract state each produce an explicit `incomplete` reason.
* **By default (`requireDocumentedContract`, an owner-only switch) every
  paginated run reports `incomplete / provider-contract-unverified` even
  when all pages were received** — the honest ceiling until the provider
  contract is verified.
* Modes (`DISCOVERY_PAGINATION`): `off` (default — exact classic behavior),
  `probe` (gate + fail-closed handling), `require` (capability failure is a
  hard error; no artifact). Classic fallback is NEVER silent:
  `DISCOVERY_CLASSIC_FALLBACK=bounded` runs the capped classic path whose
  completeness is refused above `CLASSIC_FALLBACK_COMPLETE_MAX_ACCOUNTS`
  (default 10,000) — at corpus scale (~561k filtered accounts) a classic
  fallback can NEVER claim completeness.

## 8. Slot-observation semantics (Part 2 §3) — corrected

* Discovery stats now carry `observation`: provider identity, commitment,
  the actual response `context.slot` (classic) or per-page context slots
  (paginated), and an explicit bounded-window description.
* Every log line and the paginated run state that a lower/upper slot
  bracket — even within 400 slots — is NOT an atomic snapshot: accounts may
  change during the window and pages may observe different context slots.
  **No point-in-time snapshot is claimed anywhere.** The artifact schema is
  unchanged (Console compatibility); this is provenance + observability.

## 9. C3/C4 pre-funnel (Part 2 §5, Phase 4) — INVESTIGATION ONLY, not activated

Findings:

1. **Current artifact surface.** Schema-v2 criteria are
   `c1Inactivity`/`c3MinTvl` (evaluated) and `c2PriceCollapse`, `c4LpBurned`,
   `c5LpLocked`, `c6MultiEpoch` — all hard-pinned `"pending"` in the
   producer (`consoleSnapshot.ts::deriveCriteria`). The strict parser and
   the independent completeness verifier accept exactly these shapes.
2. **What the proposed pre-funnel would change.** Skipping activity
   derivation for pools failing the cheap C3/C4 checks
   (WSOL quote-side vault ≥ 500,000,000 raw; LP supply > 1,000 raw) removes
   the only input C1 evaluates (`lastSwap`). Those pools would render C1
   `"pending"` instead of a computed `"pass"`/`"fail"` — an artifact-behavior
   change for a subset of pools. C1's evaluation and rendering must remain
   byte-compatible, so the funnel CANNOT be activated inside schema-v2.
3. **Fail-closed machine-verifiability of a skip state.** The verified
   paths are: (a) the producer marks skipped pools with an explicit
   `failures` entry (stage `funnel-skip`, free-form today) AND the pool
   simply has no enrichment entry (already fail-closed: un-enriched pools
   are excluded from the artifact and force enrichment-completeness INCOMPLETE);
   or (b) a versioned schema-v3 with explicit criterion states
   (`"skipped:funnel-c3c4"`) validated by the strict parser, mirrored in the
   independent verifier, and coordinated with the Console renderer.
   Option (a) is safe today but makes the artifact INCOMPLETE by the
   existing contract (the skipped pools are missing) — honest, but not the
   intended "render with pending criteria". Option (b) is the recommended
   versioned schema change and requires a coordinated producer/verifier/
   Console rollout.
4. **Recommendation.** Do NOT activate. Pursue (b) as a separate schema-v3
   PR after the owner confirms the Console can render new criterion states.
   The savings estimate (skipping ~6+N RPC calls for non-viable pools) is an
   estimate only; measure on a bounded run before deciding.

## 10. Operational configuration (defaults preserve behavior)

| Env | Default | Meaning |
|---|---|---|
| `RPC_MAX_RETRIES` | 3 | bounded retry attempts |
| `RPC_BASE_DELAY_MS` / `RPC_MAX_DELAY_MS` | 250 / 8000 | backoff schedule |
| `RPC_DEADLINE_MS` | 45000 | per-operation wall-clock budget |
| `ENRICHMENT_CONCURRENCY` | 4 | enrichment workers (order-preserving) |
| `RPC_RATE_LIMIT_PER_SEC` | 0 (provider-aware) | shared governor budget |
| `RPC_BATCH_ENABLED` | false | validated batch path |
| `DISCOVERY_PAGINATION` | off | off / probe / require |
| `DISCOVERY_MAX_PAGES` | 100 | page budget |
| `DISCOVERY_CLASSIC_FALLBACK` | none | none / bounded (explicit only) |
| `CLASSIC_FALLBACK_COMPLETE_MAX_ACCOUNTS` | 10000 | classic-fallback completeness cap |

Rollback: every new path is behind an env switch whose default reproduces
the previous behavior (single-attempt exceptions: retry/concurrency ship ON
with conservative defaults; both are pure process-local changes and revert
by setting `RPC_MAX_RETRIES=0`, `ENRICHMENT_CONCURRENCY=1`). The SDK early
exit is reverted by the submodule pin (unchanged here).

## 11. Production blockers (must be resolved by the owner)

1. Provider pagination contract verification (Part 2 §2 checklist) with
   provider credentials — until then `provider-contract-unverified` is the
   honest ceiling and no corpus-scale completeness is claimable.
2. `RPC_BATCH_ENABLED` rollout after provider batch-limit validation.
3. SDK early-exit submodule bump after the companion PR merges.
4. Historical backward paging: requires documented retention coverage;
   remains unimplemented by design.
5. C3/C4 funnel: requires the schema-v3 decision in §9.
6. Production caps/rate limits must be chosen from MEASURED bounded runs
   (Part 3 Phase 5) — not from this document's estimates.
