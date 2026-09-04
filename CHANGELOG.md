# Changelog

All notable changes to `br-service-engine` are documented here. The whole
workspace ships **one version**: every crate inherits `version.workspace = true`,
and a single git tag `v{version}` releases the set. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver.

## 0.1.0 - 2026-09-04

First engine release. `service-engine` ships the reactive personalized delivery
skeleton; `conformance-service-engine` ships its black-box battery.

### Added

- Registry and render core: `RenderRegistry` (`bind_noun`, `register_projector`,
  `register_rls`, `register_principal_resolver`), `SessionRuntime`
  (connect barrier, snapshot, `Reset`/`Upsert`/`Remove` over a contiguous
  per-session `Revision`, coalescing render pass, `PassReport`, GC), the delta
  table, cohort/RLS/foreign-axis routing, `Population::{Keys, Ordered, Query}`
  with `Interest` routing (a `Query` window's membership is re-evaluated from
  `populate` on every intersecting impact). A `Query` built with `with_keys` is
  authoritative: on every re-evaluation its membership becomes exactly
  `populate`'s result plus the keys discovered this pass, so a key that leaves
  the result is `Remove`d and per-session membership stays bounded by `populate`;
  a `Query` without `with_keys` is discovery-only and grows only from its
  predicate. Per-session fault isolation follows: a failed render or repair is
  retried and the session is ended after a config-raisable number of failed
  attempts (`EngineConfig::repair_attempts`), never served a
  `Reset` rebuilt from its stale last-sent view; a session left `repair_pending`
  after a failed reconnect resnapshot is retried by the housekeeping beat, so an
  idle pod with no further impact still repairs or ends it rather than serving
  the stale pre-gap view indefinitely. Cohort keys are
  collision-free: an RLS render group is keyed on the exact `PrincipalId` and a
  declared cohort on the exact bytes of its parts, never a 64-bit hash, so two
  principals can never share one RLS render. A focused go-live replay holds its
  replayed impacts only for the session going live, never re-holding them for
  other pending sessions.
- `Engine<P>` facade composing the render runtime, transport, accumulators,
  housekeeping beat and mirror supervision: `boot(config, pg, fabric,
  readiness)` (the caller owns the `ReadinessHandle`, so a boot that fails the
  posture or listener probe leaves it DOWN with the reason), `bind_noun`, the
  fallible `register_*` seams (`register_projector`, `register_rls`,
  `register_principal_resolver`, `register_accumulator`, `register_relay`,
  `register_cron`, `register_mirror` — each returns `Result` and rejects a
  duplicate name with a typed error, since every registry is keyed by name and a
  silent duplicate would overwrite a same-named component's health condition and
  hide a degraded one), `readiness`, `attach`, `push_chunk`, `seal`, `run`.
  `run` supervises its render, beat, flush and mirror workers: if one ends or
  panics before shutdown it flips readiness DOWN (fixed operator reason, the dead
  worker in the typed `EngineError::WorkerStopped`) and returns `Err`, never
  serving readiness over a dead loop. A mirror step that panics is caught and
  takes the same restart-and-backoff path as an error, so a mirror self-heals
  rather than freezing readiness at Converged over a dead mirror. `RowClaim` relays drain at the end of every render
  pass, so a command staged with its impact leaves within one window, not only on
  the beat. `attach` after the engine has begun shutting down returns
  `AttachError::ShuttingDown` rather than a stream that never ends.
- Impact transport over PostgreSQL `LISTEN`/`NOTIFY` (`PgListenNotify`):
  `stage_in` in the caller's transaction, `schedule_in` / `fire_due` for
  scheduled boundaries, a framed-group payload split that admits a frame only
  strictly below Postgres's 8000-byte `NOTIFY` limit (7999 bytes is the maximum)
  and whole reassembly, a self-repairing `listen()` stream surfacing every loss of
  continuity as `Reconnected`, and `queue_usage()`.
- Boot posture assertion (`assert_posture`: no superuser, no `rolbypassrls`, no
  ownership or membership of the engine schema/database) and the boot listener
  probe (`arm`/`fire`/`hear`) that holds readiness DOWN behind a
  transaction-mode pooler.
- Streaming accumulators keyed by the source's own `ChunkSeq` (a checked newtype
  bounded to the range a `bigint` column stores faithfully — a value above it is
  refused typed at construction, never wrapped negative and treated as a gap):
  per-chunk `Durable` flush receipts, fold-stops-at-a-gap, seal verdict on the
  flush transaction under an advisory lock, buffer ceiling, and a table-verified
  fold cache bounded by `EngineConfig::fold_cache_capacity` (LRU eviction, so a
  stream of never-sealed keys cannot grow the cache without bound) with a
  whole-stream sweep. A chunk resubmitted at an already-durable sequence with
  identical content is an idempotent `Durable`; the same sequence with different
  content is a typed `EngineError::ChunkConflict` (never a silent replay), so
  `Durable` means this payload is durable, not that some payload occupies the
  sequence.
- Outbox relays with `RowClaim` and `Leader` disciplines (leader slot as a
  lease over `leader_slot`, quantised database-clock slot), the hosted
  `FabricOutboxRelay` draining through its `hosted_drain` seam, and
  `KvDrainRelay` publishing the identity published language monotonically by key
  and version. Monotonicity holds across deletion: a per-key watermark persisted
  in the engine's own schema (so it survives restarts) is consulted before every
  write, so a stale or replayed `Put` that arrives after a newer `Retract` is a
  no-op rather than resurrecting the tombstoned key.
- Cron over slot leases: five-field UTC `croner` grammar plus `EveryBeats` and
  the anchored `Every { period, anchor }`, once-per-slot claim on `leader_slot`,
  catch-up bounded by slot retention, and a `Never` schedule refused at
  registration.
- Mirror supervision (`MirrorSupervisor`): re-reconcile before re-watch,
  the registered backfill run once at adoption (none unless the service supplies
  one), and readiness gated on mirror convergence.
- Observability: `service_engine_*` metrics through the `metrics` facade.
- Postgres schema in the reserved migration range, applied by `schema::migrate`
  (`ignore_missing`) with `grant_engine_access`. The engine owns five tables:
  `scheduled_impact`, `leader_slot`, `accumulator_chunk`, `accumulator_seal`,
  and `kv_relay_watermark` (the per-key KV publish watermark that survives
  restarts). Scheduled boundaries are claimed against the database clock
  (`now()` in the claiming statement), never the pod clock, so a skewed pod
  never fires a boundary early or late.
- `conformance-service-engine`: the named scenarios `s01`–`s25` plus
  `s26_pooler_probe` and `s27_worker_supervision`, run against a fresh database
  and a spawned `nats-server` per test; a two-slice sample service with a
  tenant-bearing sample principal, a synthetic streaming source, and its own
  infra fixtures (`infra/pg.rs`, `infra/nats.rs` — the sole `async-nats` user,
  only to declare gitops-owned streams and buckets). Scenarios pin the fixes to
  the review findings: worker supervision (`s27`), reconnect-resnapshot repair
  and bounded end (`s13`), the chunk-content conflict verdict (`s14`), the
  bounded `ChunkSeq` boundary (`s14_chunk_seq_bounds`), and the KV
  no-resurrection-after-retract guarantee (`s15`).
- `Timestamp` is a newtype truncated to microseconds at construction, so every
  instant that crosses the PostgreSQL `timestamptz` boundary (cron and leader
  slots, scheduled boundaries, seal times) round-trips equal on nanosecond
  clocks; a sub-microsecond value is unrepresentable.

### Deployment constraint

- No transaction-mode pooler in front of an engine service: `LISTEN` is session
  state a transaction pooler drops silently. The engine proves the path with the
  boot probe and holds readiness DOWN when the probe is not heard, so a
  mispooled service never becomes ready.

## 0.0.0 - 2026-09-02

### Added

- Repository scaffold: workspace root, governance files (LICENSE, CONTRIBUTING,
  SECURITY, SUPPORT, PR template, issue-template config), `.gitignore`, and
  `deny.toml`.
- Two empty crates — `service-engine` (the engine) and
  `conformance-service-engine` (its black-box conformance battery) — carrying no
  dependency and no code. Both ship with 0.1.0.
- CI (`ci.yml`): fmt + clippy + test, MSRV 1.88 build, `cargo doc`, `cargo-deny`,
  `cargo-machete`, `cargo semver-checks`, changelog + README-pin check,
  shellcheck, trufflehog secret scan, and the conformance battery against real
  PostgreSQL 16 + NATS JetStream.
- CD (`release-tags.yml`): auto-tag and release the unified workspace version on
  merge to `main`.

No engine functionality.
