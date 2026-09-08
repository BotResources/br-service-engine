# Changelog

All notable changes to `br-service-engine` are documented here. The whole
workspace ships **one version**: every crate inherits `version.workspace = true`,
and a single git tag `v{version}` releases the set. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver.

## 0.1.0 - 2026-09-04

First engine release. `service-engine` ships the reactive personalized delivery
skeleton; `conformance-service-engine` ships its black-box battery.

### Changed (0.1.0 rework, unit U1)

- The engine owns its NATS layer. Its internal loops (stream/bucket bind, KV
  read/write/watch, outbox publish) run on `async-nats` directly through the new
  `nats` module (`Nats`, `KvBucket`, `KvKey`, `RelayHealth`, `PublishOutcome`).
  The engine no longer depends on `br-util-nats-fabric`, `br-util-postgres` or
  `br-util-directory`; it keeps only the frontier `br-rust-common` crates
  (`br-core-auth`, `br-core-integration`) plus `br-util-axum-readiness`.
  `Engine::boot(config, pg, nats, readiness)` now takes a `Nats`, not a fabric.
- The public surface is the intent's authoring surface. `Engine` exposes
  `register_reaction` / `register_mutation` / `register_projector` /
  `register_offer` / `register_mirror` / `register_accumulator` /
  `register_presence` / `register_blobs` / `register_cron` and `declare_scopes`,
  plus the author types `Gate`/`Reason`, `OneShot`, `Disposition`,
  `Persistence`, `Offer`, `Visibility`, `Presence`, `Blobs`, `Erasable`,
  `ScopeManifest`, `BlobPolicy`, `PersonId` and the `Mutation`/`Reaction`/`Bulk`
  contexts. The engine internals (`RenderRegistry`, `SessionRuntime`, `Relay`
  and its `Claim`/`Discipline`/`Drained`, `PgListenNotify`, `ImpactTransport`,
  `RelayRuntime`, the outbox/kv relays, and the `bind_noun` / `register_relay` /
  `transport` / `transport_arc` / `accumulators` / `render` accessors) are
  gated behind the `test-support` feature — the sanctioned battery-only seam,
  with no semver promise — and are private in a normal service build.
- The register-methods a later rework unit fills return the typed
  `EngineError::NotYet` until then; the module map in the README names the unit
  for each.

### Added (0.1.0 rework, unit U2 — inbound loop + poison/dead-letter)

- The engine-owned inbound loop over integration commands and events. One
  shared durable consumer per registered reaction, bound on the gitops-declared
  `INTEGRATION_CMD` / `INTEGRATION_EVT` streams (bind-only, fail-loud, never
  created by the engine). The engine creates or updates its own durable with a
  frozen work-loop contract: explicit ack, deliver-all, instant replay, an
  exact filter subject rendered from the reaction's coordinates, `max_deliver`
  unlimited (the loop enforces the budget in code), `ack_wait` and
  `max_ack_pending` from `InboundConfig`. Every pod binds the same durable name,
  so a message is owned by one pod at a time.
- Ack-after-durable: a message is acked (double-ack) only once its effect has
  committed. A retryable failure `Nak`s with a growing, capped backoff and frees
  the slot; a redelivery after a crash-before-commit finds the idempotency claim
  and acks as a no-op, so the effect lands exactly once.
- `Disposition` (Retry / Park / Terminal) routed against per-reaction budgets:
  a retryable message `Nak`s up to its delivery budget, an early message `Nak`s
  up to its `parking_budget`, and either exhausted budget, or a Terminal
  disposition, dead-letters. `sqlx_is_terminal` classifies a Postgres integrity
  (SQLSTATE class 23) or data (class 22) violation as terminal on the engine's
  own authority, ahead of the handler's disposition.
- The engine dead-letter table (`service_engine.dead_letter`) with a
  `DeadLetters` store: `record` (upsert by reaction + message id, bumping the
  delivery count), `list` by `DeadLetterSource`, `discard`, and `retry` — retry
  re-publishes the stored frame with a fresh dedup token but the original
  logical message id, so it re-enters the pipeline as a fresh delivery while the
  claim and the sequence guard keep a replay on newer state a no-op.
- The per-(producer, key) sequence guard (`service_engine.sequence_guard`) and
  the idempotency claim (`service_engine.message_claim`), applied inside the
  effect transaction: a message whose producer sequence is not above the last
  applied is an acked no-op, so a view never walks backwards, and a
  claim already present for the same (message id, reaction) is an acked no-op.
  The claim is keyed `(message_id, reaction)` — matching the dead-letter table —
  so two reactions of one service that subscribe to the same coordinate each run
  their own effect once; the claim dedupes redeliveries within a reaction, never
  across sibling reactions. Both are engine tables the direct write pipeline
  (a later unit) writes alongside the effect.
- `register_reaction::<M, _, _>(durable, handler)` (and
  `register_reaction_with_budgets`) records a reaction: it validates the durable
  name, derives the subscription from the message type's coordinates, and stores
  the handler behind `ReactionInvoker` for the write pipeline to run.
  `Engine::inbound_subscriptions()` exposes the derived set.
- The narrow loop-to-pipeline contract: the `Dispatch` trait
  (`Applied::{Committed, NoOp}` / `DispatchError` carrying a `Disposition`),
  implemented by the direct write pipeline in a later unit and, behind the
  `test-support` feature, by an in-crate `StubDispatch` that exercises the whole
  loop end-to-end (claim, sequence guard, effect, crash-before-commit, poison
  and parking verdicts) against real Postgres.
- `EngineError::Nats` surfaces a fail-loud stream bind. Five conformance
  scenarios (`s28`–`s32`) cover shared-consumer ownership across two pods
  (observed directly: the two pods dispatch each message exactly once, with zero
  duplicate/stale no-op outcomes, which a regression to per-pod consumers would
  break), ack-after-durable with a crash before commit, poison budget to dead
  letter with the discard gesture, early parking then release with the retry
  gesture, and the sequence guard rejecting a stale producer sequence. The
  scenarios poll for the committed DB state under a bounded timeout rather than
  sleeping a fixed delay.
### Added (0.1.0 rework, unit U3 — direct write pipeline + handler contexts)

- The one direct write pipeline for a GraphQL mutation and a NATS command
  alike: load, gate (the affordance function in deny mode, so the affordance and
  the validation are one method), domain command, `save`, stage impacts, stage
  outbox rows, commit, respond. The transaction runs under `SET LOCAL
  lock_timeout` below the consumer's `ack_wait`; a lock timeout (SQLSTATE class
  55) is retryable and `nak`s. For a NATS message the idempotency claim on the
  message id and the per-(producer, key) sequence guard are inserted inside the
  same transaction as the effect, so a redelivery after a crash-before-commit is
  a claimed no-op and a stale producer sequence is a dropped no-op.
- The real `Dispatch` implementation (`DirectPipeline`) for the U2 inbound loop:
  it looks up the reaction's handler, runs the pipeline, and classifies the
  errors it raises on its own (transaction begin, the idempotency claim, the
  sequence guard, `flush`/commit) with `sqlx_is_terminal` (integrity/data
  violations terminal, everything else retryable). A violation raised inside the
  handler's `cx.save` / `cx.create` and propagated as the handler's own error is
  classified by the engine in U4, ahead of the handler's `Disposition`. The
  engine now **starts the inbound loop at boot**, after the scope handshake, so
  a booted engine with registered reactions consumes over the real pipeline with
  no `test-support` seam; the loop is stopped and joined on shutdown.
- Handler contexts `Reaction`, `Mutation<P>` and `Bulk<P>` over a shared `Ops`
  core. The shared `Ops` methods are `cx.load` / `cx.save` / `cx.create`
  through the `Persistence` trait, `cx.impact` / `cx.impact_caused` /
  `cx.impact_at` (a scheduled impact on the DB clock), `cx.command` / `cx.emit`
  (outbox rows), `cx.seal` (delegates to the accumulator seal), `cx.schedule_at`
  (a scheduled reaction), `cx.now`, and `cx.blob` (a typed `NotYet` until U9).
  `Mutation<P>` adds `cx.principal` and `cx.present`, which delegates to U6's
  presence handle and is put after commit on the loss-tolerant presence lane —
  a put that fails once the state has committed is logged, never turning a
  committed mutation into a failed response. `Bulk<P>` adds `cx.principal` and
  `cx.impact_all(projector)`, which stages one projector-reset impact so every
  pod delivers a `Reset` of that projector's windows — a fixed `Keys` window
  included — rather than one impact per key. Impacts, outbox rows, scheduled
  impacts and scheduled reactions are staged inside the effect transaction; an
  ordinary transaction that dirties more than `impacts_per_commit` keys is
  refused and named the bulk path.
- `register_projector` auto-binds the projector's noun to its key type (the
  `add_projector` semantics), so a service that owns a domain noun with a
  projector assembles it through the public authoring surface alone; binding a
  noun by hand is no longer required and stays a `test-support` seam. A second
  projector on the same noun with a different key type is still a typed
  `NounKeyMismatch`.
- `register_mutation` / `register_bulk` are live, keyed by `MutationInput::NAME`;
  `Engine::mutation_executor()` hands out a cloneable `MutationExecutor` that
  runs a registered mutation or bulk for a resolved principal and returns
  `M::Output` (`()` or a `OneShot`) on the synchronous channel, or a
  `MutationError` carrying the gate's `Reason` code. A `OneShot` value is typed
  so it can only reach the caller, never a view, impact, offer or outbox row.
  `register_reaction` keeps the shape U2 established.
- The `Persistence` trait gains `type Key` / `type Event` and
  `load` / `save` / `create`, plus an `Aggregate` trait naming a `Store`, so
  `cx.load::<A>` / `cx.save(&a)` / `cx.create(&a)` resolve statically with no
  registry. The CRUD style ships (the row is the truth). The pipeline calls
  `load` / `save` and never learns the style; soft-EDA and full-EDA fill the
  same trait in U4. The state row is written in the same transaction as any
  impact, outbox row or claim.
- The engine `service_engine.scheduled_message` table and a beat-paced firing
  loop: a scheduled reaction staged with the write transaction is claimed
  `FOR UPDATE SKIP LOCKED` on the database clock, republished onto its
  coordinate subject, and deleted; a redelivery is deduped by the receiver's
  claim on the stable logical message id.
- Dead-lettering now stages an impact on the ops noun
  (`service_engine_dead_letter`) inside the same transaction as the dead-letter
  row, so an ops view of dead letters updates like any other change.
- `engine/mod.rs` is split by capability into `engine/{mod,register,mutate,run}`
  so no file exceeds the size limit, and the wave-1 minor is fixed: the presence
  watch task is now stopped and joined on the scope-handshake early returns.
- Conformance scenarios `s33`–`s40` against real infra: a mutation gate deny
  (typed error with the affordance's reason) and allow (commit → impact →
  session `Upsert` with the flipped affordance) through one pipeline; a booted
  engine consuming a NATS command through the pipeline exactly once via the
  claim; a `OneShot` secret returned only on the synchronous channel; a row
  lock timeout retried (never dead-lettered) and committed once the lock frees;
  an emitted event staged in the write transaction and published by the leader
  relay; `schedule_at` firing on the DB clock; dead-lettering staging an
  ops-view impact; and a bulk `impact_all` resetting a fixed `Keys` window on
  the session (`s40`), which a per-key impact would have missed.

### Added (0.1.0 rework, unit U4 — soft-EDA and full-EDA persistence styles)

- Soft-EDA and full-EDA fill the `Persistence` trait behind the same one-arg
  `cx.save` / `cx.create` the CRUD style established, so one mutation handler
  runs unchanged over all three styles and the pipeline never learns which one.
  The command's returned events reach `save` through a new default
  `Aggregate::pending_events` method (`&[]` for CRUD, which persists no events):
  `cx.save(&agg)` passes `agg.pending_events()` into `Persistence::save`, so the
  one-arg call is unchanged and the log styles get their events without the
  pipeline threading them.
- Both log styles write the state row (or snapshot) and the events (or facts) in
  the **one transaction the pipeline opened** — a style never opens its own
  transaction. Soft EDA writes the state row then appends one fact per event;
  full EDA appends one event per change then writes the snapshot row, which is
  also the locked state row. A foreign key, unique or check constraint on either
  table rejects the write and its events together: the transaction rolls both
  back, so no state can exist without its event and no crash can land between
  them.
- The reference stores take the row (or snapshot) lock at `load` with
  `SELECT … FOR UPDATE`, so concurrent commands on one aggregate key **serialize
  in every style**: the read-modify-write cannot lose an update, and two log-style
  commands never race to the same event sequence. Locking the row in `load` is an
  author responsibility — the engine cannot inject it into author-owned load SQL
  — so the `counter` sample locks it and the slices copied from it inherit the
  guarantee. The render side stays lock-free: `Projector::load` reads with its own
  plain `SELECT`.
- Full-EDA hydration on `load`: read the snapshot, replay the events above its
  version, then run the aggregate's hydration check as the second barrier, so a
  malformed log fails to load with a typed `EngineError::Service` rather than
  hydrating an illegal state. The full-EDA kit also owns the log's two gestures —
  **upcasting** an older event version to the current shape at read time, and
  **erasure**, which rewrites a person's events in place, anonymising the
  personal fields, and re-snapshots the aggregate from the rewritten log in the
  same transaction (append-only except for erasure; no tombstone, no key table).
- Engine authority over poison, completed for the handler path: an integrity
  (SQLSTATE class 23) or data (class 22) violation raised inside a handler's
  `cx.save` / `cx.create` is classified **terminal by the engine whatever the
  handler's `Disposition` says**, so a coarse `Store(_) => Retry` can no longer
  nak a constraint violation forever. The pipeline records the violation as it
  passes back through `cx.save` / `cx.create` and, when the handler then fails,
  routes the delivery straight to the dead-letter table on its first delivery
  rather than by the handler's disposition. A raw `cx.connection()` write is not
  on this path and stays the handler's to classify.
- The render-side `Projector::load` and the write-side `Persistence` read one
  committed store: a full-EDA slice's snapshot **is** the state row the projector
  reads, written synchronously in the effect transaction, so a `fetch` or a
  session `Upsert` and a write-side `load` return the same committed truth, with
  no asynchronous projection between them.
- Conformance scenarios `s41`–`s46` against real infra, over a `counter` sample
  slice built once and persisted in all three styles: one bump handler unchanged
  across CRUD, soft and full EDA (`s41`); a failing constraint rolling back the
  state row and its events together in soft and full EDA (`s42`); full-EDA replay
  from a lagging snapshot equal to replay from scratch, and the hydration barrier
  refusing a malformed log (`s43`); upcasting a v1 event and an erasure that
  leaves the log readable (`s44`); a class-23 violation in `cx.save` dead-lettered
  on the first delivery under a coarse Retry disposition (`s45`); the render
  side and the write side reading the same committed full-EDA snapshot (`s46`);
  concurrent commands on one key serialising without a lost update in all three
  styles, exercising the `cx.create` open path and the `cx.save` bump path
  (`s47`); and a unique constraint rejecting a concurrent duplicate `cx.create`
  and rolling its appended events back with it (`s48`).

### Added (0.1.0 rework, unit U5)

- Gate/affordance author layer (`gate` module): `Gate`/`Reason` (a `Reason` is a
  stable, serialisable error code), `ActionName`, the `Affordances` map that
  serialises to the client wire (`{ action: { allowed, reason? } }`), the `Gated`
  trait (`ACTIONS`, `gate`, `affordances`), and the `gated!` macro that declares
  each gate once so the projector's affordance pass and the mutation's deny-check
  are the one function — never two implementations. `check_gates_match_affordances`
  lets the battery enumerate a gate set and prove it equals the rendered affordance
  surface; a blocked `Gate::require()` yields the `Reason` a mutation refuses with,
  which is the same code the affordance shows.
- Visibility author layer (`visibility` module): the `Visibility` trait
  (`cohorts(row)` / `memberships(principal)`) with three derivations off the one
  cohort-intersection rule — `visible(row, principal)` (the render/query filter and
  the facts-change removal, threaded through the projector's `project`), and
  `visible_keys` / `window`, which build a session window's key set from candidate
  rows so `populate` derives its membership from the same declaration rather than a
  second hand-authored query. `check_window_matches_visibility` lets the battery
  prove a projector's populated window equals the declaration's visible set,
  reporting a `WindowMismatch` on any drift. One declaration, three enforcement
  points: the query filter, the window membership, and the removal of a row from a
  live session when a principal's facts change.
### Added (0.1.0 rework, unit U6 — presence lane)

- The presence lane (lane B). `register_presence::<Pr>(ttl)` binds one
  `EPHEMERAL_{service}` KV bucket at boot — bind-only, never provisioned — and
  fails loud (readiness DOWN, `run` returns `Err`) when the bucket is absent, has
  no TTL (`max_age` is zero), or has no delete markers on expiry (without them an
  expired key raises no watch event, so a presence `Remove` could never fire).
  The bucket name comes from `EngineConfig::with_service`; each lane's declared
  `ttl` must be at least the bucket's `max_age`, or boot fails loud.
- The `Presence` author trait: a `Noun` (its key type and impact noun), a
  `Value`, a `View`, a `NAME`, and the pure functions `kv_key` / `parse_kv_key`
  (typed key ↔ KV key), `view` (value → view) and `in_window` (which keys a
  session's window covers). Presence values are principal-independent, so every
  session of a presence projector shares one render.
- Delivery over the existing session/render machinery. Every pod watches the
  bucket, folds the latest value per key into an in-process presence view (no
  transaction, no store write), and injects a local `(projector, key)` impact so
  the render pass delivers an `Upsert` on a put and a `Remove` on a TTL-expiry or
  clear, through the same `Reset`/`Upsert`/`Remove` wire and contiguous revision
  as every other lane. A session that attaches after the pod is ready finds the
  current presence in its `Reset` (the bucket is fully read into the store at
  boot). Two pods watching one bucket each deliver to their own sessions.
- The write side is `Engine::present::<Pr>(key, value)` and a cloneable
  `PresenceHandle<P>` (`Engine::presence_handle`) the direct-lane pipeline calls
  as `cx.present`; both put the value into the bound bucket, and every pod hears
  it through its own watch. A one-value-per-key bucket makes last-write-wins and
  loss-tolerance hold by construction.
### Added (0.1.0 rework, unit U10)

- Scope declaration at boot with a readiness gate. `Engine::declare_scopes`
  takes a `ScopeManifest` (the union of the slices' scope-key groups), validates
  it into a `br_core_scope::ScopeDeclaration` eagerly (a malformed key, a
  manifest that spans two services, or an empty manifest is a boot-time error,
  not a wire failure) and stores it. After the mirrors converge, `Engine::run`
  runs the scope-declaration handshake over the engine's own NATS connection and
  holds readiness DOWN until Identity confirms: it subscribes to Identity's two
  confirmation subjects, publishes the `service_scope.declare` command with a
  correlation id, and awaits the correlated `accepted`/`rejected` reply. On
  acceptance boot proceeds and the beat brings the pod UP; on rejection the pod
  stays DOWN with the rejection reason in the readiness payload and the logs,
  and `run` returns `EngineError::Scope` so a scope typo is a failed deploy,
  never a silent deny; while Identity is unreachable the handshake re-publishes
  and waits, readiness DOWN throughout. A scopeless service never calls
  `declare_scopes` and skips the gate entirely. New public surface:
  `ScopeManifest` and `ScopeError`; new `EngineError::Scope` variant. Scope
  declaration is the one frontier the engine reaches across the service
  boundary, so it takes the frozen wire from the `br-rust-common` frontier
  crates `br-core-scope` (the declaration and confirmation DTOs) and
  `br-scope-declaration-contract` (the subject coordinates); the engine renders
  the subjects and drives the handshake itself over its own `async-nats`
  connection, with no `br-util-nats-fabric` dependency.
- Conformance scenario `s28_scope_declaration` against real NATS with a fake
  Identity responder: an accepted declaration brings the pod UP, a rejected one
  keeps it DOWN with the reason, and a service that declares nothing becomes
  ready without ever publishing a declaration.
### Changed (0.1.0 rework, unit U15)

- `EngineConfig` now validates every bound of the intent's config table at
  boot and carries the ones that were missing: `session_max_age` (12h),
  `lock_timeout` (5s), `nats_grace` (10s), `listener_queue_threshold` (0.5),
  `window_capacity`, `impacts_per_commit`, and an optional `service` label.
  `validate` refuses a zero duration or bound, a `listener_queue_threshold`
  outside `(0.0, 1.0]`, a `lease` that does not outlast the `beat`, and a
  `session_max_age` that does not outlast the idle `session_ttl`. `config.rs`
  became `config/{mod,validate}.rs` for the file-size limit.
- A session now lives at most `session_max_age`: it carries an attach instant
  that activity never refreshes, and the housekeeping beat ends it with the
  stream-closing signal once it reaches the bound, so the client reconnects
  with a fresh passport. This is distinct from `session_ttl`.
- The degrade table gains its NATS-grace behaviour: a `NatsHealth` tracker
  keeps the pod UP through an outage shorter than `nats_grace` and takes it
  DOWN with `REASON_NATS_UNREACHABLE` past it, wired into the readiness verdict
  after the listener and before the mirrors.
- Every engine metric is labelled by `service` and `pod`
  (`observe::install_identity` at boot). New metrics: `impacts_committed_total`
  (the notify-budget counter), `impacts_received_total`, `sessions_ended_total`
  (by reason), and `dependency_up` (per degrade-table dependency). Resets carry
  a `reason` label.
- The four shipped alerts (notification queue usage, notify budget per Postgres
  cluster, sustained resets, dead letters present) ship as a `PrometheusRule`
  in `observability/service-engine-alerts.yaml`.

### Added (0.1.0 rework, unit U1 — delivery core)

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
  housekeeping beat and mirror supervision: `boot(config, pg, nats,
  readiness)` (the caller owns the `ReadinessHandle`, so a boot that fails the
  posture or listener probe leaves it DOWN with the reason), the
  fallible `register_*` seams (`register_projector`, `register_rls`,
  `register_principal_resolver`, `register_accumulator`,
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
  (`ignore_missing`) with `grant_engine_access`. The delivery core owns
  `scheduled_impact`, `leader_slot`, `accumulator_chunk`, `accumulator_seal`,
  and `kv_relay_watermark` (the per-key KV publish watermark that survives
  restarts); the inbound loop adds `message_claim`, `sequence_guard` and
  `dead_letter` (unit U2, documented above). Scheduled boundaries are claimed against the database clock
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

### Added (0.1.0 rework, unit U8)

- Mirror author surface. `Mirror::new(name).consume::<C>().keyed_by(f).project(p)`
  builds a typed mirror over the engine's own KV watch and the retained
  shadow/leader supervisor, generic over any `Consumed` published type crossing
  the frontier (a `PREFIX` under `PUBLISHED_LANGUAGE` by default). Each
  consumption watches the shared bucket and drops every key outside its `PREFIX`
  **before** decoding the value as `C` (`KvWatch::next_under`), so a foreign
  published type in the one `PUBLISHED_LANGUAGE` bucket — identity publishes
  users, groups and service accounts side by side — is skipped, never a decode
  error that would flap the mirror. `register_mirror`
  now takes the builder and wires it to the engine's `Nats`, pool and impact
  transport; the raw `MirrorHandle` entry is `register_mirror_handle`, kept behind
  `test-support`. Each `consume::<C>()` keeps a per-pod typed `Shadow<C>` of the
  offer; `keyed_by` names the join keys a `Change` touches; `project` receives a
  `Projection` with typed `shadow::<C>()` access, the transaction connection and
  `impact`/`impact_foreign`/`impact_resource`, and rewrites the `known_*` rows for
  one key. The projection runs through the direct lane: the `known_*` write and
  its impacts commit in one transaction, so a projected row is a write with
  impacts that reaches every pod's sessions. Boot does a full read of every
  consumed prefix and rebuilds the shadows before it reports converged, so a
  pre-filled bucket is caught up before the pod is ready. The rebuild is
  faithful, not upsert-only: an optional `reconcile_keys` hook names the join
  keys the `known_*` store currently holds, and reconcile projects the union of
  those and the keys the bucket offers, so a key retracted while the pod was
  down (a Recreate deploy) is projected once more against the now-absent shadow
  and its `known_*` row and impact are retired — the `known_*` tables are a pure
  function of the offers on every boot. Readiness stays DOWN
  until every consumption converges; a consumed prefix that reads empty holds
  readiness DOWN, names the prefix and keeps the shadows and `known_*` exactly as
  they are (never projecting to empty); a projection panic takes the same
  restart-and-backoff path as an error. The directory roster is the first
  instance, wired in the conformance sample.

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
