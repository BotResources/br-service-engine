# Changelog

All notable changes to `br-service-engine` are documented here. The whole
workspace ships **one version**: every crate inherits `version.workspace = true`,
and a single git tag `v{version}` releases the set. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver.

## 0.1.0 - 2026-09-04

First engine release. `service-engine` ships the reactive personalized delivery
skeleton; `conformance-service-engine` ships its black-box battery.

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
- Conformance scenarios `s41`–`s48` against real infra, over a `counter` sample
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

### Added (0.1.0 rework, unit U7 — offer author surface + leader-gated mirror)

- The `Offer` author trait: a `Row` (the aggregate the service owns), a
  `Published` value, a `NAME` (the offer's relay/watermark identifier and its
  version discriminator), a `PREFIX` (the bucket-key prefix the reconcile sweep
  scans), `key(row)` (the versioned `PUBLISHED_LANGUAGE` key) and
  `publish(row) -> Option<Published>` (the published value, or `None` to
  retract). `Engine::register_offer::<O>()` replaces the `NotYet` stub: it
  registers a leader-discipline offer relay and records a staging closure keyed
  by the aggregate type.
- A saved noun that carries an offer stages the offer's dirty key **in the same
  transaction as the write**. `Ops::save` / `Ops::create` add a row to the new
  `service_engine.offer_dirty` table via `Staged::flush`, so the dirty key
  commits with the state and a rolled-back mutation leaves none behind. The
  dirty key carries the aggregate key (to re-read) and a monotonic
  `offer_dirty_seq` sequence value (the coalescing guard and the publish
  version).
- The pod that holds the offer's leader lease (a `Discipline::Leader` relay on
  the beat) drains the dirty keys: it re-reads the row, calls `publish`, and
  puts or retracts the key on the `PUBLISHED_LANGUAGE` bucket under a per-key
  watermark (`service_engine.kv_relay_watermark`, monotone by the staged
  sequence) and a bucket-revision compare-and-swap that skips the put when the
  bucket value already equals `publish(row)`. On its first drain after
  boot, and then every `EngineConfig::with_offer_reconcile` period (default
  five minutes), it reconciles the bucket against the store — re-putting a row
  whose published value differs from (or is missing from) the bucket, retracting
  orphans under its prefix — so a rebuilt or drifted bucket is repaired, and a
  stable leader that never restarts still repairs out-of-band drift on its next
  periodic pass. The reconcile timer advances only under leadership, so a pod
  reconciles on the first drain after it acquires the lease and periodically
  thereafter. The `PUBLISHED_LANGUAGE` bucket must exist; a missing
  bucket surfaces as a failing relay (readiness DOWN). The offer version lives
  in the key, so a breaking change is a second `register_offer` on the same
  noun.
- The mirror projection into `known_*` is now leader-gated, as the intent
  requires: `Engine::register_mirror` builds the mirror with a `MirrorLeader`
  (`build_led`), so only the pod holding the mirror's lease (`leader_slot` name
  `mirror:{name}`, a fenced lease under the mirror's advisory lock) projects.
  The lease is renewed **on the beat** (a per-mirror beat tick in the watch
  loop), not only when a change is projected, so leadership no longer lapses in
  a quiet period. Standby pods keep their shadows current from the KV watch
  without projecting. When a standby acquires the lease (the leader stopped and
  its lease expired) it **re-projects its already-current shadow** — a local
  reconcile from the in-memory shadow, no bucket re-read — so a put or retract
  delivered inside the failover window (applied to the standby's shadow before
  it could take over) reaches `known_*` on takeover and is never missed. The
  lower-level `MirrorReady::build` stays ungated for the battery's direct-drive
  scenarios.
- The app-role grant now includes `USAGE, SELECT` on the engine schema's
  sequences (for `offer_dirty_seq`).
- `EngineConfig::offer_reconcile` (`with_offer_reconcile`, default five minutes,
  validated non-zero) sets how often the leader re-reconciles an offer bucket
  against the store.
- Conformance: `s49_offer` (a mutation's dirty key commits in the same tx and
  the leader publishes it; a rolled-back mutation leaves no dirty key and
  nothing offered; a noun that stops being offerable is retracted; boot
  reconcile repairs a drifted bucket), `s51_offer_periodic` (a stable leader that
  never restarts repairs out-of-band bucket drift on its periodic reconcile), and
  `s50_mirror_leader` (two pods, exactly one projects into `known_*`, and the
  standby takes over after the leader stops).

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

### Added (0.1.0 rework, unit U9 — blobs / S3-compatible object storage)

- `register_blobs::<Kind>(BlobPolicy)` records a per-kind policy (`max_bytes`,
  `orphan_after`) and `cx.blob::<Kind>(name, content_type)` (plus
  `cx.blob_owned` carrying an owner for erasure) replaces the pipeline's
  `NotYet`: it stages a blob **reference row** — reference, object key, kind,
  content type, file name, owner, size and state — in `service_engine.blob`
  inside the write pipeline's transaction, so the reference commits with the
  referencing aggregate and a rolled-back handler leaves neither. The bytes flow
  client-to-storage directly, so `size` is null at commit and is recorded from
  the object's head when the reaper first sees the upload completed (promoting
  the row to `uploaded`), independent of the orphan window (aligned in U9b). A
  `pending` reference whose object has not landed resolves to no download URL
  (U9b). Orphan detection is at the aggregate boundary in U9b; `cx.release_blob`
  stays as the explicit escape hatch.
- The service's S3-compatible bucket is bound at boot (bind-only, fail-loud via
  a HEAD, never created — `EngineConfig::with_blob_storage(BlobConfig)`); a
  registered blob kind with no configured storage, or an absent bucket, holds
  readiness DOWN and fails `run` loud (`EngineError::BlobBucketAbsent`).
- Upload and download URLs are S3 SigV4 presigned and short-lived. The upload is
  a **presigned POST** (U9b, see below); the download a presigned GET. `UploadUrl`
  (from `cx.blob`) carries the POST endpoint and its signed form fields, and
  `DownloadUrl` (from `Engine::download_url`) does not implement `Serialize`, so
  — mirroring `OneShot` — neither can enter a view, an impact, an offer, an
  outbox row or a chunk; a view carries only the opaque `BlobRef`, and the client
  asks for a URL. `reqwest` (rustls, no default features) is the thin HTTP client
  for the engine's own bucket/object HEAD and DELETE. No cloud SDK.
- A beat reaper, per `BlobPolicy`, deletes an abandoned upload (a `pending`
  reference past `orphan_after` whose object never landed) and an unreferenced
  blob (a reference released past `orphan_after`, whose object it deletes from
  storage), and promotes a completed upload (recording the object's size from its
  head — see U9b for the promotion/reaping split), claiming rows
  `FOR UPDATE SKIP LOCKED` so pods never double-reap; its cadence is
  `EngineConfig::with_blob_reaper_interval` and its per-outcome counts export as
  the `service_engine_blobs_reaped_total` metric. The engine does not
  reference-count slice-owned tables.
- `Engine::purge_person_blobs(person)` is the erase hook U11 calls: it deletes
  every object a person owns and its reference rows. Only `cx.blob_owned` records
  an owner; a blob attached with the un-owned `cx.blob` is not reached by it.
- New engine migration `service_engine.blob` (reserved range) and the eleventh
  engine table; real-infra conformance scenarios (`s52`–`s58`, extended in U9b)
  run against a MinIO the battery spawns per test (a `TestMinio` alongside
  `TestNats`), and the conformance CI job installs `minio`: the reference commits
  with the aggregate (rollback leaves no row), a presigned upload/download
  round-trips real bytes, a presigned URL never appears in the delivered view or
  impact stream, the reaper removes an abandoned upload and an orphan, promotion
  records the object's size, the erase hook purges a person, and an absent bucket
  fails boot loud.

### Changed (0.1.0 rework, unit U9b — blob alignment to the intent)

- **The size cap is enforced at upload, not after the fact.** `cx.blob` now
  returns a `UploadUrl` that is an S3 SigV4 **presigned POST** whose policy
  carries `content-length-range = [0, BlobPolicy::max_bytes]`, so object storage
  refuses an oversize object and it never lands. `UploadUrl` changed shape from a
  single URL string to a POST endpoint plus its signed form fields
  (`UploadUrl::{url, fields, into_parts}`). The POST-policy signer is in-engine
  (`hmac` + `sha2` + `base64`, new deps); the download GET still uses `rusty-s3`.
  The reaper no longer inspects size: its scope is exactly the intent's two
  categories, incomplete uploads and unreferenced blobs. `ReaperRound` drops
  `reaped_oversize` and the `oversize` metric label is gone.
- **A blob's size is recorded at completion, not after the orphan window.** The
  reaper promotes a `pending` reference the moment its object is present (heading
  every `pending` row each sweep, no `orphan_after` gate); only the deletion of a
  never-completed upload is gated by `orphan_after`.
- **A `pending` reference whose object never landed resolves to no download URL.**
  `Engine::download_url` returns `None` for a `pending` row whose object is absent
  (and for an `orphaned` one); a landed-but-not-yet-promoted object still
  resolves via a head.
- **Orphan detection moved to the aggregate boundary — no slice-table scanning.**
  `Aggregate::blob_refs(&self) -> Vec<BlobRef>` (default empty) exposes a row's
  live references; the pipeline diffs them between `load` and `save` and releases
  any dropped or repointed reference in the **same transaction** as the write.
  `cx.delete::<A>(&agg)` releases all of a deleted aggregate's references.
  `cx.release_blob` remains as the explicit escape hatch.
- Conformance: `s58` now asserts an oversize POST is rejected by MinIO and the
  reference row is reaped as incomplete (no oversize reaping); new scenarios
  `s65` (repointing releases the old blob in the same tx), `s66` (deleting the
  aggregate releases), `s67` (a `pending` blob yields no download URL); the
  round-trip/reaper/erase scenarios upload through the presigned POST form.
- The blob boot-bind moved out of `engine/run.rs` into `blobs::bind`, and
  `conformance-service-engine`'s `sample/engine.rs` split its persistence boots
  into `sample/engine_persistence.rs`, keeping every source file under ~300 lines.

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

### Added (0.1.0 rework, unit U12 — GraphQL surface kit)

- The async-graphql kit in the `graphql` module: a service composes its slices'
  root objects (async-graphql `MergedObject` / `MergedSubscription`) into one
  schema with `engine_schema`, and mounts it with `app`, which serves
  `POST /graphql`, the GraphQL-over-WebSocket subscription transport on
  `GET /graphql/ws` (`graphql-transport-ws`), and `/readyz`. `serve` runs the
  router with graceful shutdown. `Engine::graphql_state` hands the router and
  the schema the executor, the render runtime and the pool.
- Mutation resolvers run on `Engine::mutation_executor`: `execute` /
  `execute_bulk` run `MutationExecutor::run` / `run_bulk` and map a
  `MutationError` to a typed GraphQL error carrying the gate's `Reason` code in
  the `code` extension; `ack` / `ack_bulk` answer `{ success }` for a `()`
  output; a `OneShot`'s inner value is returned by the resolver only in the
  mutation response and never enters a view, an impact, an event or an outbox
  row.
- Query resolvers read rendered views through the render kit, never the
  database: `fetch` / `fetch_json` render one key and `fetch_window` /
  `fetch_window_json` a window, reusing the frame's `populate` for visibility
  (a key the principal may not see answers as absent, fail-closed for a
  non-authoritative window) and the same batched load and affordance pass as a
  subscription frame.
- The subscription kit maps the engine's `Reset` / `Upsert` / `Remove` wire to
  the `EngineDelta` GraphQL union (`ResetPayload` / `UpsertPayload` /
  `RemovePayload`) with the contiguous revision exposed and the causing domain
  event riding along as `cause`; `attach` returns the raw `SessionStream` for a
  service that maps to its own per-projector union, `subscribe` the mapped
  `EngineDelta` stream. Each delta carries the projector name so a service
  discriminates one union member per projector.
- Principal resolution is authZ-only: the axum layer decodes the trusted
  `X-Passport` header (`br-core-auth`) and builds the service's `P` through the
  new `PassportPrincipal` trait before calling the executor or attaching a
  session; a missing or malformed passport is rejected with `401` before any
  resolver runs. The kit never authenticates the header's origin.
- Six conformance scenarios over real HTTP against a booted engine
  (`s59`–`s64`): a mutation denied with the affordance's reason code and then
  allowed over one pipeline; an allowed mutation answering `{ success }` while
  the subscription receives `Reset` (revision 1) then `Upsert` (revision 2) for
  the same view; a one-shot secret present in the mutation response and absent
  from every subscription frame and the outbox; a query returning the rendered
  view with its affordances and hiding another tenant's row as absent; an
  unauthenticated and a malformed-passport request rejected before the pipeline;
  and two slices' SDL fragments composing into one valid schema.

### Added (0.1.0 rework, unit U11 — erase a person)

- The `Erasable` author trait (`erase(cx, person) -> Result<Erased, Error>`), the
  `Erase` handler context (an `Ops` wrapper carrying the `PersonId`, so a slice's
  erase uses the same `connection`/`impact`/`impact_caused`/`dirty_offer` surface
  as the write pipeline), the `Erased` post-commit purge manifest
  (`purge_stream` / `purge_presence` / `purge_blob`, and a `rows` count), and
  `Engine::register_erasable`.
- `Engine::eraser()` returns a cloneable `Eraser<P>` handle (captured before
  `run` consumes the engine, like `mutation_executor` and `blob_reader`) and
  `Engine::erase(person)` delegates to it. The gesture opens **one** direct-lane
  transaction under `lock_timeout`, records the durable erasure fact in the new
  `service_engine.person_erasure` table (insert-or-ignore, so the first erase is
  `fresh`), runs every registered slice's `erase` over all three persistence
  styles (full EDA reuses the slice's own event-rewrite/re-snapshot), stages the
  `PersonErased` integration event through the outbox in that same transaction on
  the fresh erase only, and commits — a failing slice rolls the whole gesture
  back. After the commit it purges the manifest's accumulated-lane stream rows
  (`service_engine.accumulator_chunk` / `accumulator_seal`), the presence keys
  (KV retract) and the released blob references, and calls `purge_person_blobs`
  for the person's owned blobs. Idempotent: a second call finds nothing to erase,
  does not re-stage `PersonErased`, and returns the same outcome. This replaces
  the last `EngineError::NotYet`; no engine gesture returns it any more.
- `PersonErased` is emitted as `integration.evt.{service}.person.erased.v1` with
  a `{ person_id }` payload and a deterministic event id, so the receiver's claim
  deduplicates a redelivery. Other services erase their own rows on the event;
  `known_*` mirrors and shadows follow the producer's offer retract, never the
  event.
- `PresenceHandle::purge`, `BlobReader::purge_references` and
  `BlobStore::purge_references` back the post-commit purges.
- `conformance-service-engine`: the `erase` sample module (three personal slices
  — CRUD `sample_erase_note` with an offer, soft `sample_erase_memo`, full
  `sample_erase_ledger` — plus a `FailingEraser`) and scenarios `s70`–`s73`:
  erase removes state across the three styles in one transaction and is
  idempotent (`s70`, also proving the live session's `Remove`, the offer retract,
  the stream purge and `PersonErased` staged exactly once), a failing slice rolls
  the whole gesture back (`s71`), the person's presence keys are purged (`s72`),
  and the person's blobs leave storage and the reference table (`s73`).

### Changed (0.1.0 rework, unit U11)

- `housekeeping/ready.rs` (317 lines) is split by capability into
  `housekeeping/ready/mod.rs` (the `ReadinessAssembly` builder and its NATS
  probe) and `housekeeping/ready/verdict.rs` (the pure readiness `verdict` and
  its reason constants, with the readiness tests), each under the file-size
  limit.

### Changed (0.1.0 rework, unit U12b — GraphQL kit aligned to the intent's authoring ergonomics)

- Boot ergonomics: `Engine::run_with(app)` and `Engine::run_with_listener(listener, app)`
  own both the engine run loop and the HTTP server lifecycle in one call and shut
  both down gracefully on the engine's shutdown signal; `run_with` binds the new
  `EngineConfig::http_addr` (default `0.0.0.0:8080`, set with `with_http_addr`),
  `run_with_listener` takes a pre-bound listener. `Engine::run` and `serve` stay
  for callers that drive the two lifecycles themselves.
- Query surface: a typed `Query<'_, P>` context, built from the async-graphql
  `Context` with `Query::new`, replaces the free `fetch` / `fetch_json` /
  `fetch_window` / `fetch_window_json` functions as the authoring surface.
  `cx.fetch::<Projector>(key)` and `cx.fetch_window::<Projector>(params)` return
  the projector's typed `View` (with `fetch_json` variants kept as the untyped
  escape hatch), reusing the same `populate` visibility and batched render as a
  subscription frame. The `Projector::View` no longer needs a hand-written
  `DeserializeOwned` shim to be fetched typed: `Affordances`, `Gate`, `Reason`
  and `ActionName` now `Deserialize` (a bounded interner keeps `Reason` /
  `ActionName` as `&'static str`), so a view carrying affordances round-trips
  through the rendered store, and `Affordances` is an async-graphql scalar so an
  affordance-carrying view is a valid GraphQL output type.
- Subscription surface: the `subscription_union!` macro takes a service's
  `Projector => View` mapping once and emits a typed subscription: a `Union` of
  one member per projector (each the projector's own `View` type, not a
  projector-name string over opaque JSON) inside the `Reset` / `Upsert` /
  `Remove` payloads and the top `EngineDelta` union, with the contiguous revision
  and the causing event as `cause`. `attach` stays as the engine primitive the
  generated `from_delta` mapping consumes; the free `subscribe` / `to_engine_delta`
  and the engine-owned `EngineDelta` / `ProjectedView` / `*Payload` types are
  removed in favour of the per-service typed union the macro emits.
- SDL assembly, wired into boot: a slice declares its root fields and types as a
  `SliceFragment` and registers it with `Engine::register_schema_slice`; the
  engine assembles the registered fragments (`SchemaSlices::assemble`) at the
  start of `run` — so through `run_with` too — and fails boot loud with
  `EngineError::DuplicateSchemaMember`, naming both slices, when two claim the
  same root field or GraphQL type, before the pod serves. `SchemaSlices` /
  `SliceFragment` stay public for a service that wants to assemble ahead of boot.
- Conformance: `s59`–`s64` now drive the typed `Query` context and boot the
  sample through `Engine::run_with` binding `EngineConfig::http_addr`, over the
  per-projector typed union (`... on WidgetView { .. }` in place of a
  `ProjectedView { projector view }` shape). `s68` proves two projectors are two
  typed union members and that a client subscribing to one receives only its own
  member's deltas; `s69` boots a real two-slice engine (real Postgres and NATS)
  whose slices claim the same root field (and, in a second scenario, the same
  GraphQL type) and asserts the boot returns `EngineError::DuplicateSchemaMember`
  instead of standing the pod up.

### Changed (0.1.0 rework, wave-3 integration)

- Query-time RLS: `Query::fetch` / `fetch_window` (and their `_json` variants)
  no longer hard-code the render's RLS flag to `false`; a service with a
  registered `RlsApplier` now runs typed query-time fetch under the same RLS the
  subscription render applies, so a key a principal may not see answers as absent
  with RLS actually engaged in the DB session — not only when `populate`/`project`
  happen to filter it. New real-infra scenario `s74` boots a service whose
  RLS-backed projector populates permissively and projects with no tenant filter,
  so the forbidden row is hidden only by the query-time RLS session context.
- Schema-derived boot gate: the slice-assembly gate now derives the composed
  schema's root fields from the built async-graphql SDL
  (`SchemaSlices::verify_root_fields`) and fails boot with the new
  `EngineError::UndeclaredSchemaMember` when the schema exposes a root field no
  slice fragment declared, so an under-declared fragment can no longer leave a
  real root field outside the declared-collision gate. A service feeds the SDL
  with `Engine::set_schema_sdl(schema.sdl())` before `run`. New scenario `s75`
  proves an undeclared root field fails the boot loud. (GraphQL object *types*
  keep the declared-vs-declared gate: the engine injects many payload, union and
  scalar types no slice owns, so the schema's type set is not a slice-only set.)
- `PresenceHandle::present` returns the precise `EngineError::PresenceNotRegistered`
  (naming the presence type) instead of the placeholder `EngineError::NotYet`;
  the `NotYet` variant is removed entirely, so no engine gesture returns it.
- `engine/run.rs` is split by capability: the HTTP-server lifecycle wrappers
  `run_with` / `run_with_listener` move to `engine/serve.rs`, keeping each file
  under the size limit.
- The type-erasure (dyn-compat) wrappers move out of `erase` into their own
  `dyn_compat` module (`ErasedProjector`/`ProjectorAdapter`/`erase_projector`,
  `ErasedAccumulator`/`AccumulatorAdapter`/`erase_accumulator`, `ErasedPopulation`
  /`ErasedInverse`/`ErasedLoadScope`/`ErasedWindowQuery`, `ErasedState`,
  `ErasedFacts`), so `erase` means person-erasure only.
- README: the blobs section now states that a blob reference dropped by raw SQL —
  bypassing `cx.delete`, a `load`+`save` or `cx.release_blob` — is never observed
  by the pipeline diff and so is never reaped.
- Conformance scenario numbers made unique and contiguous after the parallel
  wave-3 merges: blob `s65`–`s67`, GraphQL `s68`–`s69`, erase `s70`–`s73`,
  query-RLS `s74`, schema-derived gate `s75`. The `UploadUrl` the erase sample's
  blob mutation returns is the U9b presigned-POST `{ url, fields }` on the
  synchronous channel, uploaded via the shared `post_upload` helper.

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
