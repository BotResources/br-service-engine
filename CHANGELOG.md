# Changelog

All notable changes to `br-service-engine` are documented here. The whole
workspace ships **one version**: every crate inherits `version.workspace = true`,
and a single git tag `v{version}` releases the set. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver.

## Unreleased

### Added

- **Post-save policies (declared subjection).** `Engine::register_post_save_policy::<A>(f)`
  registers a policy the engine runs after every `save`/`create` of aggregate `A`,
  inside the transaction and before the commit. The policy (`Fn(&A, &mut PostSave)
  -> Result<(), Refused>`) is pure domain logic over the just-saved aggregate: it
  stages impacts/commands/events through `PostSave`, or `PostSave::refuse(reason)`
  to roll the write back and answer the mutation with that `Reason` code (a
  refusing reaction is dead-lettered with it). It cannot save, so it cannot
  recurse. This generalises the cross-slice interlocks whose call sites the 0.1
  rewrite left unwired — the two known uses are the Services breach interlock and
  the Runners reconcile-journal impact. New public exports: `service_engine::{PostSave,
  Refused}`; new `EngineError::PolicyRefused { code }` (non-exhaustive enum, no
  adopter break).
- **Seam completeness at registration.** `Engine::require_post_save_policy::<A>()`
  declares aggregate `A` *subject* to a post-save policy; `Engine::run` fails at
  boot with `EngineError::UnhonouredSeam { aggregate }` unless some slice
  registered one — the same registration gate the schema type check applies, so a
  missing interlock is a loud boot error, not a silent absent call. New
  `EngineError::UnhonouredSeam` variant (non-exhaustive enum, no adopter break).
- **`Ops::load_many::<A>(&keys)`** loads several aggregates of one noun in one
  pipeline transaction, each locked for the transaction, so a service mutates
  several aggregates atomically **without a global advisory lock of its own**. The
  keys are deduplicated and locked in ascending order of their encoded bytes; that
  ascending `(store type, encoded key)` order is the engine's documented global
  aggregate-lock discipline (call `load`/`load_many` in it to hold different nouns
  in one transaction), and it makes two concurrent multi-aggregate writes
  deadlock-free. Absent keys are omitted, as for a batched read. Additive — no
  existing signature changed.
- `Reason::parse(&'static str) -> Result<Reason, ReasonFormat>` — the fallible
  sibling of `Reason::new` for a `'static` code whose shape is only known at
  runtime, and `service_engine::{ReasonFormat, is_reason_code}`. A code arriving
  over the wire is validated and interned by `Reason`'s `Deserialize`, not through
  `parse`.
- `Reaction::message_id()` exposes the stable inbound message identity to handlers,
  enabling domain deduplication that outlives the engine's delivery-claim retention.
- Mirrors persist a per-bucket **stream identity** beside the watermark and commit
  both with the projections in one transaction. The boundary is the last sequence
  the read captured, taken from fresh `get_info()` metadata before the first scan;
  every scanned entry is applied, the watch resumes at `S + 1`, and a genuinely
  zero boundary rescans before it opens a future-only `watch_all()`. A standby
  reports converged once the leader has committed the boundary the standby's own
  boot read captured, with no broker round-trip per beat. Migration
  `9113000023_mirror_stream_identity.sql` adds the nullable column; existing
  watermark rows keep their revision and adopt an identity on their next read.

### Changed

- **Reason codes are now `SCREAMING_SNAKE_CASE`.** `Reason::new` validates its
  argument against `^[A-Z][A-Z0-9_]+$` (a leading capital, then one or more
  capitals, digits or underscores); a mistyped literal is a compile error at the
  `const` site, and a code decoded from the wire that fails the shape is rejected
  at deserialization rather than trusted. This aligns the engine with the frozen
  `br-test-harness` `verdict::expect_code_shaped`, which already demands this
  shape, and with a consumer that assumes the casing. All in-tree codes were
  migrated (e.g. `already_closed` → `ALREADY_CLOSED`).
  **Migration for adopters:** rename every reason-code literal to
  `SCREAMING_SNAKE_CASE` — a lower-case literal that compiled under 0.1 is now a
  compile-time panic in `Reason::new`. Any client, test or fixture that matched a
  reason code as a string (for example asserting `"already_closed"`) must match
  the upper-case code. The wire encoding is unchanged; only the accepted alphabet
  narrowed.
- **A mirror no longer judges the producer's content.** Converged means the bucket
  is bound and one full read completed with the watch attached at the revision that
  read reached — nothing more. A consumed prefix that reads empty is a converged
  prefix with nothing in it: `known_*` follows the source and is projected to
  empty, at boot or during a run. Only a read that fails or does not complete
  projects nothing. This withdraws the 0.1.0 rule that an empty consumed prefix
  holds readiness DOWN, which made the first deploy of every consumer fail, since
  every producer's bucket is empty until it publishes.
- A consumed bucket whose stream identity changed, or whose sequence is below the
  held watermark, is the **first-adoption** case: full read, reconcile, adopt the
  new identity and boundary. It never holds readiness DOWN and never asks an
  operator to run SQL. The engine also no longer validates the producer's bucket
  history (`max_messages_per_subject`); it keeps the latest value per key whatever
  the bucket retains.
- Reconciling the persisted projection keys against the snapshot is now the one
  behaviour of every scan, with no opt-in. On a watch event the engine keys the
  change against the shadows both before and after it is applied, so a retract
  whose projection key lived only in the retracted payload still reaches `known_*`.

### Fixed

- No NATS round-trip inside the mirror's leader transaction, and no `STREAM.INFO`
  per beat while a standby waits: a slow broker no longer pins the advisory lock
  and the `leader_slot` row, nor polls the broker once per beat per bucket.
- `/readyz` names the mirror and its own failure after the fixed operator copy,
  read from one sample of the health board rather than two.
- A mirror resuming a bucket from sequence zero no longer owns a window it cannot
  see: the watch it opens there is future-only, so the engine re-reads the
  bucket's metadata after the subscription exists and reconciles when the bucket
  gained a sequence. Zero is the boundary of every consumer's first boot, so a
  producer's first key reached `known_*` only at the next periodic reconcile.
- A standby no longer stalls a rolling upgrade on a leader watermark that carries
  no identity: a row written before the identity column is compared by its
  sequence alone — the semantics it was written with — and takes an identity when
  this pod holds the lease.
- A leader holding a stream identity the standby never read is an adoption, not an
  `EngineError::Config`: the standby re-reads once and waits, instead of failing
  into the supervisor's restart counter, backoff, `Restarting` on `/readyz` and
  dead letter.
- A single mirror watch that ends is logged and reopens the watches, instead of
  being dropped silently by `select_all` and leaving that bucket unwatched until
  the next periodic reconcile.
- A shadow keeps the revision it holds and refuses an older one, so the overlap
  between the scan and the watch can never write a stale value over a newer one.
  The new `Shadows::put_at`/`remove_at` carry that revision guard (`put`/`remove`
  keep their 0.1.0 unconditional signatures for compatibility), and
  `KvBucket::entries_with_revisions` reads the revision per key.

## 0.1.0 - 2026-09-11

This is the first functional engine release: `service-engine` ships
the reactive personalized delivery and process skeleton, and
`conformance-service-engine` its conformance battery in two modes — in-crate
against the real engine through a sample service, and black-box against the real
`example-service` binary. `example-service` (with `example-contract` and
`example-twin`) is the in-repo reference service the battery and the functional
spec run against. The sections below describe the shipped 0.1.0 surface; where a
choice is not obvious the rationale is stated.

### Added

**Engine facade and multi-pod delivery core.** `Engine<P>` composes the render
runtime, transport, accumulators, housekeeping beat and mirror supervision:
`boot(config, pg, nats, readiness)` (the caller owns the `ReadinessHandle`, so a
boot that fails the posture or listener probe leaves it DOWN with the reason),
the fallible `register_*` seams (each returns `Result` and rejects a duplicate
name with a typed error, since a silent duplicate would hide a degraded
component's health), `readiness`, `attach`, `push_chunk`, `seal`, `run` /
`run_with` / `run_with_listener`. `run` supervises its render, beat, flush and
mirror workers: if one ends or panics before shutdown it flips readiness DOWN
(the dead worker in `EngineError::WorkerStopped`) and returns `Err`, never
serving readiness over a dead loop; a mirror step that panics self-heals through
the same restart-and-backoff path as an error. `attach` after shutdown began
returns `AttachError::ShuttingDown`.

- `RenderRegistry` / `SessionRuntime`: the connect barrier, the snapshot, the
  `Reset` / `Upsert` / `Remove` wire over a contiguous per-session `Revision`,
  the coalescing render pass, `PassReport` and GC. `Population::{Keys, Ordered,
  Query}` with `Interest` routing; a `Query` built with `with_keys` is
  authoritative (its membership becomes exactly `populate`'s result plus the keys
  discovered this pass, so a key that leaves the result is `Remove`d and
  per-session membership stays bounded), a `Query` without `with_keys` is
  discovery-only. Per-session fault isolation: a failed render or repair is
  retried and the session ended after `EngineConfig::repair_attempts` failures,
  never served a `Reset` rebuilt from its stale last-sent view; a session left
  `repair_pending` after a failed reconnect resnapshot is retried by the beat.
  Cohort keys are collision-free (an RLS render group keyed on the exact
  `PrincipalId`, a declared cohort on the exact bytes of its parts, never a
  64-bit hash), so two principals can never share one RLS render.
- Impact transport over PostgreSQL `LISTEN`/`NOTIFY` (`PgListenNotify`):
  `stage_in` in the caller's transaction, `schedule_in` / `fire_due` for
  scheduled boundaries, a framed-group payload split that admits a frame only
  strictly below Postgres's 8000-byte `NOTIFY` limit and reassembles the whole,
  a self-repairing `listen()` stream surfacing every loss of continuity as
  `Reconnected` (the loss detector that resets a pod's sessions), and
  `queue_usage()`. The listening connection is drained by its own task into a
  bounded in-process channel (`listener_channel_capacity`) that the render loop
  consumes, so a slow render never stalls the drain; a channel overflow drops the
  impacts and signals one `Reconnected`, re-snapshotting every session. Past
  `listener_queue_threshold` the beat closes the listener through `set_brake`,
  which takes the pod DOWN, reconnects and resets its sessions.
  `service_engine_impacts_committed_total` counts impacts of committed
  transactions only, recorded after commit.
- Boot posture assertion (`assert_posture`: no superuser, no `rolbypassrls`, no
  ownership or membership of the engine schema/database) and the boot listener
  probe that holds readiness DOWN behind a transaction-mode pooler.
- Schema-version singleton (`service_engine.schema_version`, one row): boot claims
  it after the posture check under a per-service advisory lock, writing the engine
  version and the configured service version. A pod that finds a **different**
  version whose row is still live (heartbeat within `schema_version_liveness`,
  default 30s, refreshed by the beat) refuses to go UP — `Engine::boot` returns
  `EngineError::SchemaVersionConflict` and readiness stays DOWN with both versions
  in the reason — so a rolling deploy configured by mistake fails loud instead of
  two versions sharing one store; a stale row (heartbeat lapsed) never blocks. The
  service version is `EngineConfig::with_service_version`. A running pod whose beat
  heartbeat updates no row has been displaced by another version that claimed the
  singleton, and lowers readiness to DOWN until it owns the row again, rather than
  serving over a store it no longer owns.
- `Timestamp`, a newtype truncated to microseconds at construction, so every
  instant crossing the PostgreSQL `timestamptz` boundary round-trips equal.

**Inbound NATS loop, poison and dead-letter.** One shared durable consumer per
registered reaction, bound on the gitops-declared `INTEGRATION_CMD` /
`INTEGRATION_EVT` streams (bind-only, fail-loud, never created); the engine
creates or updates its own durable with a frozen work-loop contract (explicit
ack, deliver-all, instant replay, an exact filter subject from the reaction's
coordinates, `max_deliver` unlimited with the budget enforced in code,
`ack_wait` / `max_ack_pending` from `InboundConfig`). Every pod binds the same
durable name, so a message is owned by one pod at a time.

- Ack-after-durable: a message is acked only once its effect committed; a
  retryable failure `Nak`s with a growing capped backoff and frees the slot; a
  redelivery after a crash-before-commit finds the idempotency claim and acks as
  a no-op, so the effect lands exactly once.
- A poison message is never lost: routing to the dead letter terminates the frame
  only once the dead-letter row is durably written. If that write fails (Postgres
  unreachable during a CNPG switchover), the frame is nak'ed with the capped
  backoff, not terminated, so the broker keeps redelivering it (`max_deliver` is
  unlimited) until the table can record it — then it is terminated once. The same
  holds for a frame that cannot be identified at all.
- A reaction handler that panics is caught at dispatch, rolled back and routed to
  the dead-letter table as a terminal frame, so the consumer keeps serving instead
  of redelivering the panicking frame every `ack_wait` forever with readiness UP.
  `cx.principal` returns a typed `PrincipalUnresolved` (terminal disposition) when
  no sender principal has been resolved, rather than panicking — a handler
  propagates it like any other reaction error, and `cx.try_principal` is the
  fallible accessor a reaction that needs no sender uses instead.
- `Disposition` (Retry / Park / Terminal) routed against per-reaction budgets;
  `sqlx_is_terminal` classifies a Postgres integrity (SQLSTATE class 23) or data
  (class 22) violation as terminal on the engine's own authority, ahead of the
  handler's disposition, so a coarse `Store(_) => Retry` cannot nak a constraint
  violation forever.
- The `service_engine.dead_letter` table and `DeadLetters` store (`record`,
  `record_work`, `list` by `DeadLetterSource`, `discard`, `retry` — retry
  re-publishes with a fresh dedup token but the original logical id, so the claim
  and sequence guard keep a replay on newer state a no-op). It is the one table
  for **every source of work**: inbound reactions, scheduled messages, cron ticks,
  a mirror stuck past its restart threshold and an outbox row that keeps failing
  to publish against a reachable broker all record there
  (`DeadLetterSource::{Reaction, Scheduled, Cron, Mirror, Outbox}`), each staging an
  impact on the ops noun in the same transaction and incrementing the
  `service_engine_dead_letters_total` counter labelled by source.
- The per-(producer, reaction, key) sequence guard
  (`service_engine.sequence_guard`) and the idempotency claim
  (`service_engine.message_claim`), applied inside the effect transaction; the
  claim is keyed `(message_id, reaction)`, and the guard is scoped per reaction,
  so two reactions consuming different facts of one producer under one key keep
  independent watermarks and never drop each other's messages. The confirmation
  the reaction emitted is stored on the claim row (`message_claim.confirmations`)
  in the same transaction as the effect; a later command carrying an
  already-claimed id re-emits that stored confirmation through the outbox instead
  of acking a silent no-op, so a producer that lost its confirmation past the
  broker's duplicate window is answered again without re-running the effect — the
  aggregate does nothing, the engine replays. A command that reuses an
  aggregate id under a *fresh* message id is a genuine duplicate the reaction
  decides on (re-emit its confirmation, or a typed rejection), never a
  dead-letter for a legitimate replay.
- Inbound-consumer and lane-A ingress supervision: each runs under a supervisor
  that restarts it with bounded backoff (one log per step, never a hot spin) and,
  past a consecutive-failure threshold, lowers readiness (`REASON_INBOUND_STOPPED`,
  a `service_engine_dependency_up{dependency="inbound"}` gauge) so a dead consumer
  never leaves the pod deaf while it reports ready; on shutdown a consumer naks its
  pulled-but-unprocessed frames so they redeliver to a live pod.
- Scheduled messages carry a per-row `attempts` count and publish isolated from
  the rest of their batch, so one message that cannot publish against a reachable
  broker is dead-lettered past its delivery budget while the rest of the batch and
  the next beat still fire, rather than one poison row rolling back and re-blocking
  the queue forever. It claims one row per transaction and publishes through the
  same ack-timeout seam as the outbox. A failure observed while `Nats::reachable()`
  is false — or an `Unanswered` publish against a connected broker whose JetStream
  cannot answer — costs no attempt (the beat skips `fire_due` under an outage, the
  same broker-state gate the outbox relay reads), so a scheduled row waits out a
  broker outage or an unavailable-JetStream window without spending its budget and
  fires on return.

**Direct write pipeline and handler contexts.** A GraphQL mutation and a NATS
command run **one** pipeline: load, gate (the affordance function in deny mode,
so the affordance and the validation are one method), domain command, `save`,
stage impacts, stage outbox rows, commit, respond — under `SET LOCAL
lock_timeout` below the consumer's `ack_wait` (a lock timeout is retryable and
`nak`s). Handler contexts `Reaction`, `Mutation<P>` and `Bulk<P>` over a shared
`Ops`: `cx.load` / `cx.save` / `cx.create`, `cx.impact` / `cx.impact_caused` /
`cx.impact_at`, `cx.command` / `cx.emit`, `cx.seal` / `cx.seal_partial` /
`cx.seal_current`, `cx.schedule_at`, `cx.now`, `cx.blob` / `cx.blob_owned` /
`cx.release_blob` / `cx.delete`. `Reaction` adds `cx.delivered` (the JetStream
delivery count of the frame in hand, so a reaction can bound its own retries and
answer the producer once a failure is permanent rather than nak toward a silent
dead letter). `Mutation<P>` adds `cx.principal` and
`cx.present` (put after commit on the loss-tolerant presence lane); `Bulk<P>`
adds `cx.impact_all` (one projector-reset impact rather than one per key). An
ordinary transaction that dirties more than `impacts_per_commit` keys is refused
and named the bulk path. The synchronous channel answers `{ success }`, a typed
`MutationError` carrying the gate's `Reason` code, or a typed `OneShot` secret
that can only reach the caller — never a view, impact, offer, event or outbox row.
`register_projector` auto-binds the projector's noun to its key type.

**Frontier envelope, producer sequence and reaction principal.** Everything the
engine puts on the integration bus is wrapped in the `br-core-integration`
envelope. `cx.emit` / `cx.command` build an `IntegrationEvent<T>` /
`IntegrationCommand<T>` (`event_id`/`command_id`, a `{aggregate}.{fact|verb}`
type, the coordinate version, `occurred_at`, and `EventMetadata { actor,
correlation_id, causation_id }`); `cx.schedule_at`, the dead-letter republish and
the scope-declaration handshake carry the same shape. The actor is the acting
principal's passport actor for a mutation and the engine's own v5 service identity
for a reaction, cron or erasure; the correlation and causation ids propagate from
the inbound message that caused the effect (its envelope id is the causation).
`OutboundEvent` / `OutboundCommand` carry `sequence()`, which an author overrides
to **declare** a producer sequence — the default is `None`, and the convention
when it is declared is `(producer = service, seq_key = aggregate key, seq =
aggregate version)`. When a sequence is declared the engine persists it on the
outbox row (`integration_outbox.producer` / `seq_key` / `seq`) and renders the
three `Br-Producer` / `Br-Seq-Key` / `Br-Seq` headers on publish, so engine→engine
traffic is ordered and the per-`(producer, reaction, seq_key)` sequence guard is
reachable. A declared sequence with no configured service (`producer`) is refused
with a configuration error at emit and recorded as a terminal violation, so the
frame is dead-lettered rather than silently dropped or redelivered forever, and a
sequence a consumer would order or dedup on can never vanish.
The inbound loop decodes the envelope, dedups on the `Br-Message-Id`/envelope id
(tolerating a non-uuid `Nats-Msg-Id` — a foreign fabric producer no longer
dead-letters), hands the reaction the inner payload, and exposes the sender's
metadata on `Reaction` (`cx.metadata`, `cx.actor`). `Reaction` gained
`cx.principal` / `cx.try_principal`: the sender's `EventMetadata.actor` is resolved
into the service's own `Principal` through a resolver registered with
`Engine::register_reaction_principal`, so a reaction can gate on who sent the
command (the reference `create_card` refuses a command whose actor is not a
service). The `br-core-integration` envelope is **mandatory on the frontier**: a
frame that carries none is refused at `Incoming::identify` as terminal — one
dead-letter row (`DeadLetterSource::Reaction`) and a `Term` — so no frame reaches
a reaction without a sender, and there is no bare-body path.

**Persistence — CRUD, soft EDA, full EDA behind one trait.** `Persistence`
(`type Aggregate` / `type Key` / `type Event`, `const STYLE`, `load` / `save` /
`create` / `read_many` / `lock`) plus an `Aggregate` trait naming a `Store`, so
`cx.load::<A>` / `cx.save(&a)` / `cx.create(&a)` resolve statically with no
registry, and the pipeline never learns the style. The command's events reach
`save` through the default `Aggregate::pending_events` (`&[]` for CRUD). A style
writes the state row (or snapshot) and its events (or facts) in the one
transaction the pipeline opened, so a foreign-key, unique or check-constraint
failure rolls the state row and its events back together — those domain
constraints live on a CRUD or soft-EDA slice's own state table; the full-EDA
kit's generic jsonb `event_snapshot` (keyed by noun) carries only the structural
`(noun, key)` primary key, so a full-EDA slice enforces uniqueness and integrity
in the aggregate's write-time gate and hydration barrier, not in a declared
FK/unique/check. Full EDA is not
hand-rolled per slice: the `full_eda` kit ships the log. A slice declares an
`EventSourced` aggregate (`NOUN`, `EVENT_VERSION`, a `SNAPSHOT_EVERY` cadence,
`to_snapshot`/`from_snapshot`, `genesis`, `apply`, `check_hydrated`, `upcast`) and sets
`type Store = FullEda<Self>`; the kit owns the engine's generic `event_log` and
`event_snapshot` tables (keyed by noun, in the reserved migration range), the
append with per-key seq arithmetic, the snapshot cadence (rewritten only when a
`SNAPSHOT_EVERY` boundary is crossed, so the snapshot lags the log), the replay
from the snapshot with the aggregate's hydration check as the second barrier, and
the log's two gestures — upcasting an older event version at read time, and
`full_eda::erase` (rewrite the person's events in place through a slice-supplied
redactor, then re-snapshot each touched aggregate by replaying the whole
rewritten log from `genesis` in the same transaction, so a person folded into a
snapshot past a crossed cadence boundary leaves no residue). `full_eda::keys` lists a noun's keys for a window populate.
`load`
and `read_many` are both non-locking; the write pipeline serialises concurrent
commands on one key itself by taking a transaction advisory lock in `load`
(`pg_advisory_xact_lock` over a domain-tagged, separator-safe FNV-1a hash of the
aggregate's store type and JSON-encoded key) before calling `Persistence::lock`,
inside the transaction under `lock_timeout`, so concurrent commands on one key
serialize in every style even when `lock` is the default no-op; the reference
stores' `SELECT … FOR UPDATE` before `load` stays as an optimisation that pins
the row image. The render side never takes a row lock. The engine loads a projector's rows
through `Persistence::read_many`; a CRUD or soft-EDA store overrides it with a
single batched read of the same committed table `load`/`save` use, and a
full-EDA store keeps the default (a `load` per key) so the render side replays
the same snapshot and log the write side does — the render-side load and the
write-side load are one truth in every style.

**Gate/affordance and visibility author layers.** `Gate` / `Reason` (a stable
serialisable error code), `ActionName`, the `Affordances` map that serialises to
the client wire, the `Gated` trait and the `gated!` macro that declares each gate
once — so the projector's affordance pass and the mutation's deny-check are the
one function, never two implementations — with `check_gates_match_affordances`
for the battery. `Visibility` (`cohorts(row)` / `memberships(principal)`) with
one cohort-intersection rule deriving three enforcement points from one
declaration — the query/render filter, the session-window membership, and the
removal of a row when a principal's facts change — with
`check_window_matches_visibility`.

**Presence lane (lane B).** `register_presence::<Pr>(ttl)` binds one
`EPHEMERAL_{service}` KV bucket at boot (bind-only, fail-loud when absent, with
no TTL, without delete markers on expiry — without them an expired key raises no
watch event — or when the bucket refuses per-message TTL). Each write carries its
lane's `ttl` as a per-key `Nats-TTL` header (NATS ≥ 2.11), so lanes with
different lifetimes share one bucket and each key expires at its own `ttl`; the
bucket's `max_age` must therefore be at least the longest lane's `ttl`, a global
ceiling that never truncates a lane below its declared lifetime. Every pod
watches the bucket, folds the latest value per key with no store write, and
delivers `Upsert` on a put and `Remove` on TTL-expiry or clear through the same
session/render machinery. On a NATS reconnection the watch (which async-nats
silently resumes from new, losing the gap) is detected by the connection state:
every pod reseeds from the bucket and pushes a `Reset` per presence projector, so
a key that expired or changed while NATS was down is repaired rather than served
forever. Written with `Engine::present` / `PresenceHandle<P>` (`cx.present`).
Last-write-wins and loss-tolerance hold by construction.

**Accumulated lane (lane A) and seal.** Streaming accumulators keyed by the
source's own `ChunkSeq` (a checked newtype bounded to the `bigint` range; an
over-range value is refused at construction, never wrapped and treated as a gap):
per-chunk `Durable` receipts, fold-stops-at-a-gap, a table-verified fold cache
bounded by `EngineConfig::fold_cache_capacity` (LRU). A chunk resubmitted at an
already-durable sequence with identical content is idempotent; different content
is a typed `EngineError::ChunkConflict`. `Ops::seal::<A>(key, last_seq, hash)`
replays the stream up to `last_seq`, refuses a truncated prefix
(`EngineError::SealTruncated`) or a hash mismatch (`EngineError::SealHashMismatch`),
writes the seal marker at high water `last_seq + 1` and returns the folded state
(`SealHash` = SHA-256 of the concatenated chunks, hex on the wire). A chunk that
landed beyond `last_seq` between the replay and the seal is refused
(`EngineError::SealChunkBeyondLastSeq`) so a chunk acked durable is never dropped
by the seal, and sealing a key that already carries a marker is refused
(`EngineError::AlreadySealed`) rather than rewriting the sealed record — the
deadline `seal_current` treats that as a lost race. `seal_partial` and
`seal_current` are the cancel gestures. `seal_current` folds the chunks under the
same per-key advisory lock it seals with and returns *that* state, so a chunk that
becomes durable while the seal is waiting for the lock is folded, deleted and
present in the returned state rather than read before the lock and lost. Boot binds the gitops-declared
`STREAMING_{service}` stream (bind-only, fail-loud; readiness stays DOWN when a
registered accumulator has no stream) and one ephemeral consumer per pod folds
every `(key, seq, chunk)` frame published on `stream.{service}.{key}` into the
same Postgres-backed accumulator as `Engine::push_chunk`, so a separate
out-of-process producer feeds the lane while late joiners, seal, the
refusal-after-seal marker and retention are unchanged; every pod drops chunks of
a sealed key. After the commit the sealing pod purges the key's NATS subject
synchronously and the beat is the backstop for a key still in the stream if the
pod died first. An engine that registers an accumulator with no service
configured fails loud at boot (`EngineError::AccumulatorWithoutService`) rather
than silently keeping an in-process-only `push_chunk` path with no stream to
bind. The flush takes each stream's advisory lock with a non-blocking try under
a `lock_timeout` and defers a stream a slow seal is holding to the next window,
so one slow seal never parks the whole pod's flush batch. The marker must
outlive the stream: boot fails loud unless `seal_retention` (the former
`chunk_retention`) covers the bound stream's `max_age`.

**Leader work — offers and mirrors.** Outbox relays with `RowClaim` and `Leader`
disciplines (a fenced lease over `leader_slot`), the hosted `HostedOutboxRelay`
and `KvDrainRelay` publishing the published language monotonically by key and
version (a per-key watermark in the engine's own schema survives restarts, so a
stale `Put` after a newer `Retract` is a no-op). The hosted outbox relay drains
in batches whose cap equals its drain bound, so a backlog bursts within one beat
instead of one batch per beat, and a periodic hygiene pass (`with_sweep_every`)
deletes rows that reached `PUBLISHED` or a terminal `FAILED` (whose audit copy
already lives in the dead-letter table) and sweeps `message_claim` rows older than
`with_message_retention`; because a durable replays from the start of its stream,
the engine refuses at boot a `message_retention` below the `max_age` of any
integration stream an inbound reaction binds (an unlimited `max_age` is refused
too), so a claim can never be swept before its message can still be redelivered.
The sweep is best-effort and never lowers readiness. The relay skips its publish
pass while `Nats::reachable()` is false, so a full outbox during a broker outage
costs nothing per beat and never freezes the single beat task (heartbeat, cron,
scheduled boundaries and the readiness refresh keep running; the `nats_grace`
probe alone takes the pod DOWN); each publish is bounded by `PUBLISH_ACK_TIMEOUT`.
A transient publish failure never counts an attempt while the broker is
unreachable — nor while a connected broker's JetStream cannot answer (a timeout,
a broken pipe or the engine's own ack-timeout, classified `Unanswered`), which the
relay halts on exactly like an outage — so a committed row waits out an outage or
an unavailable-JetStream window rather than exhausting a budget; only a genuine
broker rejection (a size or limit refusal, a wrong sequence) is bounded and, at
the bound, is dead-lettered (`DeadLetterSource::Outbox`) in the same transaction
that marks it `FAILED`. `service_engine_outbox_pending` and
`service_engine_outbox_oldest_age_seconds` gauge the backlog depth and its oldest
waiting row. `Offer` (`Row`, `Published`, `NAME`, `PREFIX`, `key`,
`publish`) and `register_offer::<O>()`: a saved or `cx.delete`'d noun that
carries an offer stages its dirty key (`service_engine.offer_dirty`) in the same
transaction as the write; the pod holding the offer's single fixed-slot lease
(renewed on the beat, taken over by another pod only once it expires) drains
dirty keys, claiming them skip-locked, reading each key's current bucket revision,
then resolving the row image in a short fenced transaction — the revision is
observed before the image, so a write landing between the two bumps the revision
and loses the compare-and-set rather than regressing the bucket. It then does the
KV round-trip outside any transaction — a `create` for an absent key or a
compare-and-set on that observed revision, a failed set leaving the key dirty for
the next drain — and finally raises the per-key watermark and deletes the marker
in a small fenced transaction that asserts the lease. So a concurrent mutation on an offered noun never waits on the drain, and
a leader frozen past its lease fails its writes instead of regressing the bucket.
It reconciles the bucket against the store on its first drain after boot and
every `with_offer_reconcile` period, so a rebuilt or drifted bucket is repaired
even under a stable leader; a `KvDrainRelay` over a truncated-and-rebuilt bucket
is repaired by `reset_watermarks` before its source re-stages its set.
`Mirror::new(name).consume::<C>()…consume::<D>().keyed_by(f).project(p)`
and `register_mirror`: a per-pod typed `Shadow<C>` of each consumed offer merged
by `keyed_by`, a full read of every consumed prefix at boot before readiness
reports converged, and leader-gated projection into `known_*` through the direct
lane (standbys keep shadows current and re-project their shadow on takeover, so a
change inside the failover window is never missed). A projector writes no SQL of
its own: it implements `Known` (a derived row's table write) and `KnownScope` (a
selector's delete) once, and the projection function calls `Projection::replace`,
`replace_one` and `remove`, which also stage the foreign impacts. The mirror
persists a per-bucket watermark — the bucket revision it has projected up to,
advanced by the leader as it projects — so a standby reports converged only once
its shadows are loaded **and** the watermark has reached the revision its boot
read reached, and the watch resumes from that revision (`watch_all_from_revision`)
so a put or retract between the boot read and the watch is not lost. A periodic
reconcile on `with_mirror_reconcile` (a new validated bound) repairs drift. A
consumed prefix that reads empty — at boot, at the reconcile deadline, or a
watch change that would empty it during a run — holds readiness DOWN with the
prefix's name and keeps `known_*` and the shadows as they are, never projecting
to empty.

**Blobs over S3-compatible object storage.** `register_blobs::<Kind>(BlobPolicy)`
records a per-kind policy; the service's bucket is bound at boot (bind-only,
fail-loud, never created). `cx.blob` / `cx.blob_owned` stage a blob reference row
(`service_engine.blob`) inside the pipeline transaction, so the reference commits
with the aggregate. `UploadUrl` is an S3 SigV4 **presigned POST** whose policy
carries `content-length-range = [0, max_bytes]`, so an oversize object is refused
at upload and never lands; `DownloadUrl` is a presigned GET. Neither is
`Serialize` (the POST carries endpoint + signed fields, not a URL string), so —
like `OneShot` — neither can enter a view, impact, offer, outbox row or chunk;
only the opaque reference travels. A download URL is minted only through the
gated `Query::download::<View>(key, reference)` gesture, never from a bare
reference: because a reference travels in a view, a bare-reference presign would
be a permanent bearer capability outliving the row and the viewer. `download`
takes the same visibility path as `fetch` — it presigns only when the caller can
currently see the referencing view and the loaded aggregate still lists the
reference in `Aggregate::blob_refs`, and resolves to `None` otherwise (the
authorization is the view's own gate, one code path with `fetch`). The beat reaper's scope is exactly two
categories — an incomplete upload (a `pending` row past `orphan_after` whose
object never landed) and an unreferenced blob (a reference released past
`orphan_after`, whose object it deletes) — and it promotes a completed upload,
recording the object's size from its head, independent of the orphan window; it
does not police size, because the POST policy already did. Orphan detection is at
the aggregate boundary: `Aggregate::blob_refs` exposes live references, the
pipeline diffs them between `load` and `save`, and a dropped/repointed reference
or a `cx.delete`'d aggregate releases the old blob in the same transaction, with
no slice-table scanning and no reference counting. Presigning uses `rusty-s3`
(GET) plus an in-engine SigV4 POST-policy signer (`hmac` + `sha2` + `base64`) and
`reqwest` (rustls) for the engine's own bucket HEAD/DELETE — no cloud SDK.

**Scope declaration at boot, assembled from the slices.** A slice contributes
its own scope keys from its register line — `Engine::contribute_scopes(&[..])` —
and `Engine::declare_contributed_scopes()` assembles the union into one
`ScopeManifest`, validates it into a `br_core_scope::ScopeDeclaration` (a
malformed key, a manifest spanning two services, or an empty manifest is a
boot-time error), and declares it; a service that contributes nothing is
scopeless and skips the gate. `Engine::declare_scopes(ScopeManifest)` remains for
a service that assembles the manifest itself. After the mirrors converge, `run`
drives the scope-declaration handshake over the engine's own NATS connection and
holds readiness DOWN until Identity confirms; a rejection keeps the pod DOWN with
the reason and returns `EngineError::Scope`, so a scope typo is a failed deploy,
not a silent deny. The handshake takes the frozen wire from the frontier crates
`br-core-scope` and `br-scope-declaration-contract` and renders the subjects
itself, with no `br-util-nats-fabric` dependency.

**Principal facts contributed by the slices.** `Engine::register_principal_fact`
registers a per-slice loader (`PrincipalFactLoader<P>`) that the engine runs
right after the principal is resolved — at the GraphQL attach/request path and on
a principal-facts refresh — so a slice's fact reaches the principal without the
service kernel loading it by name (a fact-load failure is fail-closed, matching a
resolve failure). With `contribute_scopes` above, this lets the service kernel
name no slice: a slice that owns a table exposing a principal fact registers a
loader, and a slice that gates on scopes contributes them, both from the slice's
own folder.

**Erase a person.** The `Erasable` trait (`erase(cx, person)`), the `Erase`
context (the write pipeline's `Ops` carrying the `PersonId`), the `Erased`
post-commit purge manifest, and `Engine::register_erasable`. `Engine::erase`
(via a cloneable `Eraser<P>` captured before `run`) opens **one** direct-lane
transaction that sets a transaction-local `app.erasing = 'on'`, so a strict,
deny-when-unset RLS policy erases under the low-privilege app role by
whitelisting `current_setting('app.erasing', true) = 'on'`. It records the
durable fact in `service_engine.person_erasure`, runs every slice's `erase`
across all three styles, persists the manifest's stream/presence/blob keys onto
that row, stages `PersonErased` (`integration.evt.{service}.person.erased.v1`)
through the outbox on the fresh erase only, and commits (a failing slice rolls
the whole gesture back); after the commit it purges the person's accumulated-lane
data (keeping the `accumulator_seal` marker unpurged and deleting only the
Postgres chunks, so the erased key stays sealed and the beat's seal-purge then
drops its NATS subject), presence keys and blobs, then marks the row purged. The purge
is durable: a pod that dies between commit and purge leaves the row unpurged and
the beat drains it over the persisted manifest. A slice retracts the person's
offers by dirtying them — `cx.delete` for a deleted row, `cx.dirty_offer` for a
row anonymised outside the aggregate API — so the leader retracts within a beat.
Idempotent.
Other services react to `PersonErased`; `known_*` mirrors follow the producer's
offer retract, never the event.

**GraphQL surface kit (`graphql`).** `compose_service!` lists a service's slices
once (module, cargo feature, root objects on one line) and generates the merged
`QueryRoot`/`MutationRoot`/`SubscriptionRoot` and the `register` function, so
`register.rs` and `graphql.rs` name no slice. `engine_schema` composes the roots;
`app` serves `POST /graphql`, the `graphql-transport-ws` subscription on
`GET /graphql/ws`, and `/readyz`; `run_with` / `run_with_listener` own the engine
loop and the HTTP server in one call. Mutation resolvers run on
`Engine::mutation_executor` (`ack` / `execute` and bulk forms). A domain refusal
carries its `Reason` code to the caller; an internal fault the pipeline itself
owns — the transaction begin, or the flush-and-commit of the staged impact /
outbox / snapshot writes — surfaces as a generic `mutation failed: database` with
no sqlx internals on the wire, while the full underlying error chain is logged at
`error` level server-side (with the failing stage), so the cause the wire
deliberately hides is still recoverable by an operator rather than discarded. The
reaction pipeline gets the symmetric treatment: an internal `Db` fault classified
on the flush-and-commit of a reaction (or the replay of a stored confirmation)
logs the same error chain server-side, so a database fault on a reaction is no
longer opaque in the dead-letter row and ack path — a *terminal* fault at `error`,
a *retryable* one (redelivered) at `warn`, so a transient blip does not read as a
hard error on every redelivery. Because that recovery is a server-side log, the
`example-service` e2e harness installs a `tracing` subscriber (honouring
`RUST_LOG`, defaulting to `error` so a green run stays quiet while a fault's cause
still surfaces) that writes to process stderr — not the per-test capture, whose
thread-local buffer would miss a cause emitted from a Tokio worker thread — so a
fault raised by the in-process engine reaches the CI job log rather than being
discarded by a test process that installed no subscriber. Query resolvers
take a typed `Query<'_, P>` context and read rendered views through
`cx.fetch::<Projector>` / `cx.fetch_window::<Projector>` (and `fetch_view` /
`fetch_view_window` over the ergonomic `view::Projector`), never the database,
under the RLS regime the **projector** declares — `view::Projector`'s `const RLS`
(the raw `projector::Projector`'s `renders_under_rls`), read on the fetch, the
snapshot, the render pass and the repair alike, so a fetch and a subscription of
one key by one principal are one code path with no per-call switch that could
make them disagree. A `WindowSpec`'s `rls` flag is a caller assertion validated
against the projector at attach: a contradiction is refused with
`AttachError::RlsRegimeMismatch`, an RLS projector with no `RlsApplier` with
`AttachError::MissingRlsApplier`. The reference RLS projector `OrgBoardsRls`
reads the `org_board` projection under the applier's org context on every path —
`populate`, so the window is scoped at populate time and not merely at render,
and the render load through its own `OrgBoardStore`. That projection is
genuinely deny-when-unset: with no `app.current_org_id` in the transaction it
returns no rows, so a read that forgets the context leaks nothing instead of
falling back to every row. The base `board` table carries no RLS and is the
non-RLS cohort projector `BoardsView`'s source, which must see every candidate so
`Board::memberships` can grant a cross-org `Member` visibility in the app layer
that a fail-closed org policy would wrongly hide. The two regimes therefore sit
on two sources — a fail-closed `org_board` for RLS, the plain `board` table for
cohorts — and the render regime is the projector's, never the table's. The subscription is one typed
union member per projector via `subscription_union!` (and `presence_subscription_union!`
for a presence lane) — the `Reset`/`Upsert`/`Remove` payloads over a typed view
union with the contiguous revision and the causing event as `cause`. Each slice
declares its root fields and types as a `SliceFragment`
(`Engine::register_schema_slice`); the engine assembles them at the start of
`run` and fails boot loud with `EngineError::DuplicateSchemaMember` (two slices
claiming one root field or type) or, once fed the composed SDL with
`set_schema_sdl`, `EngineError::UndeclaredSchemaMember` (a real root field no
slice declared). The axum layer resolves the principal from the trusted
`X-Passport` header (`PassportPrincipal`) — authZ only, never authN. The
subscription principal is resolved once at the WebSocket handshake and serves
every operation on that socket, so the kit bounds the connection itself: it
closes the `graphql-transport-ws` socket at `session_max_age` measured from the
handshake with a `1001` going-away close frame (`SESSION_MAX_AGE_CLOSE_CODE` /
`SESSION_MAX_AGE_CLOSE_REASON`), then the client reconnects on a fresh upgrade
where the gateway re-injects the resolved `X-Passport`. A revoked scope cannot
outlive the bound by keeping the socket open and re-subscribing, and a mutation
over the socket runs under a principal no older than the bound.

**Ergonomic projector surface (`view`).** A `view::Projector` (re-exported as
`service_engine::Projector`) names `type Noun` / `type Store` / `type Query` /
`type Visibility` and writes only `async fn populate(cx, q)` over a `Populate`
context and `fn project(row, principal)` — no future plumbing and no render load
SQL; the engine loads through `Persistence::read_many` and `ViewProjector` (a
zero-sized adapter) owns `Facts`, the `LoadScope` match and the derived
`name`/`nouns`/`inverse`. `type Visibility` makes the same cohort declaration
`populate` uses through `Visibility::window` the render-time gate too: the engine
applies the projector's `visible` method (defaulting to that declaration) before
projecting, so a row that leaves the principal's cohorts is delivered as a
`Remove` and one that enters as an `Upsert`. `Ops::impact_principal_facts(id,
deps)` stages the principal-facts impact a mutation or reaction emits when it
changes what a principal may see; the engine re-resolves that principal and
repopulates every window shape (a `Population::Keys` window included) so both
directions reach a live session within the frame. A projector gated by Postgres
RLS or open to every viewer declares `type Visibility = Unrestricted<Row,
Principal>`, an explicit "no cohort gate here" rather than a permissive default.
The low-level `projector::Projector` stays as the escape hatch for a projector
that joins nouns. `view::Projector` also carries the render-pass hooks: `fn
cohort(principal)` (default per-principal) groups the sessions a frame loads
together — sessions sharing a cohort key load once and personalise per session,
and in debug the pass recomputes each grouped session's cohort key and asserts it
equals the one it was grouped under, so a `cohort()` that is not a pure function
of the principal (which would land a session in more than one cohort) panics in
test; that a shared cohort renders one identical view for all its members is not
re-projected (it would double the load and projection the cohort saves, and
`s164` proves it directly) but rests on the total, injective `Visibility`
declaration and the collision-free `CohortKey`; `const RESET_THRESHOLD:
Option<usize>` overrides the global
`reset_threshold` per projector; and `fn emission(&Impact)` chooses `PerImpact`
(one delta per causing impact with its `cause`) or the default `Coalesced`, where
a `PerImpact` projector folds a **causeless** impact (principal-facts, foreign,
scheduled) coalesced instead of faulting, since only a caused impact can be
delivered per impact. A `Cause` that exceeds one 8000-byte notification fails the
write transaction fail-closed rather than being dropped — a cause is a small
fact, bulk rides the view.

**Page through history behind a live window.** The kit gesture
`service_engine::page::<P, V>(ctx, session, &cursor)` re-runs `populate` with a
cursor and appends the older keys to the window the session holds, delivering them
as `Upsert`s on the contiguous revision — scrolling back never sends a `Reset`. A
`Remove` leaves a window only when the row is deleted or becomes invisible, never
because it fell off a page bound. `window_capacity` (validated at boot) bounds the
keys a session holds across its pages: once appending a page would exceed it the
oldest appended page is released from the window and `last_sent` with no `Remove`
delta (the client that
asked for the page drops it too), while the live head is always retained; a paged
history survives a principal refresh and a reconnect `Reset`. The gesture is
authorized against the caller's `Passport`: the engine serves only a live session
**owned by the calling principal**, and refuses a page for a session this pod does
not hold, or one held for a different principal, with `EngineError::NoLiveSession`
(knowing another session's id buys nothing). `attach_with_session` / a
client-supplied `SessionId` correlates the subscription and the `page` mutation.
The reference `card` slice ships the pair (`cardPageDeltas` + `pageCards`). A
page renders its appended keys outside the session lock, so the final delivery
under the lock skips any paged key a concurrent render pass has already
delivered (its `last_sent` is present): a key scrolled in while it is being
written settles on the committed view, never a stale page render that lost the
race to the pass (`s183`).

**Engine-owned NATS, Postgres connect and readiness.** The engine's internal
loops run on `async-nats` directly through the `nats` module (`Nats`, `KvBucket`,
`KvKey`, `RelayHealth`, `PublishOutcome`). `service_engine::connect_pool` /
`validate_database_tls` is the engine's own secure-by-default pooled Postgres
connect (remote hosts require `sslmode`; a `host=`/`hostaddr=` override is
refused even percent-encoded; `TRUSTED_NETWORK_HOSTS` is the explicit per-host
opt-out), and `service_engine::{Readiness, ReadinessHandle, readiness_route}` is
its own readiness handle and `/readyz` route.

**Configuration, degradation and observability.** `EngineConfig` validates every
bound of the intent's config table at boot (`session_max_age`, `lock_timeout`,
`nats_grace`, `listener_queue_threshold`, `window_capacity`, `impacts_per_commit`,
`listener_channel_capacity`, the `lease` outlasting the `beat`, `session_max_age`
outlasting `session_ttl`, `listener_queue_threshold` in `(0.0, 1.0]`,
`lock_timeout` and `publish_ack_timeout` each strictly below `ack_wait`,
`max_ack_pending` positive) and carries an optional `service` label and
`http_addr`. `ack_wait` (30 s), `max_ack_pending` (256) and `publish_ack_timeout`
(2 s) are fields on `EngineConfig` that build the inbound consumer and bound a
JetStream publish, so the lock-timeout-below-ack-wait and
publish-ack-below-ack-wait relations are enforced rather than assumed. A session lives at most `session_max_age` (ended with the
stream-closing signal so the client reconnects with a fresh passport, distinct
from `session_ttl`); the WebSocket connection carrying it is closed at the same
bound measured from the handshake, so the bound holds even when a client keeps
the socket open. A `NatsHealth` tracker keeps the pod UP through an outage
shorter than `nats_grace` and DOWN past it. Past `nats_grace` the accumulated and
presence lanes pause and every session that watches one is told on its
subscription: the engine emits an out-of-band `LanesPaused { lanes }` notice and,
on return, a `LanesResumed { lanes }` notice followed by a `Reset` of the session.
The notice carries no revision, so `Reset` / `Upsert` / `Remove` contiguity is
untouched; the subscription union exposes `LanesPaused` / `LanesResumed` as
members, `Engine::lane_notices` exposes the raw signal, and the union's generated
`subscribe(deltas, notices)` merges it. Every metric is labelled by `service`
and `pod`; `impacts_committed_total` is the notify-budget counter,
`dead_letters_total` counts dead letters by source, `outbox_pending` and
`outbox_oldest_age_seconds` gauge the outbox backlog and the age of its oldest
waiting row, and each degrade-table
dependency (postgres, listener, nats, mirrors, inbound) is a `dependency_up`
gauge. Five alerts ship as a `PrometheusRule` in
`observability/service-engine-alerts.yaml`: a filling notification queue, the
per-cluster notify budget nearing its ceiling, a sustained reset rate, an aging
outbox backlog, and dead-lettered work waiting on a human.

**Postgres schema (reserved range),** applied by `schema::migrate`
(`ignore_missing`) with `grant_engine_access`: `scheduled_impact`, `leader_slot`,
`accumulator_chunk`, `accumulator_seal`, `kv_relay_watermark`, `message_claim`,
`sequence_guard`, `dead_letter`, `scheduled_message`, `offer_dirty`, `blob`,
`person_erasure`, `schema_version`, `event_log`, `event_snapshot`,
`mirror_watermark`. Scheduled boundaries are claimed against the database clock,
never the pod clock. The app-role grant includes `USAGE, SELECT` on the engine
schema's sequences.

**`conformance-service-engine`.** The battery runs in **two modes** against real
infra (a fresh database and a spawned `nats-server` per test, plus a spawned
`minio` for the blob scenarios). **In-crate mode** — the named scenarios
`s001`–`s190` — drives the real engine through an in-crate `sample` service and
keeps the properties that need the `test-support` seam (a driven clock, fault
injection, a one-shot offer-drain pause, direct impact-bus/transport
assertions): shared-consumer ownership
across two pods, ack-after-durable with a crash before commit, poison budget to
dead letter, early parking and release, the sequence guard, the mutation gate
deny/allow through one pipeline, all three persistence styles over one `counter`
sample, a render frame that takes no row lock while a mutation does (`s130`), the
seal and its hash barrier, presence, offers and leader failover, the mirror,
scope declaration against a fake Identity, the blob presigned-POST round-trip and
reaper, erasure across slices, the query-time RLS gate, the schema-collision and
undeclared-member boot gates, reconnect-resnapshot repair, a `Terminal` reaction
that dead-letters through the running inbound loop (`s128`), a live `Upsert` that
reaches only its own projector's subscription-union member (`s129`), and a
principal-facts change whose refresh resolver errors ending the session
fail-closed through the running engine (`s131`), the listener isolated into a
bounded channel whose overflow resets every live session (`s132`) and whose
queue-usage brake closes the listener into readiness DOWN (`s133`), the
impacts-committed metric counted only after the transaction commits (`s134`), a
row that leaves its cohort removed on the `view::Projector` surface with a fixed
window repopulated in both directions on a principal change (`s135`), the
schema-version singleton refusing a second pod on a different service version
(`s136`), erasure under a strict deny-when-unset RLS policy (`s137`), the
beat completing a purge the erase committed but never finished (`s138`), and
erasing a person purging their chunks from the lane-A NATS stream (`s141`). The
view-surface scenarios prove paging and its render hooks: a page appends the
older keys behind the live window with contiguous revisions and no `Reset`, and
an edit to an old paged row reaches its holder (`s162`); `window_capacity`
releases the oldest page silently — no `Remove`, the live head retained (`s163`);
one load per `(dirty key, cohort)` per frame with several sessions in one cohort
and two in another on the `view::Projector` cohort hook (`s164`); a
per-registration `RESET_THRESHOLD` resetting a window where the global default
still diffs (`s165`); a `PerImpact` view emitting its cause per caused impact
yet folding a causeless impact coalesced without faulting (`s166`); and a page
request for a live session the caller does not own refused
(`EngineError::NoLiveSession`) while delivering nothing on the victim's wire,
the caller still paging her own session (`s167`). A window that asserts an RLS
regime the projector does not declare, and the reverse, are both refused at
attach (`s168`); and a stuck worker re-recorded on every beat records its
dead-letter row once and increments `service_engine_dead_letters_total{source}`
once, never once per restart, while a genuinely distinct dead letter still
counts (`s169`); and a `LaneNotice` maps through a service's generated
`subscription_union!` into the `LanesPaused` / `LanesResumed` union members and
`subscribe(deltas, notices)` merges them into the subscription stream, so a lane
pause reaches a GraphQL subscriber and not only `Engine::lane_notices` (`s170`);
and a `KvDrainRelay` whose published-language bucket an operator truncated and
rebuilt refuses to re-put its unchanged-version keys until `reset_watermarks`
clears the per-key watermark, after which the source's re-staged set converges
the rebuilt bucket back to the store (`s171`). The outbox-outage scenarios prove
a broker outage never loses a committed row nor freezes the beat: rows committed
while NATS is down wait as `PENDING` and deliver exactly once on restart, with no
dead letter (`s175`); a full outbox backlog with NATS down keeps the beat ticking
— the schema-version heartbeat and a cron's leader slots keep advancing while
readiness carries the nats reason (`s176`); the same backlog does not delay the
`REASON_NATS_UNREACHABLE` verdict past `nats_grace` plus a small detection margin,
where a beat frozen on the backlog would have held the pod UP for minutes
(`s177`); and a row the *reachable* broker keeps rejecting is retried under a
bound and then dead-lettered `DeadLetterSource::Outbox`, never silently
abandoned as `FAILED` (`s178`).
**Black-box mode** — `bb01`–`bb06`
— spawns the real `example-service` binary (and the `example-twin` binary for the
cross-service cycle) and drives them over their public channels only (GraphQL
over HTTP and `graphql-transport-ws`, NATS subjects and streams, the
published-language KV, Postgres state, `/readyz`): readiness plus the boot scope
handshake plus a second pod on the same store (`bb01`); the affordance==gate
identity (`bb02`); `Reset`→`Upsert` with a contiguous revision and a reconnect
`Reset` from committed state (`bb03`); a full cross-service cycle driven by the
spawned twin binary (`bb04`); and seal — a streamed reply sealed against its hash
inside the running binary (`bb05`); and the `graphql-transport-ws` socket closed
at `session_max_age` measured from the handshake, so the client reconnects with a
fresh passport (`bb06`). `bb05` drives the real lane-A ingress: the
spawned `example-twin` streams the reply's chunks over NATS on
`stream.example.{reply_id}`, the running binary's ephemeral consumer folds them,
and the reply-finished command then replays, verifies the hash, commits and
delivers — nothing seeded through Postgres. The in-crate `s139`/`s140` scenarios
prove the boot bind (absent stream → DOWN, `seal_retention` below the stream's
`max_age` → boot fails loud) and that a separate producer's chunks reach two
pods while a chunk after the seal is refused on every pod. The black-box harness
provisions the owner/app Postgres roles and the
engine + example migrations, provisions NATS, seeds the roster and answers the
scope declaration, then spawns the binary and gates on `/readyz`, taking the
binaries from `EXAMPLE_SERVICE_BIN` / `EXAMPLE_TWIN_BIN` when set and building
them on demand otherwise. `infra/pg.rs` / `infra/nats.rs` are the sole
`async-nats` user, only to declare gitops-owned streams and buckets.

- CI runs the battery in both modes: the `conformance-service-engine (real
  infra)` job runs the whole crate (both modes, needing MinIO for the in-crate
  blob scenarios), and a dedicated `conformance-service-engine black-box (real
  binary)` job builds the two example binaries and runs only `bb01`–`bb06`
  against them on real PostgreSQL and a spawned NATS — no MinIO, since the example
  binary boots without S3 and no black-box scenario exercises a blob.

**Reference service (`example-service`, `example-contract`, `example-twin`).**
A complete, bootable service built **only** on the public authoring surface (no
`test-support`, no `pub(crate)` reach-around), laid out as the intent's Code
structure prescribes: a thin `kernel/` (the principal and its generic
`PrincipalFacts` bag, the error base — and no scope registry), one folder per
slice, a `slices/mod.rs` that lists the slices once through `compose_service!`,
and a slice-agnostic `register.rs` and `graphql.rs`. **Every slice is removable
by deleting its folder plus its one `compose_service!` line, with no kernel
edit** — including a slice that contributes scopes or a principal fact: `board`
owns `BOARD_ARCHIVE` and the `BoardMemberships` principal fact and registers both
from its own `register`, `card` owns `CARD_ADVANCE`; the kernel names no slice.
The `removability` CI job proves the kernel-only build and each slice removed in
turn. The five slices exercise every gesture: `board` (direct CRUD, gate ==
affordance, cohort `Visibility`, RLS read projector, published-language offer,
`OneShot` invite, `Erasable`, a contributed scope and principal fact), `card`
(soft EDA, command and event reactions, an emitted integration event, a
scheduled deadline reaction, a cron, a bulk import, a contributed scope), `ledger`
(full EDA with upcasting, a hydration barrier, in-log erasure), `reply`
(accumulated lane + verified seal, cancel-in-flight, presence, blob attachment;
a finish the seal cannot honour is answered with a `SealFailed` integration
event so the runner resends a corrected finish, rather than nak-ing forever into
a silent dead letter — a finish whose `last_seq` sits below an already-durable
chunk fails at once, and a truncated stream is retried a bounded number of times
(`cx.delivered`) so an in-flight fold can heal and, once the truncation is
permanent, is answered with `SealFailed`; a hash mismatch is answered with
`SealFailed` the same way, so the runner republishes a corrected finish) and
`roster` (a
KV mirror into `known_persons`). `example-twin` is a **separate**
crate (the producer/runner that closes the cross-service cycle over NATS), so the
reference service holds no NATS client. `tests/e2e.rs` (split into
`tests/scenarios/`, driven by `tests/harness/`) proves the slices against real
PostgreSQL, NATS and MinIO over the four observation channels — including the
subscription deltas over a real `graphql-transport-ws` WebSocket with their typed
cause — and two-pod convergence.

### Changed

- **The write-path serialisation is engine-owned; `Persistence::lock` is an
  optimisation.** `load` is a plain, non-locking read and `read_many` (the batched
  render read) defaults to it, so both reads are lock-free and a store author who
  writes only `load` gets a render frame that never takes a row lock. The write
  pipeline serialises concurrent commands on one aggregate itself: before `load` it
  takes a transaction advisory lock keyed on `(store type, JSON key)` — a
  `pg_advisory_xact_lock` over a domain-tagged FNV-1a hash with a byte separator
  between the parts, so two keys or two store types never collide — held to commit.
  Concurrent commands on one key therefore serialize in every style even when the
  store's `lock` is the default no-op; the reference stores' `SELECT … FOR UPDATE`
  on the row (or snapshot) key stays as an optimisation that also pins the row
  image. The advisory lock runs inside the pipeline transaction under
  `lock_timeout`, so a contended write that waits past the timeout is retryable,
  and a render never waits on it. A store that wants a single batched
  render query overrides `read_many` (`WHERE id = ANY($1)`) per the "every read
  function answers in one query" rule; the reference stores do.
- **A connected broker whose JetStream cannot answer is an outage, not a
  failure.** A publish that ends in a timeout, a broken pipe or the engine's own
  ack-timeout is classified `Unanswered` in one shared place; the outbox relay and
  the scheduled-message publisher both treat it exactly like an unreachable broker
  — they spend no delivery attempt, halt the pass and let the committed rows wait,
  so a JetStream-unavailable window over a live TCP connection never dead-letters
  or `FAILED`s a row (`s189`, `s190`). Only a real broker rejection (a size or
  limit refusal, a wrong sequence) still counts as a bounded attempt.
- **The scheduled-message publisher claims one row per transaction** and publishes
  through the same ack-timeout seam as the outbox, instead of holding the whole
  due batch locked across a sequence of raw publishes.
- **The offer relay reads the published-language revision before it resolves the
  row image** (both inside the leader's fenced pass), so a lease that expires
  between the two can never let a stale image win the compare-and-set and regress
  the bucket (`s188`).
- **An inbound frame with no resolvable envelope derives its dead-letter id from
  the `Nats-Msg-Id` / `Br-Message-Id` header** when one is present, so a redelivery
  records one dead-letter row rather than a fresh one each time; only a frame with
  no id header at all falls back to a generated id.
- **The retention sweep also removes terminal `FAILED` outbox rows** once they age
  past the retention window, so the outbox table stops accumulating rows whose
  audit copy already lives in the dead-letter table.

### Removed

- **The engine depends on no `br-util` crate.** It owns its NATS
  (`br-util-nats-fabric`), its Postgres connect (`br-util-postgres`) and its
  readiness (`br-util-axum-readiness`), and consumes no `br-util-directory`; the
  same fail-loud, bind-only, never-provision discipline applies, implemented over
  `async-nats` directly. The only `br-rust-common` crates it keeps are the
  frontier ones that cross the service boundary — `br-core-auth`,
  `br-core-integration`, `br-core-scope`, `br-scope-declaration-contract`.
- **`EngineError::NotYet` is gone.** No engine gesture returns it; every
  author-facing surface of the release is implemented.
- The engine-owned `EngineDelta` / `ProjectedView` / `*Payload` types and the
  free `subscribe` / `to_engine_delta`: a service maps deltas through the
  per-projector typed union the `subscription_union!` macro emits instead.

## 0.0.0 - 2026-09-02

### Added

- Repository scaffold: workspace root, governance files (LICENSE, CONTRIBUTING,
  SECURITY, SUPPORT, PR template, issue-template config), `.gitignore`, and
  `deny.toml`.
- Two empty crates — `service-engine` (the engine) and
  `conformance-service-engine` (its black-box conformance battery) — carrying no
  dependency and no code. Both ship with 0.1.0.
- CI (`ci.yml`): fmt + clippy + test, MSRV 1.89 build, `cargo doc`, `cargo-deny`,
  `cargo-machete`, `cargo semver-checks`, changelog + README-pin check,
  shellcheck, trufflehog secret scan, and the conformance battery against real
  PostgreSQL 16 + NATS JetStream.
- CD (`release-tags.yml`): auto-tag and release the unified workspace version on
  merge to `main`.

No engine functionality.
