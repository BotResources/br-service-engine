# br-service-engine

The reactive personalized delivery and process skeleton a service built on it
runs on — sessions, cohorts, impacts, projection, diff, multi-pod fan-out,
streaming sources, boot, relays, cron, and mirror supervision. Two crates share
one workspace version: the `service-engine` library a service builds on, and
`conformance-service-engine`, its black-box conformance battery run against real
PostgreSQL and NATS JetStream. Not published on crates.io and shipped as no
image and no CLI: the git tag is the release.

## Install

A service depends on the `service-engine` library only. `conformance-service-engine`
is not a kit to import: it is this repository's own executable spec and lives here.

```toml
[dependencies]
service-engine = { git = "https://github.com/BotResources/br-service-engine", package = "service-engine", tag = "v0.3.4", version = "0.3.4" }
```

The `version` beside the `tag` is required: a tag-only git dependency carries a
`*` version requirement, which `cargo-deny`'s `wildcards = "deny"` rejects. The
engine has its own version line and depends on `br-rust-common` alone; each
engine minor pins one exact `br-rust-common` tag.

| Engine version | `br-rust-common` |
|---|---|
| 0.3.4 | `v1.3.0` |
| 0.3.3 | `v1.3.0` |
| 0.3.2 | `v1.3.0` |
| 0.3.1 | `v1.3.0` |
| 0.3.0 | `v1.3.0` |
| 0.2.0 | `v1.3.0` |
| 0.1.0 | `v1.3.0` |

## Module map

The engine is one crate laid out one module per capability. The multi-pod
delivery core (`transport`, `render`, `session`, `cohort`, `population`,
`accumulator`, `housekeeping`, `relays`, `mirror`, `principal`, `projector`) and
every author-facing surface in the table below are implemented and
battery-backed.

| Module | Responsibility |
|---|---|
| `engine` | `Engine::boot` and the `register_*` / `contribute_scopes` / `declare_scopes` / `register_principal_fact` / `erase` surface; `engine::boot` also holds the boot kit (`run_service` / `BootPlan`) — the one call from a service `main` that dispatches on argv over three entry points: `migrate` (owner role, from `DATABASE_URL_OWNER` only, applies the engine, library and service migration sets in that order, waits for the app role, grants it every schema), `serve` (app role, refuses an unmigrated store and names the pending set, derives `message_retention` from the bound streams, installs logging + `/livez` + `/metrics` + `/sdl`, and serves), and `schema` (prints the SDL, touches no infra). A service reads no engine env by hand: `EngineConfig::from_env()` reads the ops contract in one place |
| `nats` | Engine-owned NATS: stream/bucket bind, KV read/write/watch, outbox publish |
| `inbound` | Inbound NATS loop: durable consumer, poison/dead-letter, `Disposition` |
| `pipeline` | Direct write pipeline; `Mutation` / `Reaction` / `Bulk` contexts; `OneShot` |
| `persistence` | `Persistence` trait + `Aggregate` (`Clone`); CRUD, soft-EDA and full-EDA behind one trait; a **required** non-locking `read_many` (one batched read per call — `WHERE id = ANY($1)` for a row store; there is no per-key default, so a store without a batched read does not compile) with `load` derived from it (a one-key `read_many`; override it only when a single-key read is cheaper), `save`/`create`, a `lock` the write pipeline calls before `load` (default no-op; the reference stores implement it as `Self::row_lock(conn, table, key)`, the defaulted `SELECT … FOR UPDATE` helper (which locks the row by its `id` column), as an optimisation — the engine already takes a per-key transaction advisory lock in `load`, so a lock-less store still serialises), and a `delete` the pipeline calls from `cx.delete` (default refuses with `EngineError::DeleteUnsupported`, so a store that never deletes writes nothing); log-style events reach `save` via `Aggregate::pending_events` |
| `full_eda` | the full-EDA kit: `EventSourced` (a slice's aggregate declares `NOUN`, `EVENT_VERSION`, a `SNAPSHOT_EVERY` cadence, `to_snapshot`/`from_snapshot`, `genesis`, `apply`, `check_hydrated`, `upcast`) and `FullEda<T>` — a `Persistence` implementation over the engine's own generic `event_log` + `event_snapshot` tables (keyed by noun). Generic append with seq arithmetic and per-key uniqueness, replay from the snapshot with the hydration barrier, a configurable snapshot cadence (not on every save), the upcasting hook, and `full_eda::erase` (rewrite a person's events in place, then re-snapshot from a genesis replay of the rewritten log in the same transaction). `full_eda::keys` lists a noun's keys for a window `populate`. A slice sets `type Store = FullEda<Self>` and writes no persistence SQL |
| `gate`, `visibility` | `Gate`/`Reason` (a reason code is `SCREAMING_SNAKE_CASE` matching `^[A-Z][A-Z0-9_]+$`, validated in `Reason::new` — a mistyped literal is a compile error — and `Reason::parse` for a code decoded from the wire), `Affordances`, the `gated!` macro and `check_gates_match_affordances` (affordance == mutation check, one function); `Visibility` cohorts/memberships deriving the `visible` filter and the `window` membership from one declaration, with `check_window_matches_visibility`; the derived window shape (`LIVE`/`DEPS`), the `CohortIndex` read seam (`keys_in_cohorts`) plus `view::cohort_window`/`windowed`, and `Unrestricted<_, _, Why>` carrying an `open_access!` `AccessReason` |
| `accumulator` | Accumulated lane (lane A): `register_accumulator`, the `STREAMING_{service}` stream bound at boot, one ephemeral consumer per pod folding `(key, seq, chunk)` frames into Postgres, `Ops::seal*` and the seal marker |
| `presence` | Presence lane: `EPHEMERAL_*` bucket, `register_presence`, `cx.present` |
| `offer` | `Offer` trait (`VERSION`), `register_offer`, `register_offer_trigger::<O, T>` (a `T: OfferTrigger<O>` in the offer's own slice re-publishes the offer when it changes; its `row_key()` names the offer row's store key and its `key_from()` the offer's KvKey), leader-drained dirty keys, versioned watermark, boot + periodic reconcile, `OfferManifest` published at reconcile |
| `mirror` | `register_mirror` over the direct KV watch into `known_*`: multi-offer `keyed_by` join, `Projection` `upsert` (row-diff) / `replace_rows` (full-row set diff inside a declared scope: stages exactly the rows inserted, changed on any column, or deleted) both returning `Written` and staging nothing when unchanged, `KnownRow::PRINCIPAL` (the uuid key column whose principal's facts a staged row also refreshes), plus `retire` (stages only a deleted row) and the `Known`/`KnownScope` escape hatch (`replace_one`/`remove`), `require_key`, offer-manifest and per-value `wire_version` verdicts (dead-lettered, readiness-neutral), leader-gated projection, per-bucket stream identity + boundary watermark, watch-from-boundary, periodic reconcile |
| `blobs` | Object-storage references, `register_blobs`, presigned URLs, reaper |
| `scopes` | scopes assembled from the slices' `contribute_scopes` (`declare_contributed_scopes`); the `declare_scopes` handshake gates readiness |
| `erase` | `Erasable` and `engine.erase(person)` (person-erasure only) |
| `dyn_compat` | Type-erasure wrappers behind the registries (`ErasedProjector`/`ErasedAccumulator` and their adapters) |
| `view` | ergonomic projector surface: a `Projector` declares `type Noun`/`type Store`, a typed `Query`, `type Visibility`, `async fn populate(cx, q)` and `project(row, principal)`; the engine loads the noun's rows through `Persistence::read_many`, applies the projector's `visible` gate (defaulting to the `Visibility` declaration) before projecting so a row that leaves the principal's cohorts becomes a `Remove`, and `ViewProjector` owns `Facts`, the `LoadScope` match and derives `name`/`nouns`; a view that joins a mirror declares `fn inverse(foreign) -> Inverse` (`Keys`/`Query`/`Lookup`/`None`, default `None`). The low-level `projector::Projector` is the join escape hatch |
| `readiness` | `Readiness`/`ReadinessHandle` and the `/readyz` route, re-exported from `br-util-axum-readiness` (the engine holds no copy); the shared crate's `readiness: UP` / `readiness: DOWN` tracing wording is the one the black-box battery greps |
| `db` | `connect_pool` + `validate_database_tls`: the engine's own pooled Postgres connect, secure-by-default (remote hosts need TLS; `TRUSTED_NETWORK_HOSTS` is the per-host opt-out) |
| `graphql` | async-graphql kit; `compose_service!` (one line per slice generates the merged roots + `register`, with a mandatory `prefix = <snake_ident>;` and a `slice … from <lib>::<macro>` arm that embeds a library slice at the host's prefix and principal), `RootPrefix` (validate + `owns`, declared once and gated at `assemble`/`verify`, boot errors `RootPrefixInvalid`/`RootPrefixUndeclared`/`RootPrefixRedeclared`/`RootFieldOutsidePrefix`), the generic `gated! { generics [P: …] ; … }` and `subscription_union! { generics [P: …] ; … }` arms so a library writes its gate and delta union once over the principal, `pastey` re-exported for library slices, `app` answering `POST /graphql` as JSON or — on `Accept: text/event-stream`, the gateway's subscription leg — as a graphql-sse stream bounded like a `/graphql/ws` session, `run_with` boot, the edge (`/livez` + `/metrics` + `/sdl` and the HTTP metrics layer beside `app`) mounted by `serve` (`with_edge_observability` is crate-private), typed `Query` context (`fetch_view` / `fetch_view_window` over a typed `Query`; `load_visible::<View>(key)`, one aggregate behind its view's `Visibility` and RLS regime; `read_under_rls(|conn| …)`, hand SQL in a read-only transaction under the principal's RLS context), per-projector typed subscription union, `SliceFragment::derive` reading each capability's root fields and owned object types from its `#[Object]` impls, the composed schema parsed and gated against the fragments at boot (unclaimed root field or object type fails loud) — the subscription delta-envelope object types are derived from any union that also lists `LanesPaused`/`LanesResumed` and exempted from the gate, so two subscription slices share one delta union with no synthetic slice; `coded_error`/`forbidden` for a coded refusal on a query or subscription; each slice's SDL fragment is emitted as a committed `schema.graphql` |

No `register_*` method or engine gesture returns `EngineError::NotYet`; every
author-facing surface is implemented.
`register_reaction` records a reaction and
derives its inbound subscription, and the engine-owned inbound loop (durable
consumer, ack-after-durable, `Disposition` routing, poison budget with the
`service_engine.dead_letter` table and its retry/discard gestures, the
per-(producer, reaction, key) sequence guard beside the idempotency claim) runs
over it. Each inbound consumer and the lane-A ingress task run under a
supervisor: a consumer that ends, errors or panics is restarted with bounded
backoff (one log per step, never a hot spin), and past a consecutive-failure
threshold the supervisor lowers readiness with
`REASON_INBOUND_STOPPED` so a dead consumer never leaves the pod deaf while it
reports ready; on shutdown a consumer naks its pulled-but-unprocessed frames so
they redeliver to a live pod. A handler that panics is caught at dispatch, rolled
back and dead-lettered as a terminal frame, so one panicking reaction never
becomes an invisible poison that redelivers every `ack_wait` with readiness UP.
This catch relies on the default `panic = "unwind"`: a build that sets
`panic = "abort"` turns a handler panic into a process abort, so the pod exits
and is rescheduled rather than dead-lettering the one frame — keep the unwinding
profile for a service that wants a single panicking frame parked instead of a
pod restart.
`cx.principal` returns a typed `PrincipalUnresolved` (terminal) rather than
panicking when a message carries no resolvable sender; `cx.try_principal`
returns `None` for the same case, for a reaction that does not need a sender. The
frontier itself refuses a frame with no `br-core-integration` envelope at
`Incoming::identify` (terminal — one dead-letter row and a `Term`), so a reaction
never runs on a frame that carries no sender at all. The `service_engine.dead_letter`
table is the one table for every source of work: inbound reactions, scheduled
messages, cron ticks, a persistently stuck mirror, an outbox row that keeps
failing to publish against a reachable broker, and a render frame that hit a
stored document a projector could not render all record there
(`DeadLetterSource::{Reaction, Scheduled, Cron, Mirror, Outbox, Render}`), each staging an
ops-view impact and incrementing `service_engine_dead_letters_total` by source. A
frame is terminated only once its dead-letter row is durably written: if that
write fails (Postgres unreachable) the frame is nak'ed, not terminated, and the
broker redelivers it until the table can record it, so an accepted message is
never lost to a transient store outage.
`register_mutation` and `register_bulk`: a GraphQL mutation and a
NATS command run **one** direct write pipeline — load, gate (the affordance
function in deny mode), domain command, `save` through the `Persistence` trait,
stage impacts (`cx.impact_caused` / `cx.impact_at` / `cx.impact_all`), stage
outbox rows (`cx.emit` / `cx.command`), commit, respond — under `lock_timeout`
below the consumer's `ack_wait` (a lock timeout is retryable, `nak`). The NATS
command entry adds two rows to that same effect transaction that the GraphQL
entry has no message for: the idempotency claim on the message id and the
per-(producer, reaction, key) sequence guard, so a redelivered command is a
no-op and a reordered one is dropped; a GraphQL mutation carries neither a
message id nor a producer sequence and runs the pipeline once per call by
construction. `ack_wait` and `max_ack_pending` are fields on
`EngineConfig` (defaults 30 s and 256) that build the inbound consumer, and boot
validation refuses a `lock_timeout` that is not strictly below `ack_wait`, so a
pipeline transaction can never still hold its row lock when the consumer
redelivers the frame. The synchronous channel answers `{ success }`, a typed
`MutationError` carrying the gate's `Reason` code, or a typed `OneShot` secret
(which never enters a view, impact, offer or event). `cx.schedule_at` stages a
scheduled reaction the beat fires on the database clock; a scheduled row carries
a per-row `attempts` count and is published isolated from the rest of its batch —
one row claimed per transaction, published through the same ack-timeout seam as
the outbox — so one message that cannot publish against a reachable broker is
dead-lettered past its delivery budget rather than blocking every message behind
it at every beat, and the next due row still fires. A publish failure observed
while the broker is unreachable — or an `Unanswered` publish against a connected
broker whose JetStream cannot answer — costs the row no attempt (the beat skips
`fire_due` while `Nats::reachable()` is false, the same broker-state gate the
outbox relay reads), so a scheduled row waits out an outage or an
unavailable-JetStream window without spending its budget and fires on return. A cron tick that fails is dead-lettered once per slot (it is never
re-run, the slot is already claimed and completed), and a mirror stuck past its
restart threshold records too — all into the one dead-letter table with an
ops-view impact. The engine starts the inbound loop at boot, after
the scope handshake, so a booted engine with registered reactions consumes with
no test-support seam.
All three persistence styles fill the same `Persistence` trait behind the
one-arg `cx.save` / `cx.create`, so one mutation handler runs unchanged over
CRUD, soft EDA (the state row plus an appended fact per change) and full EDA (an
event log whose synchronous projection is a snapshot row, and that snapshot row
is the locked state row). The command's events reach `save` through the default
`Aggregate::pending_events` (`&[]` for CRUD), never through the pipeline. A style
writes the state row (or the events and snapshot) in the one transaction the
pipeline opened and never opens its own, so a foreign-key, unique or
check-constraint failure rolls the state and its events back together. Those
domain constraints live on a CRUD or soft-EDA slice's own state table; the
full-EDA kit's generic `event_snapshot` (jsonb, keyed by noun) carries only the
structural `(noun, key)` primary key and the log the `(noun, key, seq)` one, so a
full-EDA slice enforces uniqueness and integrity in the aggregate's write-time
gate and its hydration barrier, not in a declared FK/unique/check on the state.
Both
reads are non-locking — `read_many` is a plain batched read and `load` derives
from it — so the render side takes no row lock, whatever the author writes. The write
pipeline serialises concurrent commands on one key itself: before `load` it takes
a transaction advisory lock keyed on the aggregate's store type and its
JSON-encoded key — `pg_advisory_xact_lock` over an FNV-1a hash of
`(store type, key)` with a domain tag and a byte separator between the parts, so
two keys or two store types never collide onto one lock — held until the
pipeline transaction commits or rolls back. So concurrent commands on one
aggregate serialize in **every** style even when the store's `Persistence::lock`
is the default no-op. A store that also wants the row itself locked implements
`lock`, in the common case as `Self::row_lock(conn, table, key)` — the defaulted
`SELECT … FOR UPDATE` helper the engine ships, which matches the row on its `id`
column — which pins the row image on top of the advisory lock. That is the one lock domain: every write to an aggregate's rows
goes through the pipeline's `load`/`save`/`delete`, so a raw `SELECT … FOR UPDATE`
or `DELETE` issued outside that path locks rows the pipeline does not know about
and bypasses every policy — a CAS column is a symptom of a second domain, not a
remedy. The advisory lock, like `lock`, runs inside the pipeline transaction under
`lock_timeout`, so a contended write that waits past the timeout is retryable, not
stuck; a render frame never waits on it. `read_many` is required and
has no per-key default, so a render frame or a `load_many` over N keys costs one
statement per call, never N: a CRUD or soft-EDA store reads its table with
`WHERE id = ANY($1)`, and `FullEda<T>` reads every requested snapshot with one
statement and every event logged past those snapshots with a second one, then
replays each key forward. `load` is the one-key case of the same read, so the
mutation path and the render path cannot read a row two different ways; a store
overrides `load` only when a single-key read is genuinely cheaper.

`cx.load_many::<A>(&keys)` loads several aggregates of one noun in the one
pipeline transaction, each locked for the transaction, so a handler can judge
their invariants and commit their writes together with no partial visibility and
**without a global advisory lock of its own**. The keys are deduplicated and then
locked in ascending order of their encoded bytes before any row is read; because
every transaction that touches an overlapping set takes the shared locks in that
one order, two concurrent multi-aggregate writes cannot deadlock. That ascending
`(store type, encoded key)` order is the engine's global aggregate-lock
discipline: to hold aggregates of *different* nouns in one transaction, call
`load`/`load_many` in that same order. Absent keys are omitted from the result,
as for a batched read.

After every `save`/`create`, inside the transaction and before the commit, the
engine runs the **post-save policy** registered for that aggregate, if any
(`register_post_save_policy::<A>`). The policy is pure domain logic over a
`Saved<'_, A>` — `next` is the aggregate the pipeline just saved, `prior` is the
stored image the pipeline loaded (`None` on a create), and `events` are its
pending events. It reads the transition (`saved.transitioned(|a| …)` compares a
projection of `prior` and `next`), can stage impacts, commands and
events, or `PostSave::refuse(reason)` the write — a refusal rolls the transaction
back and answers the mutation with that `Reason` code (a refusing reaction is
dead-lettered with it). It cannot save, so it cannot recurse. This is the engine's
answer to cross-slice wiring that a slice was meant to call and never did: a slice
declares its aggregate *subject* to a policy with `require_post_save_policy::<A>`,
and boot fails with `EngineError::UnhonouredSeam` unless some slice registered one
— the same registration gate the schema type check applies, so a missing interlock
is a loud boot error instead of a silent absent call.

A hard delete goes through `cx.delete(&aggregate)`. The engine runs the
aggregate's **post-delete policy** (`register_post_delete_policy::<A>`, declared
with `require_post_delete_policy::<A>`, over the same `Saved` and `PostSave`), then
issues `Persistence::delete(conn, key)`, then stages the aggregate's offer and blob
reconciliation — a refusal or a delete failure rolls the transaction back like a
save. A store's `delete` defaults to refusing with `EngineError::DeleteUnsupported`,
so a store that never deletes writes nothing; a raw `DELETE` outside `cx.delete`
is the second-lock-domain violation above, not an engine path. `cx.create` refuses
an already-held key under the same advisory lock with `EngineError::KeyReused`
(`KEY_REUSED`), so a creator-generated id is used once and the engine never
overwrites through create.

Full EDA does not hand-roll that log. The `full_eda` kit owns it: a slice
declares an `EventSourced` aggregate (its `NOUN`, `EVENT_VERSION`, the
`SNAPSHOT_EVERY` cadence, `to_snapshot`/`from_snapshot`, `genesis`, `apply`,
`check_hydrated`, `upcast`) and sets `type Store = FullEda<Self>`; the kit does
the rest over the engine's generic `event_log` and `event_snapshot` tables
(keyed by noun, shipped in the reserved migration range). `save` appends the
events with per-key seq arithmetic and, when the cadence boundary is crossed,
rewrites the snapshot — not on every save, so the snapshot lags the log by up to
`SNAPSHOT_EVERY` events. `load` reads the snapshot, replays the events above its
version, runs the aggregate's hydration check as the second barrier, and so
returns the same state whether the snapshot is fresh or lagging. The kit owns the
log's two gestures — upcasting an older event version at read time through the
aggregate's `upcast`, and `full_eda::erase`, which rewrites a person's events in
place (through a redactor the slice supplies) and re-snapshots each touched
aggregate by replaying the whole rewritten log from genesis in the same
transaction — not from the lagging snapshot, so a person folded into a snapshot
past a crossed cadence boundary leaves no residue behind — leaving the log
readable. The rebuild uses the aggregate's `genesis` (its create-time identity
with every event-folded field at zero), which is also the replay-from-scratch
seam. On the engine's own authority, an integrity (SQLSTATE class 23) or
data (class 22) violation raised inside a handler's `cx.save` / `cx.create` is
classified terminal whatever the handler's `Disposition` says, so a coarse
`Retry` cannot nak a constraint violation forever; a raw `cx.connection()` write
stays the handler's to classify. The render-side `read_many` and the
write-side `load`/`save` read one committed store — for full EDA both replay the
same snapshot and log — so a `fetch`, a session `Upsert` and a write-side `load`
return the same committed truth.
`register_accumulator::<A>` opens lane A: at boot the engine binds the
gitops-declared `STREAMING_{service}` stream (bind-only, fail-loud — readiness
stays DOWN if it is absent) and refuses to go UP unless `seal_retention` covers
the stream's `max_age`, so a straggler the stream can still redeliver always
meets a seal marker. A producer (typically an out-of-process runner) publishes
`(key, seq, chunk)` frames on `stream.{service}.{key}`; every pod runs its own
ephemeral consumer that folds each frame into the same Postgres-backed
accumulator as `Engine::push_chunk`, so late joiners replay from the store, the
verified `Ops::seal` writes the final record and a seal marker in one
transaction, and a chunk that arrives after the seal is refused on every pod.
The seal marker's high water is the declared `last_seq`, so a chunk that landed
beyond it fails the seal (`SealChunkBeyondLastSeq`) rather than being dropped
after the producer was told it was durable, and re-sealing a key that already
carries a marker is refused with `AlreadySealed` instead of rewriting the sealed
record. A replay that cannot assemble a contiguous prefix up to `last_seq` fails
with `SealTruncated`; whether that is transient fold-lag worth retrying or a
permanent loss to report is the reaction's call, informed by `cx.delivered` — the
reference reply slice retries a bounded number of deliveries and then answers the
runner with a `SealFailed` event so it resends the finish, never nak-ing forever
into a silent dead letter (a hash mismatch is answered the same way, so the runner
republishes a corrected finish rather than dead-lettering silently). After the commit the
sealing pod purges the key's NATS subject
synchronously; the beat is the backstop that purges any sealed key still in the
stream if the pod died first. Name the stream with `EngineConfig::with_service`;
a serviceless engine that registers an accumulator fails loud at boot
(`AccumulatorWithoutService`) rather than silently folding in process with no
stream to bind. `register_presence` binds the
`EPHEMERAL_{service}` bucket at
boot (bind-only, fail-loud), every pod watches it, and put/expiry reach sessions
as `Upsert`/`Remove` through the same session/render machinery as every other
lane; name the bucket with `EngineConfig::with_service`. With `register_offer`,
a saved or deleted noun that carries an offer stages the offer's dirty key in
the same transaction as the write (`service_engine.offer_dirty`; `cx.delete`
stages the same key so a deleted row retracts), the pod that holds the offer's
single leader lease — one fixed-slot row it renews on the beat, taken over by
another pod only once it expires — drains those keys, claiming them skip-locked,
reading each key's current bucket revision, then resolving the row image in a
short fenced transaction — the revision is observed before the image, so a write
landing between the two bumps the revision and loses the compare-and-set rather
than regressing the bucket — then putting or retracting the published value on the
`PUBLISHED_LANGUAGE` bucket outside any transaction (a `create` for an absent key
or a compare-and-set on that observed revision, a failed set left dirty for the
next drain), and finally raising the per-key watermark and deleting the marker in
a small fenced transaction that asserts the lease — so a concurrent write to an offered noun never waits on the
drain and a leader frozen past its lease fails its writes rather than regressing
the bucket. A `register_offer_trigger::<O, T>` stages the same dirty key from a
*second* aggregate `T` in the offer's slice, so the offer re-publishes when a fact
it derives from changes and not only when its own row does; `T` names the offer
row through `row_key()` (a typed offer-row store key the drain loads) and the
offer's KvKey through `key_from()`, so the trigger must live in the offer's own
slice. It reconciles the
bucket against the store on its first drain after boot and then every
`EngineConfig::with_offer_reconcile` period (re-putting stale keys, retracting
orphans), so a stable leader that never restarts still repairs out-of-band
drift and an emptied or rebuilt bucket is repopulated from the store. The
reconcile cadence is held in the leader's **process memory**, not the store, so a
leader restart or a failover re-runs the reconcile on the new leader's first
drain and then resets its own timer; reconcile is an idempotent repair, not a
scheduled commitment, so an extra reconcile after a takeover is harmless. The
version
lives in the offer's key for a breaking change (register a
second `Offer`). `register_mirror` projects one or more consumed KV offers into
`known_*` through the direct lane, joined by a `keyed_by` function. A consumed
value is a typed `Consumed`, never raw JSON: a mirror that consumes
`serde_json::Value` is refused at registration (`EngineError::RawJsonConsumption`)
unless it sets `Consumed::RAW_JSON_ESCAPE_HATCH`, so the join always reads typed
rows (a typed value may still hold a `serde_json::Value` field). A `known_*` row
is written either declaratively — implement `KnownRow` (`TABLE`, `NAMESPACE`,
`KEY` — the key column names, which a write whose `key()` names other columns is
refused against — and the value columns) and the engine generates the SQL behind
`Projection::upsert` / `Projection::retire` / `Projection::replace_rows`, the
documented path with no SQL in the projector — or manually through `replace_one`
/ `remove` over the `Known` / `KnownScope` traits, the escape hatch for a write
the declarative kit cannot express (`replace_one` stages its impact whether or not
the row changed). A known row's impact key is always its key columns rendered and
joined by `/`. `KnownRow::PRINCIPAL` is required: `None`, or
`Some(PrincipalColumn { column, deps })` when a principal fact loader reads the
table — `column` names a uuid KEY column (anything else is refused,
`EngineError::Config`) — and then every row the kit stages also stages
`PrincipalFactsChanged { principal, deps }` for the principal in that column, so a
change to one membership refreshes that member's facts and no other principal's.
`upsert` stages only when it wrote the row, and `retire` only when it deleted
one. `replace_rows(scope, rows)` replaces the set of rows whose `scope`
columns (equalities, e.g. `vec![col("project_id", id)]`; empty = the whole table)
match: one multi-row upsert per ~30,000 bound values that writes a row only when
a value column differs (`IS DISTINCT FROM`), then one delete of the scoped rows
the call did not name, each returning the keys it touched — so it stages exactly
the rows inserted, changed on any column, or deleted, and nothing for an
unchanged set. It refuses (`EngineError::Config`) a row outside the scope, a null
scope column, two rows with one key and different values (keys compare by typed
column values, never by the `/`-joined impact key, so `("a/b", "c")` and
`("a", "b/c")` are two rows; an exact duplicate is written once), rows that name different value columns, and a key column that is
not a uuid, text, integer or boolean (the impact key of a deleted row is the
column's SQL text, which equals its rendering only for these). A producer that extends a shared type is consumed with
`Extended<Core, Ext>`: the project names its own extension as the second type
parameter and an unknown extension is denied at deserialization, never mirrored
as opaque JSON. A mirror joins one or more offers, and the producer's wire
version is checked two ways. An **engine producer** publishes an
`OfferManifest { prefix, version }` under `manifest_key(prefix)` (a sibling key,
outside the data prefix, `Offer::VERSION`) at every reconcile; the consumer reads
it at scan against `Consumed::manifest()` (`ConsumedManifest::accepts`,
`ManifestMismatch`) and, on a mismatch, dead-letters the whole prefix once
(`DeadLetterSource::Mirror`, keyed `(mirror, prefix)`) and leaves that prefix's
shadow empty; an absent manifest is the pre-0.3 producer case and is applied
silently. Because `manifest_key(prefix)` is a sibling outside the watched data
prefix, no watch event ever carries it, so the manifest verdict is re-evaluated
only at scan — boot and every periodic reconcile — and a producer version bump is
caught within one reconcile deadline, not instantly. For a **non-engine
producer** that carries a per-value version field,
the consumer declares `Consumed::wire_version(&value)`; the engine compares it to
`Consumed::VERSION` at scan and at watch and dead-letters a single mismatched
value (keyed `(mirror, key)`), leaving its shadow row absent while the rest of
the prefix projects. Neither verdict ever touches readiness — content is never a
readiness input.

What `Consumed::PREFIX` names follows the published language's key grammar,
where `/` and `.` both separate segments, and it takes one of two forms. A string
ending in a separator (`catalog/items/`, `catalog.item.`) is a **prefix**: the
mirror reads every key that starts with it, and its manifest key is the prefix
with the trailing separator replaced by `_manifest` (`catalog/items_manifest`,
`catalog.item_manifest`) — a sibling of the data, never under it, so a manifest
is never read as data and no data key is ever taken for a manifest. Any other
string is **one exact key** (`catalog.settings`, `catalog/_meta`), matched by
equality: a key beneath it (`catalog.settings.extra`) or sharing its characters
(`catalog.settingsx`) is not read. An engine offer always publishes a family
under a prefix, so a single key has no manifest and is judged against none;
a single key from a non-engine producer is versioned through `wire_version`.
Registration (`Mirror::validate`) refuses, as `EngineError::Config` naming the
mirror and the string, every form that is ambiguous or malformed: a string
ending in `_` or `-` (a prefix cut mid-segment, which would otherwise read as a
key nobody publishes), a single key ending in `_manifest` (it would read an
offer manifest as data), a leading separator or two separators in a row (an
empty segment), and a character outside `[A-Za-z0-9_./-]`. An offer takes the
same prefix forms — `/` or `.`, never a single key — and an engine refuses two
offers whose prefixes share one manifest key (`x/` and `x.`); across services
the manifest names its prefix, so such a collision reads as an offer mismatch
and dead-letters rather than passing silently. `Mirror::require_key::<C>(key)`
names one key under `C::PREFIX` (or `C::PREFIX` itself, for a single-key
consumption) as **configuration**: the mirror is not ready (`REASON_REQUIRED_KEYS`, `/readyz`
naming the key) until that key is present in the shadow, but nothing is
dead-lettered and no restart budget burns; the mirror stays converged and keeps
projecting. The projection is leader-gated: only the pod holding the mirror lease projects,
standby pods keep their shadows current and take over on lease loss. The mirror
persists a per-bucket watermark — the consumed stream's creation identity and
the last sequence `S` its read reached, committed with the projections in one
transaction — so a standby reports converged only once its shadows are loaded
**and** the leader has committed the boundary the standby's own boot read
captured, and the watch resumes at `S + 1` (not from now), so a put or retract
that lands between the read and the watch is not lost. A boundary of zero has no
history to resume from, so the watch it opens is future-only: the engine re-reads
the bucket's metadata **after** that subscription exists and reconciles if the
bucket gained a sequence, which is what closes the window on the boot where every
consumer's bucket is still empty. A watch that ends under the mirror is named in
the log and reopens all of them, rather than leaving one bucket unwatched until
the next scan. A periodic reconcile on
`EngineConfig::with_mirror_reconcile` repairs drift and reopens the watches from
the boundary it just read.

**Converged means bound, fully read once, and watched from the revision that read
reached — nothing about content.** A consumed prefix that reads empty is a
converged prefix with nothing in it: `known_*` follows the source and is
projected to empty, at boot or during a run, exactly as it follows any other
value. Empty is the normal state of every producer's first deploy, and a
producer in trouble is already visible on its own readiness, so the consumer
never judges the producer's content, never validates the producer's bucket
configuration, and never adds a second 503 to the producer's. Only a read that
fails or does not complete projects nothing: shadows, `known_*` and the
watermark keep their last converged state until the next successful read. A
bucket whose identity changed, or whose sequence is below the held watermark, is
the first-adoption case — a full read, a reconcile, then the new identity and
boundary are adopted — never a readiness failure and never an operator SQL. A
standby reads the leader's watermark the same way: an identity it never read is a
wait and then an adoption, never an error the supervisor would count as a restart,
and a row written before the identity column is read by its sequence alone, so a
rolling upgrade is never stalled behind a leader that can no longer write one.
Reconciling the persisted projection keys against the snapshot (`reconcile_keys`)
is the one behaviour of every scan; on a watch event the engine keys the change
against the shadows on both sides of it, so a retract whose projection key lived
only in the retracted payload still reaches `known_*`.

Scopes are assembled from the slices: each
slice contributes its keys with `engine.contribute_scopes(&[..])`, and
`declare_contributed_scopes` unions them into one `ScopeManifest` and runs
the boot scope-declaration handshake that gates readiness until Identity confirms
(`declare_scopes` remains for a service that assembles the manifest itself).
`register_blobs` records a `BlobPolicy` per blob kind and, at
boot, binds the service's S3-compatible object-storage bucket (bind-only,
fail-loud, never created — configured with `EngineConfig::with_blob_storage`). A
kind whose `BlobPolicy::orphan_after` is shorter than the store's
`BlobConfig::upload_ttl` is refused at bind (`EngineError::BlobOrphanWindowTooShort`,
readiness `blobs.policy`): a shorter orphan window would let the reaper delete an
incomplete pending row while its presigned POST is still valid, so a late upload
would orphan an object no sweep ever sees — set `orphan_after >= upload_ttl`.
`cx.blob::<Kind>(name, content_type)` stages a blob **reference row**
(`service_engine.blob`: reference, object key, kind, content type, file name,
owner, size and state) inside the pipeline transaction, so it commits with the
referencing aggregate and a rollback leaves no row; it returns a typed
`UploadUrl` — an S3 SigV4 **presigned POST** carrying a policy whose
`content-length-range` is `[0, max_bytes]`, so an object over the cap is refused
by object storage at upload and can never land, and whose `Content-Type`
condition pins the reference row's recorded content type, so the object cannot be
uploaded under a different type than the row claims. `cx.blob_verified::<Kind>(name,
content_type, UploadExpectation { size, sha256 })` (and `blob_owned_verified`)
stages a **verified** upload: the POST policy then pins the exact byte count
(`content-length-range [size, size]`) and the SHA-256 checksum
(`x-amz-checksum-sha256`), so object storage refuses any body that is not those
exact bytes and only the intended object can land; `expect.size > max_bytes` is
refused at stage (`EngineError::BlobOverPolicy`) before any presign. Single-part
only (≤ 5 GB); there is no composite/multipart checksum. `BlobConfig` may carry a
`public_endpoint`: the upload POST URL and the download presign are then signed
for that browser-facing host (SigV4 signs `Host`), while the engine's own bucket
HEAD/DELETE stay on the internal in-cluster host. For an **unverified** kind the
presigned POST stays valid for its whole `upload_ttl` even after the reaper
promotes the row, so the object can be **replaced after approval** — a plain
property of an S3 presigned POST, undefended in 0.3.0. For anything
approval-sensitive use a **verified** expectation (the checksum is pinned in the
presign, so no replacement can match) or keep `upload_ttl` short; the verified
path is immune, and nothing here claims an unverified object is immutable. A download `DownloadUrl` — an
S3 SigV4 presigned GET, short-lived, carrying `response-content-disposition` (the
stored file name, `inline` or `attachment` per the requested `Disposition`) and
`response-content-type` (the recorded content type) so the object is served under
its real name and type — is minted only through the gated
`Query::download::<View>(key, reference, disposition)` gesture, never from a bare
reference: a
reference travels in a view by design, so a bare-reference presign would make it a
permanent bearer capability that outlives the row and the viewer. `download`
loads the referencing aggregate through `Query::load_visible::<View>` — the
view's `Visibility` gate, under RLS when the view declares `RLS` — and presigns
only when the principal can see that row **and** the row still lists the
reference in `Aggregate::blob_refs`; a non-viewer, an absent key, or a reference
the named aggregate no longer holds all resolve to `None`. The gate is the
row's visibility, not membership of the view's default window, so a row the
principal sees on any page of a paged view downloads. A resolver therefore mints a download through `Query::download` and never
through a raw presign; the reference reply slice's `exampleReplyDownload(replyId,
reference, disposition)` field is the reference resolver, its `disposition`
argument defaulting to `ATTACHMENT`. That slice's `exampleAttachReply` takes an
optional `expected: UploadExpectationInput { size, sha256Hex }` to stage a
verified upload, and registers a post-upload policy on its `reply_attachment`
kind that impacts the reply view with cause `AttachmentUploaded`. The bytes flow
client-to-storage directly, so `size` is unknown at commit and is recorded from
the object's head when the reaper first sees the upload has completed (promoting
the row to `uploaded` and recording `size`, `etag` and `sha256`), independent of
the orphan window. `engine.blob_reader().head(reference)` reads a live storage
HEAD on demand: `BlobHead` takes `state` and `expected` from the row but `size`,
`etag` and `sha256` **always** from storage, so a mutation running in the pending
window (before the reaper sweeps) sees the storage checksum immediately, with
`state: Pending` and `verified() == Some(true)`. A reference whose object has not
landed yet (`pending`) resolves to **no** download URL; a `failed` row (a verified
upload whose landed object did not match, or one a post-upload policy refused)
resolves to none either. A `pending` row whose object **has** landed is
downloadable **only** when its kind has no post-upload policy **and** the row
carries no upload expectation; a verified row, or a policy-bearing kind, resolves
to **no** download until the reaper has verified the checksum and run the policy
(i.e. until the row is `uploaded`), so a client never gets a URL to bytes the
engine has not yet judged. `UploadUrl`
carries the POST endpoint and its signed form fields (no raw URL string) and
`DownloadUrl` is not `Serialize`, so — like `OneShot` — neither can enter a view,
an impact, an offer, an outbox row or a chunk; only the opaque reference travels.
The beat runs a reaper under a single `reaper:blob` **leader slot** (the cron
idiom — `claim_current_slot` on a pooled connection, `complete_slot` after — so
exactly one pod sweeps per interval; there is no advisory lock, the slot row claim
is the whole guard). Per pending row it runs the storage HEAD **outside** any
transaction, verifies size and checksum against the row's expectation, then
promotes the row in one short `state='pending'`-guarded transaction that also runs
the kind's **post-upload policy** (`register_post_upload_policy::<Kind>` /
`require_post_upload_policy`) — the policy finds its referencing key on the
promotion connection and impacts its own view; its `emit`/`command` go out as the
service actor (correlation = the blob row id). The policy returns `Result<(),
PostUpload>`: a `Refused` (`ps.refuse(reason).into()`) is terminal — it fails the
row (`policy:<code>`) and deletes the object — while a `Fault(EngineError)` (any
infra read propagated with `?`, or a panic) rolls the promotion back, leaves the
row `pending`, counts it in `ReaperRound.failures` (logged with the blob id) and
retries it on the next sweep. There is no attempt budget in 0.3.0: a persistent
fault retries every sweep rather than resolving to `failed`. The promotion
connection carries no
principal and no RLS context, so a policy reading a row over an RLS-scoped table
must query it unscoped. The reaper still deletes an **incomplete
upload** (a `pending` row past `orphan_after` whose object never landed) and an
**unreferenced** or **failed** blob (past `orphan_after`; the object is deleted
first — an idempotent DELETE — then the row, so a crash between the two leaves a
row the next sweep finishes and nothing leaks, though the two steps are not one
transaction); it does not police size, because the POST policy already did, at
upload. Orphan detection happens at
the **aggregate boundary**: `Aggregate::blob_refs` exposes a row's live
references (default empty), and the pipeline diffs them between `load` and `save`
— a dropped or repointed reference, and a `cx.delete`'d aggregate, release the
old blob in the **same transaction** as the write, with no slice-table scanning
and no reference counting. `cx.release_blob(ref)` stays as an explicit escape
hatch. Because release is driven entirely by the `load`/`save` diff, a reference
a slice drops with **raw SQL** — bypassing `cx.delete`, a `load`+`save`, or
`cx.release_blob` — is never observed by the pipeline, so its object is **never
reaped**; a slice that writes its own SQL against a blob-referencing table owns
releasing the blob. A reference is held by **exactly one** aggregate: the
`load`/`save` diff releases it the moment its holder stops listing it, so pointing
two aggregates at one reference is unsupported — the first holder to drop it
orphans the object out from under the second. A blob shared between two nouns is
modelled as two references (two uploads), never one reference held twice. `cx.blob::<Kind>(name, content_type)` records no owner, so its row is
**not** reached by `purge_person_blobs`; a personal file that must be erasable
with its owner MUST be attached with `cx.blob_owned::<Kind>(name, content_type,
person)`. `Engine::purge_person_blobs` is the erase hook `Engine::erase` calls to drop a
person's blobs from storage and the reference table. Presigning uses the sans-IO
`rusty-s3` crate for the GET, an in-engine SigV4 POST-policy signer (`hmac` +
`sha2` + `base64`) for the upload, and `reqwest` (rustls) as the thin HTTP client
for the engine's own bucket HEAD/DELETE — no cloud SDK.

The `graphql` module is the async-graphql surface kit, aligned to the intent's authoring ergonomics. A service lists its slices once with the
`compose_service!` macro, which generates the merged `QueryRoot`/`MutationRoot`/
`SubscriptionRoot` and the `register` function; it composes those roots into one
schema with `engine_schema`, mounts it with `app` (`POST
/graphql` — JSON, or a graphql-sse event stream when `Accept` names
`text/event-stream`, the gateway's subscription leg —, the GraphQL-over-WebSocket
subscription on `GET /graphql/ws`, and `/readyz`; see *Subscription transports*),
and runs both the engine loop and that HTTP server with one call:
`Engine::run_with(app)` (or `run_with_listener(listener, app)` when the caller
pre-binds), which shuts both down gracefully on the engine's shutdown signal.
`serve` and `Engine::run` stay for callers that drive the two lifecycles
themselves. `Engine::graphql_state` wires the executor, the render runtime and
the pool into the schema. Mutation resolvers run on `Engine::mutation_executor`
(`execute` / `ack` and their bulk forms), answering `{ success }` or a typed
error carrying the gate's `Reason` code, and returning a `OneShot`'s inner value
only in the mutation response. Query resolvers take a typed `Query` context and
read rendered views through `cx.fetch::<Projector>(key)` /
`cx.fetch_window::<Projector>(params)`, never the database; a view carrying
`Affordances` round-trips through the rendered store, so the fetch is typed, not
opaque JSON. The subscription is one typed union member per projector: the
`subscription_union!` macro takes a service's `Projector => View` mapping once
and emits the `Reset`/`Upsert`/`Remove` payloads over a typed view union (so a
client subscribing to one projector's field receives only that member's deltas)
with the contiguous revision and the causing event. Each capability builds a
`SliceFragment` with `SliceFragment::derive::<Query, Mutation, Subscription>(slice)`
— its root fields and owned object types are read from the capability's own
`#[Object]`/`#[SimpleObject]` impls through async-graphql's type registry, never
restated in a hand-maintained table — and registers it with
`Engine::register_schema_slice`. A slice may register **several** capability
fragments over one aggregate (a capability file per fragment, all naming the
same aggregate); capabilities of one aggregate legitimately share its owned
types, while two *different* aggregates claiming one type name is a collision.
The engine assembles the registered fragments at the start of `run` (so through
`run_with` too) and fails boot loud with `EngineError::DuplicateSchemaMember`,
naming both aggregates, if two claim the same root field or object type — the pod
never serves an ambiguous schema. The gate does not trust the fragments blindly:
when the service feeds the composed schema's SDL with
`Engine::set_schema_sdl(schema.sdl())` before `run`, the engine parses that SDL
with the real GraphQL parser (so a block-string description that wraps onto a
field-shaped line is never mistaken for a phantom root field) and fails boot with
`EngineError::UndeclaredSchemaMember` for a root field, or
`EngineError::UndeclaredSchemaType` for an object type, that the schema exposes
but no fragment claims — the seam of a capability merged into the composed roots
yet never registered. The engine's own injected object types (the mutation ack,
the lane payloads) are themselves derived from the engine's wrapper types and
allowed unclaimed. A second exempt set is read from the type system too: the
object members of any union that also lists `LanesPaused` and `LanesResumed` —
the reactive delta envelope the `subscription_union!` macro always emits
(`Reset`/`Upsert`/`Remove` payloads) — are derived from that union and both
excluded from a fragment's owned types and allowed unclaimed by the gate, so two
subscription slices sharing one delta union compose with no hand-written slice
claiming the envelope. The completeness gate is then exactly "every object type
that is neither engine-injected nor a reactive delta envelope is owned by a
fragment". The axum layer resolves the principal from the
trusted `X-Passport` header (`PassportPrincipal`) before the executor runs — the
kit does authZ only, never authN. On `POST /graphql` it does so from the headers
alone, **before it reads a byte of the request body**: a request whose passport is
absent, does not decode or is rejected is answered `401` with the body untouched,
whatever its content type, so an unauthenticated client can make the pod neither
buffer nor write anything. The `401` body is the one JSON clients have always
received (`the X-Passport header is absent`, `… is malformed: …`, `the passport is
rejected: …`). Principal facts the pod cannot load are answered `500` `INTERNAL`
(below), also before the body is read.

Multipart requests. `POST /graphql` accepts the GraphQL multipart request
(`operations`, then `map`, then the file parts `map` names) from an authenticated
client only, and reads it under `EngineConfig::multipart` (`MultipartConfig`,
validated at boot):

| Bound | Default | Refusal |
|---|---|---|
| `max_body_bytes` — the whole body; a declared `Content-Length` above it is refused before the body is read, a chunked body is cut when it crosses it | 16 MiB | `413` `MULTIPART_TOO_LARGE` |
| `max_file_bytes` — any single part: a file, `operations`, `map` | 8 MiB | `413` `MULTIPART_FILE_TOO_LARGE` |
| `max_files` — the uploads `map` binds (every path counts, so one file bound to two variables counts twice); judged on `map`, before any file part is spooled — and `map` itself may weigh at most 1 KiB per allowed upload plus 1 KiB, so a padded `map` is refused before it is parsed | 4 | `413` `MULTIPART_TOO_MANY_FILES` |

A body that breaks the spec's order, carries a part `map` does not name, or misses
one it names is `400` `MULTIPART_MALFORMED`, refused before anything of that part
is spooled. A schema that declares no `Upload` scalar accepts no file at all — its
effective `max_files` is `0` — so a multipart request to it carries `operations`
and an empty `map` only and nothing is ever spooled. A schema that declares
`Upload` spools each file part to an anonymous temporary file (no name, freed when
the request ends) in `MultipartConfig::spool_dir`, or in `std::env::temp_dir()`
(`$TMPDIR`, else `/tmp`) when unset; a spool it cannot write is `500`
`MULTIPART_SPOOL_UNAVAILABLE` (the path and the OS error go to the log, never to
the client). Each refusal is a GraphQL-shaped body, `errors[0].extensions.code`
carrying the code (`graphql::MULTIPART_*_CODE`). Whether a schema declares `Upload`
is read once from its SDL (`scalar Upload`), so a field, argument or enum value of
that name does not count. Every other body is streamed to the function
async-graphql-axum's extractor calls, so it is parsed exactly as before (an
unparseable content type is refused before the body is read).

Every authenticated body. Two bounds hold for every content type, JSON included:

| Bound | Default | Refusal |
|---|---|---|
| `MultipartConfig::max_body_bytes` — the whole body, JSON as well as multipart (the name predates its reach); a declared `Content-Length` above it is refused before the body is read, a chunked body is cut on the chunk that crosses it | 16 MiB | `413` `BODY_TOO_LARGE` (`MULTIPART_TOO_LARGE` for a multipart body) |
| `EngineConfig::body_read_timeout` (`with_body_read_timeout`) — from the passport resolving to the last byte of the body; past it the read is abandoned and what was received (buffered bytes, spooled files) is dropped | 30 s | `408` `BODY_READ_TIMEOUT` |

Both refusals are GraphQL-shaped like the multipart ones (`graphql::BODY_TOO_LARGE_CODE`,
`graphql::BODY_READ_TIMEOUT_CODE`). The bounds are per request: how many uploads
may spool at once is not bounded yet. No engine service declares `Upload` today;
the first one that does adds a concurrent-upload limit (a spool semaphore) sized
with its `emptyDir`.

Refusals on the wire. A refusal is a coded GraphQL error, never a transport
error. A mutation refusal is `mutation_error(reason)`; a query or subscription
refusal is `graphql::coded_error(code, message)`, or `graphql::forbidden()` for
the `FORBIDDEN` case — both re-exported at `service_engine::` and carrying the
code in the `code` extension the frontend reads. On a query the client sees HTTP
`200` with `errors[].extensions.code` and a null datum; on a subscription open
the client sees a `next` payload carrying that same error then `complete`, never
a transport-level `error` frame — on the WebSocket the framing is async-graphql's
own, on the event stream the engine frames it the same way.

Failures on the wire. Every failure the engine answers carries a code too. What
the client cannot fix — a database or NATS fault, a transaction that would not
begin or commit, an engine wiring fault, a handler error whose
`MutationFault::reason()` is `None` — reaches it as `INTERNAL`
(`graphql::INTERNAL_CODE`) with the fixed message `internal error`
(`graphql::INTERNAL_MESSAGE`) and no detail; the engine logs the cause at
`error` with its whole `source()` chain where it converts the error. A handler
fault joins its own chain to that log by returning `Some(self)` from
`MutationFault::as_error` (the default logs its `Display` alone). The few
failures a client can act on keep a specific code: paging a session the caller
does not hold or a window it never attached is `NOT_FOUND`, attaching under a
session id a live session holds is `CONFLICT`, attaching for a principal that no
longer exists is `UNAUTHENTICATED`. When the principal's facts cannot be loaded
the request is answered `500` with the same `INTERNAL` error body, not `401`. A
refusal that carries a reason is unchanged: its code and its `mutation refused:`
message. `graphql::internal_error(context, &cause)` gives a service's own
resolver the same shape; the codes, the message and both helpers are
re-exported at `service_engine::` like `coded_error`.

`register_erasable` and `Engine::erase` / `Engine::eraser` are the person-erasure surface. A
slice that holds personal data implements `Erasable::erase(cx, person)`, using
the `Erase` context — the same `Ops` the write pipeline gives a handler — to
delete or anonymize its rows (CRUD deletes, soft EDA also scrubs the person's
value out of the appended fact log, full EDA rewrites the person's events in
place and re-snapshots), stage a Remove impact per touched key
(`cx.impact`/`cx.impact_caused`) and dirty the offers of the rows it erases so
the leader retracts them (`cx.delete` dirties the offer for a deleted row;
`cx.dirty_offer` covers a row anonymised outside the aggregate API). It returns an `Erased` manifest
naming what to purge after the commit: accumulated-lane stream keys
(`purge_stream`), presence keys (`purge_presence`) and un-owned blob references
the person's rows released (`purge_blob`). `engine.erase(person)` runs **every**
registered slice's `erase` in **one** direct-lane transaction. That transaction
sets a transaction-local `app.erasing = 'on'` (through `set_config`, alongside
the write path's `lock_timeout`), so a service that gates its tables with a
strict, deny-when-unset RLS policy erases under the low-privilege app role
(which boot forbids `bypassrls`) by whitelisting that setting —
`current_setting('app.erasing', true) = 'on'` — in the policy; a fail-open
policy simply ignores it. The transaction records the durable erasure fact in
`service_engine.person_erasure`, persists the manifest's stream, presence and
blob keys onto that row, and — on the first erasure only — stages the
`PersonErased` integration event (`integration.evt.{service}.person.erased.v1`)
through the outbox in that same transaction; a failing slice rolls the whole
gesture back. After the commit it purges the manifest's streams (writing/keeping
the stream's `accumulator_seal` marker unpurged and deleting only the Postgres
chunks, so the erased key stays sealed, no straggler re-opens it, and the beat's
seal-purge drops the key's NATS subject), presence keys and
released blobs, and `purge_person`'s owned blobs, then marks the row purged.
That post-commit purge is **durable**: if the pod dies between commit and purge
the row is left unpurged and the **beat drains it** — the same complete-or-drain
path over the persisted manifest — so the purge always finishes. It is
idempotent: because each slice's `erase` is data-driven, a second call finds
nothing, the erasure fact conflicts (no second `PersonErased`), and the outcome
is the same. Other services react to `PersonErased` by erasing their own rows;
`known_*` mirrors and shadows are left untouched and follow the producer's offer
retract, which the slice triggers by dirtying the offers of the rows it erases
(`cx.dirty_offer`) so the leader retracts them within a beat, ahead of the
periodic offer reconcile.
Because it is a runtime gesture, `Engine::run` consumes the engine — capture
`engine.eraser()` before `run` to erase while the pod is serving, exactly as
`mutation_executor` and `blob_reader` are captured.

The authoring ergonomics follow the intent. A projector is
written as a `view::Projector` (re-exported as `service_engine::Projector`) — it
names its `type Noun` and `type Store`, a typed `Query`, its `type Visibility`, and
writes only a native `async fn populate(cx, q)` over a `Populate` context and
`project(row, principal) -> Result<Out, EngineError>`. `project` is fallible: a
stored row it cannot render (a nested blob that will not deserialize) returns
`Err`, and the engine dead-letters that poison with the projector as source at
**every render entry point** — a normal pass, the attach snapshot, a page, and a
repair re-snapshot all record the poison the moment `project` fails, deduped on
projector plus key — and repairs, then ends, the faulted sessions rather than
panicking the pod.
No hand-written future plumbing and no render load SQL live in the view: the engine
loads the noun's rows through the store's `Persistence::read_many` and owns the
`Facts` type, the `LoadScope::{Bulk, PerPrincipal}` match and the derived
`name`/`nouns`/`inverse`; the opaque `WindowParams` never reaches the author, who
works in the typed `Query` through `register_view`, `Query::fetch_view` /
`fetch_view_window`, `WindowSpec::view` and `Bulk::impact_all_view`. `ViewProjector`
is a zero-sized adapter, so a query resolver constructs no per-call state. The
low-level `projector::Projector` stays as the escape hatch for a projector that
joins nouns. `type Visibility` is the third enforcement point of one declaration:
the same cohort rule that `populate` uses through `Visibility::window` is applied
by the engine before it projects (the `visible` method defaults to it), so a row
that leaves the principal's cohorts is delivered as a `Remove` and one that enters
as an `Upsert`. When a principal's own facts change (a membership granted or
revoked, staged with `Ops::impact_principal_facts`), the engine re-resolves the
principal and repopulates every window shape — a `Population::Keys` window
included — so both directions reach a live session then and there.

The **window shape is derived from the declaration**, not hand-picked per
`populate`: `Visibility::LIVE` (the read-side default) makes a cohort view a
live surface, so `view::windowed` and `view::cohort_window` return a
`Population::Query` carrying an `Interest` on the view's noun and
`Visibility::DEPS` — a newly created in-cohort row then reaches an open session
(a `Keys`/`Fixed` window would never deliver it, since a fresh key is not yet a
member) and a membership change repopulates the window. `LIVE = false` is the
explicit override for a closed `Keys` snapshot; returning a `Population` from
`populate` directly bypasses inference. A cohort view reads **only the caller's
rows** through the store's `CohortIndex` seam
(`keys_in_cohorts(conn, &memberships)`) — one indexed query on the cohort
columns, never a table scan filtered in memory. A cohort is a `(dimension,
value)` the service names — `Cohort::uuid("manager", id)`, `Cohort::flag("public",
true)`, `Cohort::text`, `Cohort::int`; the engine hashes it to a `CohortKey` for
routing, and the store never stores the hash. `keys_in_cohorts` binds against the
**natural columns** that hold each row's dimension values: `Cohort::uuids(cohorts,
"manager")`, `Cohort::texts`, and `Cohort::holds(dimension, bool)` extract them so
a store's binding is three lines (`WHERE manager_id = ANY($1) OR $2`). A shadow
`bytea` column is a defect, not a technique; a cohort that would need a per-row
lookup to decide membership is not a cohort.

A projector that filters through Postgres RLS instead of cohorts, or one that is
open to every viewer, declares `type Visibility = Unrestricted<Row, Principal,
Why>` — where `Why` is an `AccessReason` marker (declare it with the
`open_access!` macro) stating why no cohort gate applies. The reason is surfaced
through `Visibility::OPEN_ACCESS_REASON` and **refused non-empty at
registration** (`EngineError::EmptyAccessReason`) — the guard sits in
`register_projector`, the sink every path funnels through (via the
`projector::Projector::open_access_reason` method that `ViewProjector`
overrides), so a hand-built `ViewProjector` handed to `register_projector`
directly is checked exactly like a view registered through `register_view`. So
opting out of the cohort gate is a deliberate, reviewable statement rather than
a silent default. Whether a projector renders under RLS is the **projector's** declaration,
not the call's: `view::Projector` carries `const RLS` (the raw
`projector::Projector` overrides `renders_under_rls`), and the engine reads it on
every path — the snapshot, the render pass, the repair and `Query::fetch*`. So a
fetch and a subscription of the same key by the same principal engage the same
regime and return the same view; there is no per-call switch that could make the
two disagree. A `WindowSpec` is still passed an `rls` flag, but it is a caller
assertion checked against the projector at attach: a flag that contradicts the
declaration is refused with `AttachError::RlsRegimeMismatch`, and an RLS
projector attached with no `RlsApplier` registered is refused with
`AttachError::MissingRlsApplier`.

**Cohorts, per-registration reset threshold and emission on the view surface.**
`view::Projector` carries the three hooks the render pass needs, so "one load per
frame whatever the number of viewers" is reachable without dropping to the raw
`projector::Projector`. `fn cohort(principal) -> CohortKey` (default
`CohortKey::principal(id)`) groups the sessions a frame loads together: sessions
that share a cohort key load the noun's dirty keys once and each is personalised
from that one load; in debug builds the render pass recomputes the cohort key of
every session it grouped and asserts it equals the one the session was grouped
under, so a `cohort()` that is not a pure function of the principal — which would
land a session in more than one cohort and split or merge groups wrongly — panics
in test rather than shipping. The contract a coarser `cohort()` carries is that
its key **fingerprints every fact that `visible` and `project` read off the
principal**: two principals sharing a cohort key are rendered from one load and
must be indistinguishable to both hooks, or the shared render would deliver one
member's view to another. The default per-principal cohort discharges this
trivially; any coarser key is a deliberate assertion that the grouped facts are
the only principal-derived inputs the projector reads. That a shared cohort
renders one view for all its
members is not asserted by re-projection (that would double the very load and
projection the cohort exists to save, and the black-box `s164` proves it directly
by comparing the delivered views); it is guaranteed by the `Visibility`
declaration being total and injective (the collision-free `CohortKey` invariant
below). `const RESET_THRESHOLD: Option<usize>`
overrides the global `reset_threshold` for one projector (a churn beyond it on a
repopulate sends a fresh `Reset` instead of a delta stream); `None` keeps the
global default. `fn emission(&Impact) -> Emission` lets a projector ask for
`PerImpact` delivery (one delta per causing impact, carrying its `cause`) or the
default `Coalesced`; a `PerImpact` projector never faults on a **causeless**
impact (a principal-facts change, a foreign change, a scheduled impact) — those
fold coalesced, because only an impact that carries a cause can be delivered per
impact. A `Coalesced` delta also carries a `cause`: the fold keeps the cause of
the **last** impact it folds (the one latest in the frame that carries one),
so a coalesced view attributes its delta to the most recent causing fact —
causeless impacts in the fold contribute none, and a fold with no cause at all
carries `None`. A `Cause` that does not fit one 8000-byte notification fails the **write**
transaction fail-closed (`TransportError`): the write is refused rather than the
cause silently dropped, so a service that attaches a large payload as a cause
learns at the mutation, not by a viewer missing it — keep a cause to a small fact
and carry bulk in the view.

**Page through history behind a live window.** The kit gesture
`service_engine::page::<P, V>(ctx, session, &cursor)` re-runs the
projector's `populate` with a cursor and **appends** the older keys it returns to
the window the session already holds, delivering the new keys as `Upsert`s on the
contiguous revision — scrolling back never sends a `Reset`. The window is the
live head `populate` filled at attach plus every appended page; a key changes
wherever it sits (an edit to an old row reaches the viewer who holds it), a
`Remove` leaves the window only when the row is deleted or becomes invisible,
never because it fell off a page bound. `window_capacity` bounds the keys a
session may hold across its pages: once appending a page would exceed it the
**oldest appended page is released** — dropped from the window and from the
session's `last_sent` with no `Remove` delta, since the client that asked for
that page drops it too — while the live head is always retained. A paged history
survives a principal-facts refresh and a reconnect `Reset` (still subject to its
own visibility). The gesture is authorized against the caller's `Passport`: the
engine serves only a **live session owned by the calling principal**. Because a
session lives on the pod that holds its socket, a page request must be issued
**over that session's own connection** (a mutation over the same WebSocket lands
on the same pod); a page for a session this pod does not hold — or one held for a
different principal — is refused with `EngineError::NoLiveSession`, so knowing
another session's id buys an attacker nothing. Through the gateway there is no
such connection: the session rides an event stream (see *Subscription
transports*) and the `page` mutation is a separate `POST` the gateway may route
to another replica, where it is refused the same way. Paging a gateway (SSE)
session requires the session's pod (one replica) until 0.4.0 moves paging to
subscription arguments.
The client correlates the two by supplying its own `SessionId`:
`attach_with_session` (kit) / a `session` argument on the subscription pins the
id the `page` mutation then names. The
reference `card` slice demonstrates the pair — `cardPageDeltas(session, boardId,
size)` opens the head window and `pageCards(session, boardId, before, size)`
appends an older page behind it. A page renders its appended keys **outside the
session lock**, so its final delivery — taken back under the lock — skips any
paged key a concurrent render pass has already delivered (its `last_sent` is
present): a key scrolled in while it is being written settles on the committed
view, never a stale page render that lost the race to the pass.

The accumulated lane gained `Ops::seal_partial` and `Ops::seal_current`
so a service can implement the intent's "Cancel work in flight": a direct-lane
cancel decision (with the cancel gate as its affordance, a presence signal the
producer watches and a scheduled deadline), a reaction that seals the producer's
verified partial as cancelled, and a deadline reaction that seals whatever the
stream holds when the producer never answers, treating an already-sealed key as
a lost race (no-op) rather than rewriting the sealed record.

**The frontier contract.** Everything the engine puts on the integration bus —
`cx.emit` / `cx.command`, the scheduled reaction messages, the outbox relay
publish, the dead-letter republish, and the scope-declaration handshake — travels
inside the `br-core-integration` envelope (`IntegrationEvent<T>` /
`IntegrationCommand<T>`: `event_id`/`command_id`, a `{aggregate}.{fact|verb}`
type, the coordinate version, `occurred_at`, and `EventMetadata { actor,
correlation_id, causation_id }`). The actor is the acting principal for a mutation
and the engine's own service identity for a reaction, cron or erasure; the
correlation and causation ids are propagated from the inbound message that caused
the effect (its envelope id is the causation). The inbound loop decodes the
envelope, dedups on its id (the `Br-Message-Id`/envelope id, so a foreign
producer's non-uuid `Nats-Msg-Id` no longer dead-letters), hands the reaction the
inner payload, exposes the metadata on `Reaction` (`cx.metadata`, `cx.actor`), and
resolves the sender's identity into the service's `Principal` through a registered
resolver (`Engine::register_reaction_principal`) so a reaction can gate on who sent
the command — the example's `create_card` reads it with `cx.try_principal()` and
refuses a command whose actor is not a service. `cx.principal()` is the checked
accessor: it returns a typed `PrincipalUnresolved` (terminal disposition) rather
than panicking when no sender principal is resolved. An `OutboundEvent`/`OutboundCommand`
**declares** its producer sequence by overriding `sequence()` — the default is
`None`, and the convention when it is declared is `(producer = service,
seq_key = aggregate key, seq = aggregate version)`. Its `event_id`/`command_id`
identifies exactly one fact or command instance: the same fact re-emitted MUST
carry the same id, because the receiver dedups on it. A declared sequence with no
configured service is refused with a configuration error at emit and recorded as
a terminal violation, so the frame is dead-lettered rather than silently dropped
or redelivered forever; the engine renders a declared sequence
as the three `Br-Producer` / `Br-Seq-Key` /
`Br-Seq` headers and persists it on the outbox row, so engine→engine traffic is
ordered and the per-`(producer, reaction, seq_key)` sequence guard on the receiver
drops a stale message as an acked no-op — a view never walks backwards. The guard
is scoped per reaction, so two reactions consuming different facts of one producer
under one key keep independent watermarks and never drop each other's messages.
The integration outbox is an engine-owned table (`service_engine.integration_outbox`,
in the engine schema like every `service_engine.*` table); a migrating pod adopts any
legacy `public.integration_outbox` rows once (`adopt_legacy_outbox`, an idempotent
post-migration step, not a migration — the fresh-database order would otherwise leave
an orphan) and drops the legacy table.
The hosted outbox relay drains a backlog within one beat (its batch cap equals its
drain bound, so
a full batch signals the beat to come back), and a periodic hygiene pass
(`EngineConfig::with_message_retention`, swept on `HostedOutboxRelay::with_sweep_every`)
deletes rows that reached `PUBLISHED` or a terminal `FAILED` (whose audit copy
already lives in the dead-letter table) and sweeps `message_claim` rows older than
the retention. Because an inbound durable replays from the start of its stream,
the engine refuses at boot a `message_retention` below the `max_age` of any
integration stream a reaction binds (an unlimited `max_age` is refused too), so a
claim is never swept while its message can still be redelivered and re-run; the
sweep itself is best-effort and never lowers readiness. A committed outbox row is
never lost to a broker outage: while `Nats::reachable()` is false the relay skips
its publish pass entirely, so the rows wait (the degrade table's "outbox rows …
wait") and the single beat keeps ticking — heartbeat, cron, scheduled boundaries
and the readiness refresh run every beat and the `nats_grace` probe alone takes
the pod DOWN, never a beat frozen on a full outbox. A publish is bounded by
`PUBLISH_ACK_TIMEOUT` so a send to a just-died broker cannot hang the beat. A
transient failure never counts an attempt while the broker is unreachable — nor
while a connected broker's JetStream cannot answer (a timeout, a broken pipe or
the engine's own ack-timeout, classified `Unanswered`), which the relay halts on
exactly like an outage — so an outage or an unavailable-JetStream window of any
length is retried rather than exhausting a budget; only a genuine broker rejection
(a size or limit refusal, a wrong sequence) is bounded, and at the bound it is
dead-lettered (`DeadLetterSource::Outbox`, with an ops-view impact) in the same
transaction that marks it `FAILED`, never abandoned silently.
`service_engine_outbox_pending` and `service_engine_outbox_oldest_age_seconds`
export the backlog depth and the age of its oldest waiting row. To recover a `KvDrainRelay`'s published-language
bucket that an operator truncated and rebuilt, call `reset_watermarks` and have
the relay's source re-stage its set, so the version guard does not refuse the
unchanged keys the rebuilt bucket lost.

Every command gets exactly one confirmation, and a duplicate re-emits it. The
confirmation a reaction emits is stored on its idempotency claim in the same
transaction as the effect; a command that arrives with an already-claimed id
re-emits that stored confirmation through the outbox (the aggregate does nothing,
the engine replays), so a producer that lost its confirmation past the broker's
duplicate window is answered again without re-running the effect. A command that
reuses an *aggregate* id under a fresh message id is not a claim duplicate — it
reaches the reaction, which decides: the example's `create_card` finds the card
already exists and re-emits `CardReady` rather than colliding on the unique
constraint and dead-lettering a legitimate replay.

A service depends on `br-rust-common` for frontier types (the Passport, the
integration envelope and coordinates, the scope declaration and its handshake, the
shared value types) and for the boot-time primitives it does not re-invent: the
engine keeps its own `connect_pool` / `validate_database_tls` (the secure-by-default
Postgres connect) but re-exports `Readiness` / `ReadinessHandle` / `readiness_route`
from `br-util-axum-readiness` rather than carrying a copy, and `serve` reads the
migration ledger through `br-util-postgres`.

## Writing a service

The example **is** the documentation. `crates/example-service` is a complete,
bootable reference service built only on this crate's public authoring surface —
no `test-support`, no `pub(crate)` reach-around. A handful of 0.3 authoring
gestures the example does not yet exercise — a hard `cx.delete` guarded by
`register_post_delete_policy`, an `OfferTrigger`, `Mirror::require_key`, an
`Inverse::Lookup` join, a `coded_error`/`forbidden` refusal, and branching on the
`Written` mirror verdict — are demonstrated in the conformance sample
(`crates/conformance-service-engine/src/sample/`, e.g. `pipeline.rs`,
`widget_tag.rs`, `linked.rs`, `mirror.rs`, `graphql/forbidden.rs`); copy those for
those idioms until the reference service grows them. Read the example as the
how-to: a thin
`kernel/` (the principal and its generic fact bag, the error base — and no scope
registry, since scopes belong to the slices), one folder per slice under `slices/`
(each owning its aggregate, store, view, handlers, offer/mirror and its GraphQL
capability fragments + its committed `schema.graphql`; a slice over one aggregate
may split its surface into several capability files — the example `card` slice
splits into `graphql/item.rs` and `graphql/board.rs`, each a fragment), a
`slices/mod.rs` that lists the
slices once through the `compose_service!` macro, a `register.rs` and a
`graphql.rs` that are slice-agnostic, and a `src/bin/service.rs` whose `main`
builds `EngineConfig::from_env()?` (adding only its own `with_service` /
`with_blob_storage` on top) and hands it, the composed roots, the service migrator
and `register::all` to the engine boot kit `run_service(BootPlan { .. })` — the one
call that dispatches on argv: `migrate` runs the owner→migrate→grant sequence under
`DATABASE_URL_OWNER`, `serve` refuses an unmigrated store by name then installs JSON
logging + `/livez` + `/metrics` + `/sdl` and serves under `DATABASE_URL`, and
`schema` prints the SDL. `main` reads no engine env var by hand and holds no infra
wiring of its own; it is the reference for how a service boots.

A service that embeds a library crate declares its migrations through
`BootPlan.libraries: Vec<LibraryMigrations>` (`vec![]` when it embeds none). Each
`LibraryMigrations` names the library, the Postgres **schema** it owns, a reserved
version **band** (`RangeInclusive<i64>`, disjoint from the engine's reserved range
and from every other library's), and its `sqlx::migrate!` set. `migrate` applies the
sets in a fixed order — engine, then each library in `Vec` order, then the service —
so a library table exists before a service migration references it; a cross-schema
foreign key from a service table into a library schema is expected and works, since
every service owns its whole database. `migrate` then grants the app role each schema
(engine, every library, public). All sets share the one `_sqlx_migrations` ledger and
run with `ignore_missing`, so sqlx applies any set's unapplied versions regardless of
the highest version already applied: a 0.2 adopter that later adopts a library at a
low band gets those versions applied below `max(applied)` on the next `migrate`, with
nothing to renumber. `libraries::validate` refuses — before any SQL — a schema that
shadows `public` or `service_engine`, a duplicate library, an overlapping band, a
library migration outside its band, or a service migration inside a reserved band;
`serve` re-runs the same validation and refuses a store where any declared set is
still pending, naming the pending library. A library owns **exactly one** schema:
`migrate` snapshots the store's relations around each library's set and fails with
`EngineError::LibraryMigrationEscapedSchema` (naming the library and the object) if
that set created any relation outside its declared schema — a library may not reach
into `public` or another library's schema.
A released engine migration is immutable: sqlx stores the SHA-384 of every applied
file and fails `migrate` with `VersionMismatch` when the embedded file differs. The
engine keeps one exception table, `EDITED_AFTER_RELEASE` (`schema/released.rs`):
the checksum of every released version of an engine migration that was later
edited. Before the engine set runs, under the migrator's own advisory lock and in
one transaction, `migrate` rewrites a stored checksum that appears there to the
current one and logs the version with the old and new checksums at `info`; any
other difference still fails as `VersionMismatch`. Only the engine's own versions
listed there are read or written, so the library and service rows on the shared
ledger are never touched. The one entry today is `9113000023`, whose header comment
v0.3.0 removed after v0.2.0 had applied it. CI
(`.github/scripts/check-released-migrations.sh`) exports every `v*` tag's engine
migrations and fails when a released file is gone or differs from HEAD without its
released checksum registered.
`compose_service!` takes each slice's module, cargo feature and root objects on
**one line** and generates, for the whole set, the `pub mod` declarations, the
`QueryRoot`/`MutationRoot`/`SubscriptionRoot` merged objects and the `register`
function — so `register.rs` calls the generated `slices::register(engine)` and
`graphql.rs` mounts the generated roots, and neither is touched when a slice
comes or goes. Removing a slice deletes its folder and its one line in the
`compose_service!` block; adding one is the reverse.

`compose_service!` takes a **mandatory** `prefix = <snake_ident>;` after
`principal` (a block without it does not compile), and every root field the
service exposes must be `<prefix><UpperName>`: name each resolver method
`<prefix>_<name>` (so `example_board` serves `exampleBoard`), never a bare
`<prefix>` — a root method named exactly the prefix is refused. The engine
validates the ident, derives the lowerCamel prefix once, declares it before any
slice registers, and refuses at boot (`RootPrefixInvalid` /
`RootPrefixUndeclared` / `RootFieldOutsidePrefix`, the pod never serves) any
service that leaves a root field outside its prefix. A service that hand-registers
`SliceFragment`s without `compose_service!` calls
`engine.declare_root_prefix(RootPrefix::from_snake("…")?)` itself before `run`.
The prefix is the only defense against two services claiming one root: the gateway
composer merges a duplicate plain-SDL root field silently and routes it to one
graph rather than refusing it (recorded by the plan's composition probe), so the
engine refuses the collision at its own boot instead of leaving it to compose
away. A library packaged as a slice contributes its roots through the
`slice <name> ["feat"] from <lib>::<macro> { … }` arm — the library macro is
invoked at the host's prefix and principal, writes its aggregate gate once with
the generic `gated! { generics [P: …] ; … }` arm and its delta union once with the
generic `subscription_union! { generics [P: …] ; … }` arm, and reaches `pastey`
through `::service_engine::pastey` without its own dependency. This holds even for a slice
that contributes scopes or a principal fact: the slice declares its scope key and
registers its principal-fact loader from its **own** `register`
(`engine.contribute_scopes(&[..])`, `engine.register_principal_fact(..)`), and the
engine assembles the `ScopeManifest` and fills the principal's facts from the
registered slices — so `board` (which owns `BOARD_ARCHIVE` and the
`BoardMemberships` fact) and `card` (which owns `CARD_ADVANCE`) carry no line in
the kernel, and the kernel names no slice. (Each slice is also a cargo feature —
default = all — which is the mechanism the `removability` CI job uses to compile a
slice out.) The `removability` CI job proves every configuration compiles — the
kernel with every slice removed, then each slice removed in turn. `crates/example-contract` holds what crosses the service
frontier (published types + integration coordinates), and `crates/example-twin`
is the separate producer/runner that closes a real cross-service cycle over NATS —
building the `br-core-integration` envelope exactly as a fabric producer would and
decoding the engine's emitted event back through it, which is what makes the cycle
interop and not just engine↔engine — so the reference service itself never holds a
NATS client. The slices between
them exercise all three lanes, all three persistence styles, offers, mirrors,
presence, blobs, cron, scheduled reactions, bulk writes, `declare_scopes`,
erasure and the full GraphQL surface. `tests/e2e.rs` (split into
`tests/scenarios/`, driven by `tests/harness/`) proves them against real
PostgreSQL, NATS and MinIO over the four observation channels: the mutation
`{ success }`/typed-error ack, the query with its affordances, the KV offers and
integration events, and the `Reset`/`Upsert`/`Remove` subscription deltas driven
over a real `graphql-transport-ws` WebSocket with their typed cause.

### Library slices

`crates/example-lib-roster` is the worked library slice: the roster read-slice
packaged as a standalone crate and embedded by `example-service`, proving the
whole library path — root fields prefixed by the host, value types unchanged,
migrations chained, grants and cross-schema access. The library exports one
`#[macro_export] macro_rules! roster_slice` with the arm
`(prefix = $prefix:ident ; principal = $p:ty)`; `example-service` embeds it with
`slice roster ["roster"] from example_lib_roster::roster_slice { query =
roster::RosterQuery, subscription = roster::RosterSubscription }` and the
`roster = ["dep:example-lib-roster"]` feature. Invoked at the host's prefix and
principal, the macro emits the root objects, their root methods
(`<prefix>_person` → `examplePerson`, `<prefix>_roster_deltas` →
`exampleRosterDeltas`) and the slice's `register`, reaching `pastey` through
`::service_engine::pastey` without its own dependency. The projector
`RosterUsers<P>` is generic over a `RosterPrincipal` bound the host implements in
one line (`impl RosterPrincipal for AppPrincipal {}`), and the delta union is
declared **once**, outside the callback macro, with the generic
`subscription_union! { generics [P: RosterPrincipal] ; … }` arm, so the callback's
subscription resolver only calls `RosterDelta::from_delta::<P>(&delta)`.

The library declares its store through `example_lib_roster::migrations() ->
LibraryMigrations` (schema `roster`, band `9_120_000_001..=9_120_999_999`), which
the host passes in `BootPlan.libraries`; its first migration creates the schema
and the `roster.known_persons` table (`ALTER TABLE IF EXISTS public.known_persons
SET SCHEMA roster` for a store migrated under the old numbering, `CREATE TABLE IF
NOT EXISTS` otherwise), and `migrate` grants the app role the `roster` schema. The
split is a seam: the library owns the read-slice (table, projector, GraphQL,
migrations), and the host wires the **directory mirror** that feeds
`roster.known_persons` and implements `RosterPrincipal`, because which producer
and contract feed the roster is project-specific. Value types stay plain library
types and keep their name in every embed (`RosterView`, `RosterDelta`): the gateway
composer merges identical library types across two subgraphs with no federation
directive, so only root fields are prefixed and there is no type-name prefixing.
The cost of that merge is that two embeds of one library drift silently when
additive and loudly when a field type changes, so a project pins one library
version across all its services and rolls them together.

## Conformance battery

The battery needs real infra: a PostgreSQL admin URL in `E2E_PG_ADMIN_URL`
(fallback `DATABASE_URL`), `nats-server` on `PATH` (it spawns its own broker
per test), and — for the blob scenarios — `minio` on `PATH` (it spawns its own
S3-compatible server per test).

It runs in **two modes**, and every scenario keeps the same assertions in
whichever mode it lives:

- **In-crate mode** (`sNNN_*.rs`) drives the real `service-engine` engine —
  its render pass, inbound loop, write pipeline, impact bus, relays and beat —
  through an in-crate `sample` service, in process. This mode keeps the scenarios
  whose property is **not** observable from outside a running binary because they
  need the `test-support` seam: a **clock the test drives** (the scheduled-impact
  boundary, cron slots, session max-age), **fault injection** (poison and parking
  through a `StubDispatch`, an integrity violation forced terminal, a panicking
  mirror, a lock-timeout, a NATS grace window, a listener/transport reconnect, a
  notify-queue limit, the pooler probe, per-pod lag), and **direct bus/transport
  assertions** (an impact staged atomically with a dead-letter row). These stay
  in-crate on purpose; a black-box binary exposes none of them.

- **Black-box mode** (`bbXX_*.rs`) spawns the real **`example-service` binary**
  — built from the public authoring surface, no `test-support`, no seam — and the
  **`example-twin` binary** for the cross-service cycle, and drives them over
  their public channels only: GraphQL over HTTP and a real `graphql-transport-ws`
  WebSocket, NATS subjects and streams, the published-language KV, Postgres
  state, and `/readyz`. It proves what the slices rely on end to end: the binary
  reaches readiness after declaring its scopes and a second pod shares the store
  (`bb01`); the mutation gate refuses exactly what the affordance forbids
  (`bb02`); a subscriber gets a `Reset` then an `Upsert` with a contiguous
  revision, and a reconnect gets a fresh `Reset` rendered from committed state
  (`bb03`); the twin binary drives a full command→commit→event cycle back over
  NATS (`bb04`); and a streamed reply is sealed against its hash in the running
  binary — the reply-finished command over NATS makes the binary's reaction
  replay the chunks, verify the hash, commit the record and deliver it, read back
  over GraphQL (`bb05`); and the `graphql-transport-ws` socket is closed by the
  binary at `session_max_age` measured from the handshake, so a client that holds
  it open must reconnect with a fresh passport (`bb06`); the gateway's subscription
  transport — `POST /graphql` with `Accept: text/event-stream`, graphql-sse — carries
  the same `Reset` then `Upsert` on a contiguous revision (read with
  `br-test-harness`'s `SseSubscription`, the client service e2e suites use), refuses a
  passport that is absent, undecodable or not a passport with the very `401` a JSON
  request gets, before a byte of the body is read, and completes the stream at
  `session_max_age` then ends it, a new request attaching afresh (`bb16`); and `main`'s one call to
  the boot kit runs `migrate` then `serve`, installs logging, and answers the
  `/livez` + `/metrics` + `/sdl` + `schema` surface (`bb07`); `serve` refuses an
  unmigrated store by name (`bb12`), `migrate` waits for the app role before it
  grants it (`bb13`), and `migrate` refuses an owner role that is neither a
  superuser nor `BYPASSRLS`, exiting non-zero before the first migration (`bb15`);
  the migration chain upgrades a store the v0.2.0 engine set migrated, whose
  `9113000023` checksum v0.3.0 changed, and still refuses an unknown checksum
  (`s241`, in-crate, against the real v0.2.0 files).
  A **multi-pod set**
  boots two instances of the binary against one Postgres and one NATS and proves the
  fleet behaviour §F called untested: a mutation committed on pod A produces the
  delta on a session attached to pod B (`bb08`), a client mid-session survives its
  pod being rolled and reconnects to another for a fresh `Reset` from committed state
  (`bb09`), an outbox row staged on one pod is published exactly once though both pods
  run the relay (`bb10`), and the mirror leader projects while a standby converges to
  readiness from the KV bucket and then takes over the expired lease to project a
  change published after the leader died (`bb11`, which shortens the lease and beat
  through the reference binary's `ENGINE_LEASE_MS` / `ENGINE_BEAT_MS` env). The binaries are taken from `EXAMPLE_SERVICE_BIN` /
  `EXAMPLE_TWIN_BIN` when set (the CI black-box job sets them after building),
  and built on demand otherwise, so the mode is self-sufficient locally. `bb05`
  drives the real lane-A ingress: the `example-twin` binary streams the reply's
  chunks over NATS on `stream.example.{reply_id}`, the running binary's ephemeral
  consumer folds them into `service_engine.accumulator_chunk`, and the
  reply-finished command then replays the chunks, verifies the hash, commits the
  record and delivers it — read back over GraphQL — with nothing seeded through
  Postgres. The accumulator's own internals stay proven in-crate (`s035`, `s036`,
  `s066`, `s146`, `s150`) and by the reference service's reply e2e.

```bash
# both modes (in-crate sNNN + black-box bbNN), one crate
E2E_PG_ADMIN_URL=postgresql://postgres:postgres@localhost:5432/postgres \
  cargo test -p conformance-service-engine --all-targets

# only the black-box mode, against a prebuilt binary
cargo build -p example-service --bin example-service -p example-twin --bin example-twin
EXAMPLE_SERVICE_BIN=target/debug/example-service \
EXAMPLE_TWIN_BIN=target/debug/example-twin \
E2E_PG_ADMIN_URL=postgresql://postgres:postgres@localhost:5432/postgres \
  cargo test -p conformance-service-engine \
    --test bb01_readiness_and_two_pods \
    --test bb02_mutation_gate_and_affordance \
    --test bb03_subscription_reset_and_reconnect \
    --test bb04_cross_service_cycle_twin_binary \
    --test bb05_seal_streamed_reply \
    --test bb06_session_max_age_closes_the_socket \
    --test bb07_boot_kit \
    --test bb08_reconcile_relay_cross_pod \
    --test bb09_rolling_roll_reconnect \
    --test bb10_outbox_published_once_across_pods \
    --test bb11_mirror_leader_standby_failover \
    --test bb12_serve_refuses_an_unmigrated_store \
    --test bb13_migrate_waits_for_the_app_role \
    --test bb14_migrate_needs_only_the_owner_env \
    --test bb15_migrate_refuses_an_owner_subject_to_rls \
    --test bb16_gateway_sse_subscription

# the reference service's own functional spec (same infra, plus MinIO for blobs)
E2E_PG_ADMIN_URL=postgresql://postgres:postgres@localhost:5432/postgres \
  cargo test -p example-service --all-targets -- --test-threads=3
```

CI runs both: the `conformance-service-engine (real infra)` job runs the whole
crate (both modes) on real PostgreSQL, a spawned NATS and MinIO (the in-crate
blob scenarios need it), and a dedicated `conformance-service-engine black-box
(real binary)` job builds the two example binaries and runs only the black-box
scenarios against them on real PostgreSQL and a spawned NATS — no MinIO, since
the example binary boots without S3 (blobs are registered only when configured)
and no black-box scenario exercises a blob.

## Adopter SQL and reads

The engine is not an ORM. A service writes its own SQL — a store's
`read_many`/`save`, a `populate`, a `keys_in_cohorts`, a journal page — and that
is the normal case. The rule for that SQL is bound parameters only: a value from
a request, a principal or a consumed offer is always `.bind(…)`, never formatted
into the statement; the only formatted parts are identifiers the service holds as
`&'static str` constants. Around that SQL the engine asks four things, and gives a
primitive that makes each one the easy path:

| Requirement | Primitive |
|---|---|
| A read over many keys is one statement, never one per key | `Persistence::read_many` is required and has no per-key default, so a store without a batched read does not compile; `load` is derived from it |
| A read of one aggregate for a principal honours the view's declaration | `Query::load_visible::<View>(key)`: the row is loaded under RLS when the view declares `RLS`, then kept only if the view's `visible` (its `Visibility` by default) admits it; a hidden row and an absent key both answer `None`, so a caller cannot probe which keys exist. `Query::download` is built on it. A list is a view (`fetch_view_window`, a subscription) |
| Hand SQL outside a view runs under the second enforcement layer | `Query::read_under_rls(\|conn\| Box::pin(async move { … }))`: a read-only transaction with the principal's RLS context applied through the registered `RlsApplier`, rolled back at the end. A write inside it fails; a fault answers `INTERNAL` with the cause in the log, never the database text; with no `RlsApplier` registered the read is refused (`INTERNAL`), never run without the context |
| A mirror stages only what changed | `Projection::replace_rows(scope, rows)` over `KnownRow` rows: full-row change detection inside a declared scope; a row whose table a principal fact loader reads declares `KnownRow::PRINCIPAL`, and the kit refreshes the facts of exactly the principals whose rows changed (see the mirror kit) |

A keyset page over an append-only journal (`seq > $2 ORDER BY seq LIMIT $3`) or a
context read is the case `read_under_rls` exists for: the SQL filters by the
journal's own key, and the RLS policy filters by the principal, by construction.
A service whose second layer is cohorts rather than RLS (no `RlsApplier`) gates
the parent with `load_visible` and reads the parent's children by the parent's
key, or declares the journal as a view: cohort columns on the fact row and a
`before`-cursor window on the delta subscription.

Mirror tables follow three rules:

- **One mirror owns a table set.** A table that two projections write, or that
  one projection reads to route impacts, belongs to one mirror. When the table
  set has several sources (users, projects, orgs), the mirror consumes them all
  and its key is a sum type — `enum RosterKey { User(Uuid), Project(Uuid),
  Org(Uuid) }` — so every write to the set runs in that mirror's one lead
  transaction, and no hand-written `pg_advisory_xact_lock` serializes two mirrors.
- **Key a multi-source join per row.** `keyed_by` yields the natural scope of the
  rows a change touches (the project whose members changed), and the projection
  writes that scope with `replace_rows`. A `()` key is for a true singleton only.
  Cost, stated plainly: a live change projects only the keys `keyed_by` yields; a
  full rescan (boot, the periodic reconcile, a leader takeover) calls `project`
  once per touched key, all in one transaction, so with `replace_rows` a rescan
  is about two statements per key (the upsert and the delete) — linear in keys,
  constant in rows. A `()` key with a whole-table `replace_rows` makes a rescan
  constant but re-sends every row on every change; in both shapes only the rows
  that changed are impacted.
- **No clock reads in a projection.** A projection (and a principal fact loader)
  is a function of the consumed facts, never of the time it runs. A cohort is a
  written fact: "active" is `end_date IS NULL`, never `end_date > now()`. A
  transition driven by time is a write — `cx.schedule_at` or a cron — that records
  the fact, and the projection reads it.

## Authoring caveats

- A slice's `Visibility` declaration must be **total and injective**: `cohorts`
  and `memberships` return the same cohorts for the same input on every call, and
  two cohorts the projector means to keep distinct must serialise to distinct
  bytes. The engine keys an RLS render group on the exact `PrincipalId` and a
  declared cohort on the exact bytes of its dimension and value, never a 64-bit hash, so it is
  the totality and injectivity of the declaration — not a hash width — that keeps
  two principals, or two distinct cohorts, from ever sharing one render.
- `Persistence::load` and `read_many` must stay non-locking; the engine serialises
  concurrent commands on one key with a per-key transaction advisory lock it takes
  in `load`, so a slice needs no `Persistence::lock` to be correct. `lock` is an
  optional optimisation — a `SELECT … FOR UPDATE` on the row (or snapshot) key that
  also pins the row image — and its default is a no-op; a slice may implement it to
  avoid a re-read under contention, never to obtain serialisation the engine
  already guarantees.

## Deployment constraint

No transaction-mode pooler in front of an engine service: the realtime
transport holds a session-level `LISTEN`, which such a pooler drops silently —
the engine proves the path with a boot probe and holds readiness DOWN when it
fails, so a mispooled service never becomes ready.

Recreate, never a rolling deploy: two versions must never share the store, since
the engine schema and the service migrations move with the version. Boot enforces
this after the posture check — it claims a single-row `service_engine.schema_version`
(engine version, service version, pod, heartbeat) under a per-service advisory
lock. A pod that finds a **different** version whose row is still live (its
heartbeat inside `schema_version_liveness`, default 30s, refreshed by the beat)
refuses to go UP: `Engine::boot` returns `EngineError::SchemaVersionConflict` and
readiness stays DOWN with both versions named in the reason, turning a
mis-configured rolling deploy into a loud failure instead of two versions quietly
sharing one store. A stale row (a pod that died more than `schema_version_liveness`
ago, so its heartbeat lapsed) never blocks — the booting pod claims the row. The
service version comes from `EngineConfig::with_service_version`; the engine version
is the engine crate's own version. If the beat's heartbeat later updates **no**
row — another version has claimed the singleton while this pod ran, so it has been
displaced — the pod lowers readiness to DOWN rather than keep serving over a store
it no longer owns; it recovers when it once again owns the row.

## Ops contract v1 — chart `br-engine-service` 1.x

The engine publishes the shared deployment topology as a Helm **library** chart,
`br-engine-service` (`charts/br-engine-service/`), on its own version line that
starts at `1.0.0`. Chart major 1 **is** ops contract v1; the crate version
appears nowhere in the chart. A per-service **thin** chart depends on the library
and supplies only values — no hand-written topology; the fixture
`charts/br-engine-service/ci/thin-example/` pins that shape. The named templates
(`br-engine-service.deployment`, `.service`, `.serviceaccount`, `.pdb`,
`.networkpolicy`) render the topology from those values, and a thin chart invokes
them from one include-only template. A change to any row in the table below is a
chart **major** shipped under a **new chart name** (`br-engine-service-v2`); the
old chart keeps serving old images. `check-chart-version.sh` refuses a `charts/**`
change without a `Chart.yaml` `version` bump, but it cannot tell a minor from a
contract-breaking major — that rule is the reviewer's to enforce.

Only names the engine reads belong in the contract: `EngineConfig::from_env`
reads the app group in one place, `migrate` reads the owner group, and the rest
of what a pod needs (role and database provisioning, secret material) stays in
GitOps and the NATS fabric.

| Surface | Contract |
|---|---|
| Entry points | `<binary> migrate` (owner role; exits 0 when the engine, library and service sets are current), `<binary> serve` (app role; the default with no argv), `<binary> schema` (prints SDL, reads no env, touches no infra) |
| Owner env — `migrate` only | `DATABASE_URL_OWNER` **strict**: no fallback to `DATABASE_URL`; `APP_ROLE` (the grant target — `migrate` waits until the role exists before granting app access); `TRUSTED_NETWORK_HOSTS` (the owner connect follows the same secure-by-default TLS rule) |
| App env — `serve`, all read by `EngineConfig::from_env` | required: `DATABASE_URL`, `APP_ROLE` (read into the config but only `migrate` acts on it — the grant target; `serve` performs no check against it), `NATS_URL`, `ENGINE_CHANNEL`, `HOSTNAME` (pod identity, from `metadata.name`); with engine defaults: `PORT` (default `8080`) and `HOST` (default `0.0.0.0`) — **not `HTTP_ADDR`**; `RUST_LOG`, `SESSION_TTL_MS`, `SESSION_MAX_AGE_MS`, `ENGINE_LEASE_MS`, `ENGINE_BEAT_MS`, `TRUSTED_NETWORK_HOSTS` |
| Optional S3 group | `S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION`, and optional `S3_PUBLIC_ENDPOINT` — the engine reads none of these; the service `main` reads the group and passes it to `with_blob_storage` (the reference `example-service` requires the first four, defaults `S3_REGION`, and maps `S3_PUBLIC_ENDPOINT` through `with_public_endpoint` when set, else falls through to no blob storage); the library chart emits the five core vars under `objectStore.enabled`, plus `S3_PUBLIC_ENDPOINT` from `objectStore.publicEndpoint` when set (chart 1.1) |
| Derived, never env | `message_retention`: `serve` derives it from the bound streams' `max_age`. No `MESSAGE_RETENTION_*` variable exists |
| Not in the contract | `ENVIRONMENT`: read by nothing in the engine nor in `br-rust-common`; the library chart does not set it; a service that reads it for its own code passes it through `env: []`. `HTTP_ADDR` and `POD_ID` are gone |
| HTTP | one port: `/graphql` (`POST`; JSON, or a graphql-sse stream on `Accept: text/event-stream` — see *Subscription transports*), `/graphql/ws` (`GET`, `graphql-transport-ws`), `/readyz` (200 / 503 + reason), `/livez` (200), `/metrics`, `/sdl` |
| Roll | `Recreate`; `service_engine.schema_version` singleton refuses a second live version |
| Postgres | session mode (LISTEN probe — no transaction pooler); one owner role (`BYPASSRLS` or superuser, `migrate` only — `migrate` asserts it before the first migration and exits non-zero with `EngineError::OwnerSubjectToRls` otherwise) and one app role (runtime, named by `APP_ROLE`); one database per service; `service_engine.*` engine-owned, `integration_outbox` included; one shared `_sqlx_migrations` ledger, every migrator (engine, libraries, service) runs with `ignore_missing`; a library owns its own schema in the service database |
| NATS | `PUBLISHED_LANGUAGE` KV, `STREAMING_{service}` stream, `EPHEMERAL_*` presence buckets; the manifest key per engine offer is `{prefix}_manifest` with the prefix's trailing separator stripped (`typed/v1/` → `typed/v1_manifest`, `typed.v1.` → `typed.v1_manifest`), a sibling outside the data prefix; a single consumed key has no manifest |
| Readiness reasons | the `REASON_*` constants of `engine/boot` and `housekeeping/ready/verdict.rs`, plus `REASON_MIGRATIONS_PENDING` and `REASON_REQUIRED_KEYS` |
| Metrics | `service_engine_*` (`metrics::ALL`) + `service_engine_leader{kind,name}`; common labels `service`, `pod`, `component` |

Filesystem: `migrate` and `serve` write no file, with one opt-in exception — a
service whose schema declares the `Upload` scalar spools the file parts of an
**authenticated** multipart request to `std::env::temp_dir()` (`$TMPDIR`, else
`/tmp`, or `MultipartConfig::spool_dir` when the service sets it). Under a
read-only root filesystem such a service mounts a writable `emptyDir` there, with
a `sizeLimit` sized for its concurrent uploads (each request spools at most
`max_body_bytes`); without it an upload is refused with
`MULTIPART_SPOOL_UNAVAILABLE` and nothing else changes. A service whose schema
declares no `Upload` needs no writable path. `TMPDIR` is read by the standard
library, not by `EngineConfig::from_env`, and is not an ops-contract variable.

Postgres connection strings are read as full DSNs from a Secret
(`DATABASE_URL`, `DATABASE_URL_OWNER`) — the chart never interpolates a password
into a URL, so a role password carrying a URL-reserved character cannot corrupt
the connection string; a role password with a URL-reserved character must never
be interpolated into a DSN. The in-namespace Postgres host is opted out of
`br-util-postgres`'s remote-TLS requirement through
`TRUSTED_NETWORK_HOSTS` (`postgres.trustedNetworkHosts`), a deliberate per-host
plaintext declaration behind the default-deny NetworkPolicy.

Values a thin chart supplies: `image.{repository,tag}`, `port`, `serviceKey`,
`postgres.{appRole,appSecret,ownerSecret,trustedNetworkHosts}`, `nats.url`,
`engine.channel`, `objectStore.enabled` (+ the S3 config and secret ref),
`env: []`, `resources`, `replicaCount`, `topologySpreadEnabled`,
`networkPolicy.{enabled,ingress}`, and from chart 1.1 the neutral fields below.
The library names no namespace; ingress selectors are values. A library
chart's own `values.yaml` lands under `.Values.br-engine-service` of the thin
chart, never at the top level the named templates read, so the library's
`values.yaml` documents the interface and every default is coded in the
templates: a thin chart leaves a key out to get the default.

### Subscription transports

A subscription reaches a pod over one of two transports, both served by `app` on
the one port, both authenticated and bounded the same way:

| Transport | Who uses it | Wire |
|---|---|---|
| `POST /graphql` with `Accept: text/event-stream` | the gateway: its only subscription transport to a subgraph (client ↔ gateway is a WebSocket; gateway ↔ subgraph is HTTP POST + SSE) | graphql-sse, distinct connections: `Content-Type: text/event-stream`, `Cache-Control: no-cache`, one `event: next` per GraphQL response (`data:` the response JSON on one line), one `event: complete` (`data:` empty) at the end, a `:` comment after 15 s without a frame |
| `GET /graphql/ws` | a client that reaches the pod directly (tests, tools) | `graphql-transport-ws` |

The event stream answers the same request as JSON would: the passport is resolved
from the headers first (`401` with the body never read, exactly as for JSON), then
the body is read within its bounds; only the answer differs. Any operation may be
sent this way — a query or a mutation answers one `next`, then `complete`. A
subscription opened on either transport attaches the same engine session, so the
`Reset`/`Upsert`/`Remove` wire, its revision, cohorts and the principal-facts
refresh are identical. Each is bounded by `session_max_age` (from the handshake /
the request) and by the engine's shutdown: the WebSocket is closed `1001`, the
event stream sends `complete` and ends, and the client — the gateway, on the SSE
leg — re-subscribes with a fresh `X-Passport`. A client that goes away releases
the session with its connection. Paging a gateway (SSE) session requires the
session's pod (one replica) until 0.4.0 moves paging to subscription arguments.

### Hardened pod and neutral fields (chart 1.1)

One rule sorts every field: **what the engine binary defines** belongs to the
library, with a default; **what the platform or the cluster defines** does not —
at most a neutral field whose value the service chart sets. The library gives
no label, annotation or secret name a platform meaning.

| Field | Default | Why it is here |
|---|---|---|
| `podSecurityContext` | `runAsNonRoot: true`, `runAsUser`/`runAsGroup`/`fsGroup: 65532`, `seccompProfile: RuntimeDefault` | engine-defined: the binary runs as any non-root UID, and the engine image sets no `USER`, so the UID is explicit |
| `containerSecurityContext` (`migrate` and `serve`) | `allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true`, `capabilities.drop: [ALL]` | engine-defined: `migrate` and `serve` write no file (below) |
| `automountServiceAccountToken` | `false` | engine-defined: the engine never calls the Kubernetes API |
| `probes.{startup,readiness,liveness}` | startup `periodSeconds: 5`, `failureThreshold: 30`; readiness and liveness: kubelet defaults | engine-defined: the boot sequence (below); timing keys only |
| `objectStore.publicEndpoint` | none | ops contract v1: the optional `S3_PUBLIC_ENDPOINT`, emitted when set |
| `migrate.resources` | none | neutral: the init container's own requests/limits (`resources` stays `serve`'s) |
| `deploymentAnnotations`, `podAnnotations`, `podLabels` | none | neutral: a rollout trigger, a scrape hint, a team label are the platform's |
| `service.{labels,annotations}` | none | neutral: a discovery label is the platform's |
| `imagePullSecrets` | none | neutral: registry credentials are the cluster's |
| `nodeSelector`, `tolerations`, `affinity` | none | neutral: scheduling is the cluster's |
| `extraVolumes`, `extraVolumeMounts` (`serve`) | none | neutral: a path the *service's own code* writes, kept writable without turning the read-only root off |

A key set in `podSecurityContext` or `containerSecurityContext` overrides the
default key of the same name, a key set to `null` is removed, and every other
default stays: an image that must run as UID 1000 sets `runAsUser: 1000` and
keeps the rest of the hardening. `podLabels` and `service.labels` add labels
but never replace one of the four library labels (the selector and the chart
identity): trying fails the render. `probes.*` accept only the timing keys
(`initialDelaySeconds`, `periodSeconds`, `timeoutSeconds`, `successThreshold`,
`failureThreshold`); the paths and the port are ops contract v1, and any other
key fails the render.

**Read-only root filesystem — verified.** `example-service migrate` and
`example-service serve` were run against a real Postgres and NATS under a
sandbox that refuses every file write outside `/dev`: `migrate` exits 0,
`serve` boots and answers `/livez`, `/readyz`, `/sdl` and `/metrics`. The
engine opens no file for writing; sqlx migrations are embedded at compile time,
logs go to stdout. The one write path in the dependency tree is
async-graphql's multipart parser, which spools a request's file parts to a
temporary file before the handler runs; the engine schema has no `Upload`
scalar (blobs go straight to the object store through presigned URLs), so under
a read-only root such a request is refused with `400` and the pod carries on.
The library therefore mounts no `emptyDir` — a writable `/tmp` would only give
that pre-authentication spool somewhere to write.

**startupProbe.** `serve` binds its listener last — after the app pool, the
migration check, the NATS connect, `Engine::boot`, registration and the
retention derivation — so `GET /livez` answering is the end of boot. The probe
suspends liveness until then: 5 s × 30 = a 150 s boot budget (`migrate` runs in
the init container, outside it). `/readyz` never gates startup: a healthy pod
can hold it DOWN for long (the Identity scope handshake, a mirror converging),
and a failed startupProbe restarts the container.

**Extension point.** A service chart owns everything that is not the engine's:
it ships its own templates beside the one include-only template, and reuses the
library helpers (`br-engine-service.fullname`, `.labels`, `.selectorLabels`,
`.port`) so its resources select the same pods. A second, labelled Service for
a discovery mechanism is such a template, in the service chart, never in the
library:

```yaml
# service chart: templates/discovery-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: {{ include "br-engine-service.fullname" . }}-discovery
  labels:
    {{- include "br-engine-service.labels" . | nindent 4 }}
    {{ .Values.discoveryLabel.key }}: {{ .Values.discoveryLabel.value | quote }}
spec:
  selector:
    {{- include "br-engine-service.selectorLabels" . | nindent 4 }}
  ports:
    - name: http
      port: {{ include "br-engine-service.port" . }}
      targetPort: http
```

### Release

The chart itself is published to
`oci://ghcr.io/botresources/charts/br-engine-service` by `chart-release.yml` on
the first `main` push that changes `Chart.yaml` `version`, tagged
`chart/br-engine-service/v<version>`, independent of the crate's `v*` tag. The
downstream thin charts, the Warehouse subscriptions on the chart paths and the
library OCI, and the `helm-update-chart` promotion steps live in the deploying
GitOps repository, sequenced after this release.

## Configuration, degradation and observability

`EngineConfig` carries one clock and a handful of bounds, every one validated
at `Engine::boot`: durations and capacities are non-zero,
`listener_queue_threshold` lies in `(0.0, 1.0]`, the `lease` outlasts the
`beat`, `session_max_age` outlasts the idle `session_ttl`, the multipart
bounds are non-zero with `max_file_bytes` within `max_body_bytes`, and
`body_read_timeout` is non-zero. A session lives at most `session_max_age`; when it does
the engine ends it with the same stream-closing signal as a shutdown, so the
client reconnects with a fresh passport — distinct from `session_ttl`, which
reaps a session that has lost its consumer. The bound is on the connection, not
only the session: the WebSocket principal is resolved once at the handshake and
serves every operation on that socket — subscriptions and mutations alike — so
the kit closes the `graphql-transport-ws` connection itself at `session_max_age`
measured from the handshake, with a `1001` going-away close frame the client
recognises as a reconnect. A revoked scope therefore cannot keep an affordance
allowed by keeping the socket open and re-subscribing, and a mutation over the
socket runs under a principal no older than the bound. The client opens a new
upgrade on which the gateway re-injects the resolved `X-Passport`, so the fresh
socket carries the current passport. An event stream on `POST /graphql` — the
gateway's subscription leg — is bounded the same way: its principal is resolved
once for the request, and at `session_max_age` measured from the request (or when
the engine shuts down) it sends `complete` and ends, so the gateway re-subscribes
and re-injects a fresh `X-Passport`.

The listening connection is drained by a task that does nothing else: it
forwards notifications into a bounded in-process channel (`listener_channel_capacity`)
that the render loop consumes, so a slow render pass never stops the drain and
never lets the cluster's notification queue back up behind this pod. When the
render loop cannot keep up and the channel overflows, the drained impacts are
dropped and one `Reconnected` is signalled, which re-snapshots every session on
the pod — a detectable loss, never a silent gap. Under a **sustained** overflow
each overflowing tick re-snapshots, so the pod pays a reset storm: it is bounded
(one `Reconnected` per overflow, coalesced) and visible as
`service_engine_resets_total` climbing — the `ServiceEngineResetsSustained` alert
names it, and the fix is to raise `listener_channel_capacity` or shed render load,
not to touch the drain. The beat samples
`pg_notification_queue_usage()` each tick; past `listener_queue_threshold` it
closes the listener, which takes the pod DOWN, then reconnects and resets its
sessions — losing impacts is repairable, failing every notifying commit on the
cluster is not.

Degradation follows the dependency: Postgres down means nothing serves; a lost
listener holds the pod DOWN until it reconnects and then resets every session;
a NATS blink shorter than `nats_grace` keeps the pod UP, an outage past it
takes the pod DOWN with the reason in the readiness payload and back UP on
reconnect; a consumed bucket missing at boot never comes UP and during a run
takes the pod DOWN at the reconcile deadline.

From the first beat that observes NATS unreachable the accumulated (lane A) and
presence (lane B) lanes pause, and every session that watches a paused lane is
told so on its subscription: the engine emits a `LanesPaused { lanes }` notice —
not past `nats_grace`, which governs readiness alone, but as soon as the outage
is seen, so a presence viewer never mistakes a stalled lane for "nobody is
typing". On return the engine emits a `LanesResumed { lanes }` notice followed by
a `Reset` of the session from the store and the bucket, but only if a pause was
signalled (a sub-beat blink is never observed, so it raises no notice). The notice is out-of-band — the
subscription union exposes `LanesPaused` / `LanesResumed` as members alongside
`Reset` / `Upsert` / `Remove`, but a notice carries **no revision**: it does not
participate in the contiguous per-session revision, so a client that drops it
loses nothing and the `Reset` that follows on return continues the revision
sequence unbroken. A direct-lane subscription is unaffected — mutations still
commit under an outage, so its views keep flowing. `Engine::lane_notices()`
exposes the raw signal to a service or a test; `graphql::lane_notice_stream` and
the union's generated `subscribe(deltas, notices)` merge it into a subscription
(the reference `exampleReplyDeltas` / `exampleTypingDeltas` do this).

The `serve` entry point installs the observability every engine service shares, so
a service `main` never re-adds it by hand (`with_edge_observability` is crate-private
— `serve` is the one door). It reuses the `br-rust-common` crates the engine pins
(`br-util-observability`, `br-util-postgres`): `init_logging` for a structured JSON
tracing subscriber (level from `RUST_LOG`), `init_metrics` for the process-global
Prometheus recorder, and `br-util-postgres` for the app pool and the migration
ledger read. Beside `/readyz` (re-exported from `br-util-axum-readiness`), the kit
mounts `/livez` (always 200, never gated on a dependency), `/metrics` (Prometheus
text — every engine metric already emits against the global recorder, so it is
exported here without extra wiring), and `/sdl` (the composed schema as
`text/plain`); the whole router carries the HTTP metrics layer. The `schema` argv
subcommand prints that same SDL and exits without touching Postgres or NATS, so a
build step can extract the schema from the binary alone. The database follows the
engine's posture rule (the runtime role must not own its schema), and the two
entry points split along it: `migrate` runs the engine, library and service migration
sets on one shared ledger, waits for the app role to exist, and grants it every schema,
all under the **owner** role named by `DATABASE_URL_OWNER` (strict — no fallback to `DATABASE_URL`).
Before its first migration, `migrate` asserts the owner posture
(`engine::boot::assert_owner_posture`): the owner must be a superuser or carry
`BYPASSRLS`, because a data migration run by a role subject to row-level security
touches no row of a `FORCE ROW LEVEL SECURITY` table and still reports success.
An owner without it is refused with `EngineError::OwnerSubjectToRls` naming the
role, logged, and `migrate` exits non-zero with nothing applied;
`serve` connects only the RLS-subject **app** pool named by `DATABASE_URL`, refuses
to run — it logs `REASON_MIGRATIONS_PENDING` and exits non-zero before it binds any
port — while either set is unapplied, and derives `message_retention`
from the bound streams' `max_age`. Role and database provisioning stay in GitOps. Every engine metric is exported labelled
by `service` and `pod` (with the boot kit's `component` global label); each
dependency of the degrade table is a `service_engine_dependency_up` gauge, so a
not-UP state is visible before readiness moves. Every leased loop reports whether
this pod holds its lease through one `service_engine_leader{kind,name}` gauge —
`kind` is `relay`, `cron`, `offer` or `mirror`, `name` the slot — set to `1` on
the holder and `0` on a standby, so `sum by (kind, name)` is `1` where a loop is
led and a failover shows as the gauge moving from the old pod to the new one.
`service_engine_impacts_committed_total` is the notify-budget
counter watched at the Postgres-cluster level; it counts impacts of committed
transactions only, recorded after the commit, never a rolled-back mutation. The
five shipped alerts are in
[`observability/service-engine-alerts.yaml`](observability/service-engine-alerts.yaml):
a filling notification queue, the per-cluster notify budget nearing its ceiling,
a sustained reset rate, an aging outbox backlog, and dead-lettered work waiting on
a human.

## AI disclosure

The code and the documentation of this repository were generated by an AI
system (Anthropic Claude) under the direction and review of BotResources.
BotResources takes full responsibility for them. This disclosure is made in
line with the transparency obligations of the EU Artificial Intelligence Act
(Regulation (EU) 2024/1689).

License: Apache-2.0.
