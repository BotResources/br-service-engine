# Changelog

All notable changes to `br-service-engine` are documented here. The whole
workspace ships **one version**: every crate inherits `version.workspace = true`,
and a single git tag `v{version}` releases the set. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver.

## 0.1.0 - unreleased

Prepared 2026-09-09 (UTC); unreleased — the `v0.1.0` tag is cut when this lands
on `main`. This is the first functional engine release: `service-engine` ships
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
  service version is `EngineConfig::with_service_version`.
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
- `Disposition` (Retry / Park / Terminal) routed against per-reaction budgets;
  `sqlx_is_terminal` classifies a Postgres integrity (SQLSTATE class 23) or data
  (class 22) violation as terminal on the engine's own authority, ahead of the
  handler's disposition, so a coarse `Store(_) => Retry` cannot nak a constraint
  violation forever.
- The `service_engine.dead_letter` table and `DeadLetters` store (`record`,
  `list` by `DeadLetterSource`, `discard`, `retry` — retry re-publishes with a
  fresh dedup token but the original logical id, so the claim and sequence guard
  keep a replay on newer state a no-op). Dead-lettering stages an impact on the
  ops noun in the same transaction, so an ops view updates like any other change.
- The per-(producer, key) sequence guard (`service_engine.sequence_guard`) and
  the idempotency claim (`service_engine.message_claim`), applied inside the
  effect transaction; the claim is keyed `(message_id, reaction)`, so two
  reactions of one service on the same coordinate each run once.

**Direct write pipeline and handler contexts.** A GraphQL mutation and a NATS
command run **one** pipeline: load, gate (the affordance function in deny mode,
so the affordance and the validation are one method), domain command, `save`,
stage impacts, stage outbox rows, commit, respond — under `SET LOCAL
lock_timeout` below the consumer's `ack_wait` (a lock timeout is retryable and
`nak`s). Handler contexts `Reaction`, `Mutation<P>` and `Bulk<P>` over a shared
`Ops`: `cx.load` / `cx.save` / `cx.create`, `cx.impact` / `cx.impact_caused` /
`cx.impact_at`, `cx.command` / `cx.emit`, `cx.seal` / `cx.seal_partial` /
`cx.seal_current`, `cx.schedule_at`, `cx.now`, `cx.blob` / `cx.blob_owned` /
`cx.release_blob` / `cx.delete`. `Mutation<P>` adds `cx.principal` and
`cx.present` (put after commit on the loss-tolerant presence lane); `Bulk<P>`
adds `cx.impact_all` (one projector-reset impact rather than one per key). An
ordinary transaction that dirties more than `impacts_per_commit` keys is refused
and named the bulk path. The synchronous channel answers `{ success }`, a typed
`MutationError` carrying the gate's `Reason` code, or a typed `OneShot` secret
that can only reach the caller — never a view, impact, offer, event or outbox row.
`register_projector` auto-binds the projector's noun to its key type.

**Persistence — CRUD, soft EDA, full EDA behind one trait.** `Persistence`
(`type Aggregate` / `type Key` / `type Event`, `const STYLE`, `load` / `save` /
`create` / `read_many` / `lock`) plus an `Aggregate` trait naming a `Store`, so
`cx.load::<A>` / `cx.save(&a)` / `cx.create(&a)` resolve statically with no
registry, and the pipeline never learns the style. The command's events reach
`save` through the default `Aggregate::pending_events` (`&[]` for CRUD). A style
writes the state row (or snapshot) and its events (or facts) in the one
transaction the pipeline opened, so a foreign-key, unique or check-constraint
failure rolls the state row and its events back together. Full EDA hydrates on
`load` (replay the events above the snapshot, then the aggregate's hydration
check as the second barrier) and owns the log's two gestures — upcasting an
older event version at read time, and erasure (rewrite the person's events in
place and re-snapshot from the rewritten log in the same transaction). `load`
and `read_many` are both non-locking; the write pipeline takes the row lock
itself by calling `Persistence::lock` (default no-op; the reference stores run
`SELECT … FOR UPDATE`) before `load`, inside the transaction under
`lock_timeout`, so concurrent commands on one key serialize in every style while
the render side never takes a row lock. The engine loads a projector's rows
through `Persistence::read_many`, a single batched read of the same committed
table `load`/`save` use, so the render-side load and the write-side load are one
truth (for full EDA the snapshot **is** the state row the projector reads).

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
no TTL, or without delete markers on expiry, since without them an expired key
raises no watch event); each lane's `ttl` must be at least the bucket's
`max_age`. Every pod watches the bucket, folds the latest value per key with no
store write, and delivers `Upsert` on a put and `Remove` on TTL-expiry or clear
through the same session/render machinery. Written with `Engine::present` /
`PresenceHandle<P>` (`cx.present`). Last-write-wins and loss-tolerance hold by
construction.

**Accumulated lane (lane A) and seal.** Streaming accumulators keyed by the
source's own `ChunkSeq` (a checked newtype bounded to the `bigint` range; an
over-range value is refused at construction, never wrapped and treated as a gap):
per-chunk `Durable` receipts, fold-stops-at-a-gap, a table-verified fold cache
bounded by `EngineConfig::fold_cache_capacity` (LRU). A chunk resubmitted at an
already-durable sequence with identical content is idempotent; different content
is a typed `EngineError::ChunkConflict`. `Ops::seal::<A>(key, last_seq, hash)`
replays the stream up to `last_seq`, refuses a truncated prefix
(`EngineError::SealTruncated`) or a hash mismatch (`EngineError::SealHashMismatch`),
writes the seal marker and returns the folded state (`SealHash` = SHA-256 of the
concatenated chunks, hex on the wire); `seal_partial` and `seal_current` are the
cancel gestures. The marker must outlive the stream: boot fails loud unless
`seal_retention` covers the bound stream's `max_age`.

**Leader work — offers and mirrors.** Outbox relays with `RowClaim` and `Leader`
disciplines (a fenced lease over `leader_slot`), the hosted `FabricOutboxRelay`
and `KvDrainRelay` publishing the published language monotonically by key and
version (a per-key watermark in the engine's own schema survives restarts, so a
stale `Put` after a newer `Retract` is a no-op). `Offer` (`Row`, `Published`,
`NAME`, `PREFIX`, `key`, `publish`) and `register_offer::<O>()`: a saved noun
that carries an offer stages its dirty key (`service_engine.offer_dirty`) in the
same transaction as the write; the leader drains dirty keys (re-read, `publish`,
put/retract under a per-key watermark and a bucket-revision compare-and-swap) and
reconciles the bucket against the store on its first drain after boot and every
`with_offer_reconcile` period, so a rebuilt or drifted bucket is repaired even
under a stable leader. `Mirror::new(name).consume::<C>().keyed_by(f).project(p)`
and `register_mirror`: a per-pod typed `Shadow<C>` of each consumed offer, a
full read of every consumed prefix at boot before readiness reports converged,
and leader-gated projection into `known_*` through the direct lane (standbys
keep shadows current and re-project their shadow on takeover, so a change inside
the failover window is never missed). A consumed prefix that reads empty holds
readiness DOWN and keeps `known_*` and the shadows as they are, never projecting
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
transaction, records the durable fact in `service_engine.person_erasure`, runs
every slice's `erase` across all three styles, stages `PersonErased`
(`integration.evt.{service}.person.erased.v1`) through the outbox on the fresh
erase only, and commits (a failing slice rolls the whole gesture back); after the
commit it purges the person's stream subjects, presence keys and blobs. Idempotent.
Other services react to `PersonErased`; `known_*` mirrors follow the producer's
offer retract, never the event.

**GraphQL surface kit (`graphql`).** `compose_service!` lists a service's slices
once (module, cargo feature, root objects on one line) and generates the merged
`QueryRoot`/`MutationRoot`/`SubscriptionRoot` and the `register` function, so
`register.rs` and `graphql.rs` name no slice. `engine_schema` composes the roots;
`app` serves `POST /graphql`, the `graphql-transport-ws` subscription on
`GET /graphql/ws`, and `/readyz`; `run_with` / `run_with_listener` own the engine
loop and the HTTP server in one call. Mutation resolvers run on
`Engine::mutation_executor` (`ack` / `execute` and bulk forms). Query resolvers
take a typed `Query<'_, P>` context and read rendered views through
`cx.fetch::<Projector>` / `cx.fetch_window::<Projector>` (and `fetch_view` /
`fetch_view_window` over the ergonomic `view::Projector`), never the database,
under the same RLS the subscription render applies. The subscription is one typed
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
that joins nouns.

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
outlasting `session_ttl`, `listener_queue_threshold` in `(0.0, 1.0]`) and carries an optional `service`
label and `http_addr`. A session lives at most `session_max_age` (ended with the
stream-closing signal so the client reconnects with a fresh passport, distinct
from `session_ttl`); the WebSocket connection carrying it is closed at the same
bound measured from the handshake, so the bound holds even when a client keeps
the socket open. A `NatsHealth` tracker keeps the pod UP through an outage
shorter than `nats_grace` and DOWN past it. Every metric is labelled by `service`
and `pod`; `impacts_committed_total` is the notify-budget counter, and each
degrade-table dependency is a `dependency_up` gauge. Four alerts ship as a
`PrometheusRule` in `observability/service-engine-alerts.yaml`.

**Postgres schema (reserved range),** applied by `schema::migrate`
(`ignore_missing`) with `grant_engine_access`: `scheduled_impact`, `leader_slot`,
`accumulator_chunk`, `accumulator_seal`, `kv_relay_watermark`, `message_claim`,
`sequence_guard`, `dead_letter`, `scheduled_message`, `offer_dirty`, `blob`,
`person_erasure`, `schema_version`. Scheduled boundaries are claimed against the database clock,
never the pod clock. The app-role grant includes `USAGE, SELECT` on the engine
schema's sequences.

**`conformance-service-engine`.** The battery runs in **two modes** against real
infra (a fresh database and a spawned `nats-server` per test, plus a spawned
`minio` for the blob scenarios). **In-crate mode** — the named scenarios
`s001`–`s131` — drives the real engine through an in-crate `sample` service and
keeps the properties that need the `test-support` seam (a driven clock, fault
injection, direct impact-bus/transport assertions): shared-consumer ownership
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
fail-closed through the running engine (`s131`). **Black-box mode** — `bb01`–`bb05`
— spawns the real `example-service` binary (and the `example-twin` binary for the
cross-service cycle) and drives them over their public channels only (GraphQL
over HTTP and `graphql-transport-ws`, NATS subjects and streams, the
published-language KV, Postgres state, `/readyz`): readiness plus the boot scope
handshake plus a second pod on the same store (`bb01`); the affordance==gate
identity (`bb02`); `Reset`→`Upsert` with a contiguous revision and a reconnect
`Reset` from committed state (`bb03`); a full cross-service cycle driven by the
spawned twin binary (`bb04`); and seal — a streamed reply sealed against its hash
inside the running binary (`bb05`). Because the 0.1.0 accumulated lane stores
chunks in Postgres (`service_engine.accumulator_chunk`) and exposes no
NATS/GraphQL chunk-ingress, `bb05` seeds the chunks through Postgres — the flush
path's own table shape, a listed black-box channel — while everything the seal
*is* (replay, hash verification, the transactional final write, the impact and
delivery) runs in the spawned binary; the accumulator's own internals stay proven
in-crate. The black-box harness provisions the owner/app Postgres roles and the
engine + example migrations, provisions NATS, seeds the roster and answers the
scope declaration, then spawns the binary and gates on `/readyz`, taking the
binaries from `EXAMPLE_SERVICE_BIN` / `EXAMPLE_TWIN_BIN` when set and building
them on demand otherwise. `infra/pg.rs` / `infra/nats.rs` are the sole
`async-nats` user, only to declare gitops-owned streams and buckets.

- CI runs the battery in both modes: the `conformance-service-engine (real
  infra)` job runs the whole crate (both modes, needing MinIO for the in-crate
  blob scenarios), and a dedicated `conformance-service-engine black-box (real
  binary)` job builds the two example binaries and runs only `bb01`–`bb05`
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
(accumulated lane + verified seal, cancel-in-flight, presence, blob attachment)
and `roster` (a KV mirror into `known_persons`). `example-twin` is a **separate**
crate (the producer/runner that closes the cross-service cycle over NATS), so the
reference service holds no NATS client. `tests/e2e.rs` (split into
`tests/scenarios/`, driven by `tests/harness/`) proves the slices against real
PostgreSQL, NATS and MinIO over the four observation channels — including the
subscription deltas over a real `graphql-transport-ws` WebSocket with their typed
cause — and two-pod convergence.

### Changed

- **The write-path row lock lives in `Persistence::lock`, not in `load`.** `load`
  is a plain, non-locking read and `read_many` (the batched render read) defaults
  to it, so both reads are lock-free and a store author who writes only `load`
  gets a render frame that never takes a row lock. The write pipeline calls
  `Persistence::lock` (default no-op) before `load` to take the row lock; the
  reference stores implement it as `SELECT … FOR UPDATE` on the row (or snapshot)
  key, so concurrent commands on one key serialize in every style — inside the
  pipeline transaction, under `lock_timeout`, so a contended write is retryable —
  while a render never waits on that lock. A store that wants a single batched
  render query overrides `read_many` (`WHERE id = ANY($1)`) per the "every read
  function answers in one query" rule; the reference stores do.

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
