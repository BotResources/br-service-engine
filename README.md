# br-service-engine

The reactive personalized delivery and process skeleton every
[BotResources](https://botresources.ai) service runs on — sessions, cohorts,
impacts, projection, diff, multi-pod fan-out, streaming sources, boot, relays,
cron, and mirror supervision. Two crates share one workspace version: the
`service-engine` library a service builds on, and `conformance-service-engine`,
its black-box conformance battery run against real PostgreSQL and NATS
JetStream. Not published on crates.io and shipped as no image and no CLI: the
git tag is the release.

## Install

A service depends on the `service-engine` library only. `conformance-service-engine`
is not a kit to import: it is this repository's own executable spec and lives here.

```toml
[dependencies]
service-engine = { git = "https://github.com/BotResources/br-service-engine", package = "service-engine", tag = "v0.1.0", version = "0.1.0" }
```

The `version` beside the `tag` is required: a tag-only git dependency carries a
`*` version requirement, which `cargo-deny`'s `wildcards = "deny"` rejects. The
engine has its own version line and depends on `br-rust-common` alone; each
engine minor pins one exact `br-rust-common` tag.

| Engine version | `br-rust-common` |
|---|---|
| 0.1.0 | `v1.3.0` |

## Module map

The engine is one crate laid out one module per capability. The multi-pod
delivery core (`transport`, `render`, `session`, `cohort`, `population`,
`accumulator`, `housekeeping`, `relays`, `mirror`, `principal`, `projector`) and
every author-facing surface in the table below are implemented and
battery-backed.

| Module | Responsibility |
|---|---|
| `engine` | `Engine::boot` and the `register_*` / `contribute_scopes` / `declare_scopes` / `register_principal_fact` / `erase` surface |
| `nats` | Engine-owned NATS: stream/bucket bind, KV read/write/watch, outbox publish |
| `inbound` | Inbound NATS loop: durable consumer, poison/dead-letter, `Disposition` |
| `pipeline` | Direct write pipeline; `Mutation` / `Reaction` / `Bulk` contexts; `OneShot` |
| `persistence` | `Persistence` trait + `Aggregate`; CRUD, soft-EDA and full-EDA behind one trait; `load`/`save`/`create`, a non-locking `read_many` (the batched render read; defaults to `load` and both are non-locking, so an author who writes only `load` gets a lock-free render), and a `lock` the write pipeline calls before `load` to take the row lock (default no-op; the reference stores implement it as `SELECT … FOR UPDATE`); log-style events reach `save` via `Aggregate::pending_events` |
| `gate`, `visibility` | `Gate`/`Reason`, `Affordances`, the `gated!` macro and `check_gates_match_affordances` (affordance == mutation check, one function); `Visibility` cohorts/memberships deriving the `visible` filter and the `window` membership from one declaration, with `check_window_matches_visibility` |
| `presence` | Presence lane: `EPHEMERAL_*` bucket, `register_presence`, `cx.present` |
| `offer` | `Offer` trait, `register_offer`, leader-drained dirty keys, versioned watermark, boot + periodic reconcile |
| `mirror` | `register_mirror` over the direct KV watch into `known_*`, leader-gated projection |
| `blobs` | Object-storage references, `register_blobs`, presigned URLs, reaper |
| `scopes` | scopes assembled from the slices' `contribute_scopes` (`declare_contributed_scopes`); the `declare_scopes` handshake gates readiness |
| `erase` | `Erasable` and `engine.erase(person)` (person-erasure only) |
| `dyn_compat` | Type-erasure wrappers behind the registries (`ErasedProjector`/`ErasedAccumulator` and their adapters) |
| `view` | ergonomic projector surface: a `Projector` declares `type Noun`/`type Store`, a typed `Query`, `type Visibility`, `async fn populate(cx, q)` and `project(row, principal)`; the engine loads the noun's rows through `Persistence::read_many`, applies the projector's `visible` gate (defaulting to the `Visibility` declaration) before projecting so a row that leaves the principal's cohorts becomes a `Remove`, and `ViewProjector` owns `Facts`, the `LoadScope` match and derives `name`/`nouns`/`inverse`. The low-level `projector::Projector` is the join escape hatch |
| `readiness` | the engine's own `Readiness`/`ReadinessHandle` and `/readyz` route (no `br-util-axum-readiness`) |
| `db` | `connect_pool` + `validate_database_tls`: the engine's own pooled Postgres connect, secure-by-default (remote hosts need TLS; `TRUSTED_NETWORK_HOSTS` is the per-host opt-out) |
| `graphql` | async-graphql kit; `compose_service!` (one line per slice generates the merged roots + `register`), `run_with` boot, typed `Query` context (`fetch_view` / `fetch_view_window` over a typed `Query`), per-projector typed subscription union, per-slice SDL assembly checked against the composed schema at boot; each slice's SDL fragment is emitted as a committed `schema.graphql` |

No `register_*` method or engine gesture returns `EngineError::NotYet`; every
author-facing surface is implemented.
`register_reaction` records a reaction and
derives its inbound subscription, and the engine-owned inbound loop (durable
consumer, ack-after-durable, `Disposition` routing, poison budget with the
`service_engine.dead_letter` table and its retry/discard gestures, the
per-(producer, key) sequence guard beside the idempotency claim) runs over it.
`register_mutation` and `register_bulk`: a GraphQL mutation and a
NATS command run **one** direct write pipeline — load, gate (the affordance
function in deny mode), domain command, `save` through the `Persistence` trait,
stage impacts (`cx.impact_caused` / `cx.impact_at` / `cx.impact_all`), stage
outbox rows (`cx.emit` / `cx.command`), commit, respond — under `lock_timeout`
below the consumer's `ack_wait` (a lock timeout is retryable, `nak`), with the
idempotency claim and the per-(producer, key) sequence guard in the effect
transaction. The synchronous channel answers `{ success }`, a typed
`MutationError` carrying the gate's `Reason` code, or a typed `OneShot` secret
(which never enters a view, impact, offer or event). `cx.schedule_at` stages a
scheduled reaction the beat fires on the database clock; dead-lettering stages
an impact on the ops view. The engine starts the inbound loop at boot, after
the scope handshake, so a booted engine with registered reactions consumes with
no test-support seam.
All three persistence styles fill the same `Persistence` trait behind the
one-arg `cx.save` / `cx.create`, so one mutation handler runs unchanged over
CRUD, soft EDA (the state row plus an appended fact per change) and full EDA (an
event log plus a synchronous snapshot that is the locked state row). The
command's events reach `save` through the default `Aggregate::pending_events`
(`&[]` for CRUD), never through the pipeline. A style writes the state row (or
snapshot) and its events (or facts) in the one transaction the pipeline opened
and never opens its own, so a foreign-key, unique or check-constraint failure on
either table rolls the state row and its events back together. Both
reads are non-locking — `load` is a plain read and `read_many` defaults to it —
so the render side takes no row lock, whatever the author writes. The write
pipeline takes the row lock itself: before `load` it calls the store's
`Persistence::lock`, which the reference stores implement as
`SELECT … FOR UPDATE` on the row (or snapshot) key, so concurrent commands on
one key serialize in every style while a render frame never waits on that lock.
`lock` runs inside the pipeline transaction under `lock_timeout`, so a contended
write is retryable, not stuck. The engine loads a projector's rows through the
store's `Persistence::read_many`, a single batched read of the same committed
table `load` reads, so the author writes no render load SQL in the view. Full EDA hydrates
on `load` by replaying the events above the snapshot and running the aggregate's
hydration check as the second barrier, and owns the log's two gestures —
upcasting an older event version at read time, and erasure, which rewrites a
person's events in place and re-snapshots from the rewritten log in the same
transaction. On the engine's own authority, an integrity (SQLSTATE class 23) or
data (class 22) violation raised inside a handler's `cx.save` / `cx.create` is
classified terminal whatever the handler's `Disposition` says, so a coarse
`Retry` cannot nak a constraint violation forever; a raw `cx.connection()` write
stays the handler's to classify. The render-side `read_many` and the
write-side `load`/`save` read one committed store — for full EDA the snapshot is
the state row the projector reads — so a `fetch`, a session `Upsert` and a
write-side `load` return the same committed truth.
`register_presence` binds the `EPHEMERAL_{service}` bucket at
boot (bind-only, fail-loud), every pod watches it, and put/expiry reach sessions
as `Upsert`/`Remove` through the same session/render machinery as every other
lane; name the bucket with `EngineConfig::with_service`. With `register_offer`,
a saved noun that carries an offer stages the offer's dirty key in
the same transaction as the write (`service_engine.offer_dirty`), the pod that
holds the offer's leader lease drains those keys — re-reading the row, then
putting or retracting the published value on the `PUBLISHED_LANGUAGE` bucket
under a per-key watermark and a revision compare-and-swap — and reconciles the
bucket against the store on its first drain after boot and then every
`EngineConfig::with_offer_reconcile` period (re-putting stale keys, retracting
orphans), so a stable leader that never restarts still repairs out-of-band
drift; the version lives in the offer's key for a breaking change (register a
second `Offer`). `register_mirror` projects a consumed KV offer into
`known_*` through the direct lane, and its projection is leader-gated:
only the pod holding the mirror lease projects, standby pods keep their shadows
current and take over on lease loss. Scopes are assembled from the slices: each
slice contributes its keys with `engine.contribute_scopes(&[..])`, and
`declare_contributed_scopes` unions them into one `ScopeManifest` and runs
the boot scope-declaration handshake that gates readiness until Identity confirms
(`declare_scopes` remains for a service that assembles the manifest itself).
`register_blobs` records a `BlobPolicy` per blob kind and, at
boot, binds the service's S3-compatible object-storage bucket (bind-only,
fail-loud, never created — configured with `EngineConfig::with_blob_storage`).
`cx.blob::<Kind>(name, content_type)` stages a blob **reference row**
(`service_engine.blob`: reference, object key, kind, content type, file name,
owner, size and state) inside the pipeline transaction, so it commits with the
referencing aggregate and a rollback leaves no row; it returns a typed
`UploadUrl` — an S3 SigV4 **presigned POST** carrying a policy whose
`content-length-range` is `[0, max_bytes]`, so an object over the cap is refused
by object storage at upload and can never land. A download `DownloadUrl` — an S3
SigV4 presigned GET, short-lived — is minted only through the gated
`Query::download::<View>(key, reference)` gesture, never from a bare reference: a
reference travels in a view by design, so a bare-reference presign would make it a
permanent bearer capability that outlives the row and the viewer. `download`
takes the same visibility path as `fetch` — it presigns only when the caller can
currently see the referencing view (`populate` → membership) **and** the loaded
referencing aggregate still lists the reference in `Aggregate::blob_refs`; a
non-viewer, or a reference the named aggregate no longer holds, resolves to
`None`. A resolver therefore mints a download through `Query::download` and never
through a raw presign; the reference reply slice's `replyDownload(replyId,
reference)` field is the reference resolver. The bytes flow
client-to-storage directly, so `size` is unknown at commit and is recorded from
the object's head when the reaper first sees the upload has completed (promoting
the row to `uploaded`), independent of the orphan window. A reference whose
object has not landed yet (`pending`) resolves to **no** download URL. `UploadUrl`
carries the POST endpoint and its signed form fields (no raw URL string) and
`DownloadUrl` is not `Serialize`, so — like `OneShot` — neither can enter a view,
an impact, an offer, an outbox row or a chunk; only the opaque reference travels.
The beat runs a reaper whose scope is exactly the intent's two categories: it
deletes an **incomplete upload** (a `pending` row past `orphan_after` whose object
never landed) and an **unreferenced** blob (a reference released past
`orphan_after`, whose object it also deletes from storage); it does not police
size, because the POST policy already did, at upload. Orphan detection happens at
the **aggregate boundary**: `Aggregate::blob_refs` exposes a row's live
references (default empty), and the pipeline diffs them between `load` and `save`
— a dropped or repointed reference, and a `cx.delete`'d aggregate, release the
old blob in the **same transaction** as the write, with no slice-table scanning
and no reference counting. `cx.release_blob(ref)` stays as an explicit escape
hatch. Because release is driven entirely by the `load`/`save` diff, a reference
a slice drops with **raw SQL** — bypassing `cx.delete`, a `load`+`save`, or
`cx.release_blob` — is never observed by the pipeline, so its object is **never
reaped**; a slice that writes its own SQL against a blob-referencing table owns
releasing the blob. `cx.blob::<Kind>(name, content_type)` records no owner, so its row is
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
/graphql`, the GraphQL-over-WebSocket subscription on `GET /graphql/ws`, and
`/readyz`), and runs both the engine loop and that HTTP server with one call:
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
with the contiguous revision and the causing event. Each slice declares its
root fields and types as a `SliceFragment` and registers it with
`Engine::register_schema_slice`; the engine assembles the registered fragments
at the start of `run` (so through `run_with` too) and fails boot loud with
`EngineError::DuplicateSchemaMember`, naming both slices, if two claim the same
root field or GraphQL type — the pod never serves an ambiguous schema. The gate
does not trust the declarations blindly: when the service feeds the composed
schema's SDL with `Engine::set_schema_sdl(schema.sdl())` before `run`, the engine
derives the actual root fields from the async-graphql schema and fails boot with
`EngineError::UndeclaredSchemaMember` if the schema exposes a root field no slice
declared, so an under-declared fragment cannot leave a real root field outside
the collision gate. (Object *types* keep the declared gate only: the engine
injects payload, union and scalar types no slice owns, so the schema's type set
is not slice-only.) The axum layer resolves the principal from the
trusted `X-Passport` header (`PassportPrincipal`) before the executor runs — the
kit does authZ only, never authN.

`register_erasable` and `Engine::erase` / `Engine::eraser` are the person-erasure surface. A
slice that holds personal data implements `Erasable::erase(cx, person)`, using
the `Erase` context — the same `Ops` the write pipeline gives a handler — to
delete or anonymize its rows (CRUD deletes, soft EDA also scrubs the person's
value out of the appended fact log, full EDA rewrites the person's events in
place and re-snapshots), stage a Remove impact per touched key
(`cx.impact`/`cx.impact_caused`) and dirty the offers of the rows it erases
(`cx.dirty_offer`, so the leader retracts them). It returns an `Erased` manifest
naming what to purge after the commit: accumulated-lane stream keys
(`purge_stream`), presence keys (`purge_presence`) and un-owned blob references
the person's rows released (`purge_blob`). `engine.erase(person)` runs **every**
registered slice's `erase` in **one** direct-lane transaction, records the
durable erasure fact in `service_engine.person_erasure`, and — on the first
erasure only — stages the `PersonErased` integration event
(`integration.evt.{service}.person.erased.v1`) through the outbox in that same
transaction; a failing slice rolls the whole gesture back. After the commit it
purges the manifest's stream subjects, presence keys and released blobs, and
calls `purge_person_blobs` for the person's owned blobs. It is idempotent:
because each slice's `erase` is data-driven, a second call finds nothing, the
erasure fact conflicts (no second `PersonErased`), and the outcome is the same.
Other services react to `PersonErased` by erasing their own rows; `known_*`
mirrors and shadows are left untouched and follow the producer's offer retract.
Because it is a runtime gesture, `Engine::run` consumes the engine — capture
`engine.eraser()` before `run` to erase while the pod is serving, exactly as
`mutation_executor` and `blob_reader` are captured.

The authoring ergonomics follow the intent. A projector is
written as a `view::Projector` (re-exported as `service_engine::Projector`) — it
names its `type Noun` and `type Store`, a typed `Query`, its `type Visibility`, and
writes only a native `async fn populate(cx, q)` over a `Populate` context and
`project(row, principal)`.
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
included — so both directions reach a live session then and there. A projector
that filters through Postgres RLS instead of cohorts, or one that is open to every
viewer, declares `type Visibility = Unrestricted<Row, Principal>`, a conscious
"no cohort gate here" that keeps the render-time gate off rather than defaulting
it open. The accumulated lane gained `Ops::seal_partial` and `Ops::seal_current`
so a service can implement the intent's "Cancel work in flight": a direct-lane
cancel decision (with the cancel gate as its affordance, a presence signal the
producer watches and a scheduled deadline), a reaction that seals the producer's
verified partial as cancelled, and a deadline reaction that seals whatever the
stream holds when the producer never answers.

A service depends on `br-rust-common` only for frontier types: the
engine provides its own `connect_pool` / `validate_database_tls` (the
secure-by-default Postgres connect) and its own `Readiness` / `ReadinessHandle` /
`readiness_route`, so `br-util-postgres` and `br-util-axum-readiness` are gone from
the engine, the example and the battery.

## Writing a service

The example **is** the documentation. `crates/example-service` is a complete,
bootable reference service built only on this crate's public authoring surface —
no `test-support`, no `pub(crate)` reach-around. Read it as the how-to: a thin
`kernel/` (the principal and its generic fact bag, the error base — and no scope
registry, since scopes belong to the slices), one folder per slice under `slices/`
(each owning its aggregate, store, view, handlers, offer/mirror and SDL
fragment + its committed `schema.graphql`), a `slices/mod.rs` that lists the
slices once through the `compose_service!` macro, a `register.rs` and a
`graphql.rs` that are slice-agnostic, and a `src/bin/service.rs` that boots.
`compose_service!` takes each slice's module, cargo feature and root objects on
**one line** and generates, for the whole set, the `pub mod` declarations, the
`QueryRoot`/`MutationRoot`/`SubscriptionRoot` merged objects and the `register`
function — so `register.rs` calls the generated `slices::register(engine)` and
`graphql.rs` mounts the generated roots, and neither is touched when a slice
comes or goes. Removing a slice deletes its folder and its one line in the
`compose_service!` block; adding one is the reverse. This holds even for a slice
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
is the separate producer/runner that closes a real cross-service cycle over NATS,
so the reference service itself never holds a NATS client. The slices between
them exercise all three lanes, all three persistence styles, offers, mirrors,
presence, blobs, cron, scheduled reactions, bulk writes, `declare_scopes`,
erasure and the full GraphQL surface. `tests/e2e.rs` (split into
`tests/scenarios/`, driven by `tests/harness/`) proves them against real
PostgreSQL, NATS and MinIO over the four observation channels: the mutation
`{ success }`/typed-error ack, the query with its affordances, the KV offers and
integration events, and the `Reset`/`Upsert`/`Remove` subscription deltas driven
over a real `graphql-transport-ws` WebSocket with their typed cause.

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
  over GraphQL (`bb05`). The binaries are taken from `EXAMPLE_SERVICE_BIN` /
  `EXAMPLE_TWIN_BIN` when set (the CI black-box job sets them after building),
  and built on demand otherwise, so the mode is self-sufficient locally. The
  accumulated lane stores its chunks in Postgres (`service_engine.accumulator_chunk`)
  and the 0.1.0 engine exposes no NATS/GraphQL chunk-ingress, so `bb05` seeds the
  chunks through Postgres — the flush path's own table shape, a listed black-box
  channel — and everything the seal *is* (replay, hash verification, the
  transactional final write, the impact and delivery) runs in the spawned binary.
  The accumulator's own internals stay proven in-crate (`s035`, `s066`) and by the
  reference service's reply e2e.

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
    --test bb05_seal_streamed_reply

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

## Authoring caveats

- A slice's `Visibility` declaration must be **total and injective**: `cohorts`
  and `memberships` return the same cohorts for the same input on every call, and
  two cohorts the projector means to keep distinct must serialise to distinct
  bytes. The engine keys an RLS render group on the exact `PrincipalId` and a
  declared cohort on the exact bytes of its parts, never a 64-bit hash, so it is
  the totality and injectivity of the declaration — not a hash width — that keeps
  two principals, or two distinct cohorts, from ever sharing one render.
- `Persistence::load` and `read_many` must stay non-locking; the row lock that
  serialises concurrent commands on one key belongs in `Persistence::lock`, a
  `SELECT … FOR UPDATE` on the row (or snapshot) key. A slice whose commands
  read-modify-write one key must implement `lock`; its default is a no-op, which
  is correct only for a slice that needs no cross-command serialisation of a key.

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
is the engine crate's own version.

## Configuration, degradation and observability

`EngineConfig` carries one clock and a handful of bounds, every one validated
at `Engine::boot`: durations and capacities are non-zero,
`listener_queue_threshold` lies in `(0.0, 1.0]`, the `lease` outlasts the
`beat`, and `session_max_age` outlasts the idle `session_ttl`. A session lives at most `session_max_age`; when it does
the engine ends it with the same stream-closing signal as a shutdown, so the
client reconnects with a fresh passport — distinct from `session_ttl`, which
reaps a session that has lost its consumer.

The listening connection is drained by a task that does nothing else: it
forwards notifications into a bounded in-process channel (`listener_channel_capacity`)
that the render loop consumes, so a slow render pass never stops the drain and
never lets the cluster's notification queue back up behind this pod. When the
render loop cannot keep up and the channel overflows, the drained impacts are
dropped and one `Reconnected` is signalled, which re-snapshots every session on
the pod — a detectable loss, never a silent gap. The beat samples
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

Every engine metric is exported on the shared observability endpoint labelled
by `service` and `pod`; each dependency of the degrade table is a
`service_engine_dependency_up` gauge, so a not-UP state is visible before
readiness moves. `service_engine_impacts_committed_total` is the notify-budget
counter watched at the Postgres-cluster level; it counts impacts of committed
transactions only, recorded after the commit, never a rolled-back mutation. The
four shipped alerts are in
[`observability/service-engine-alerts.yaml`](observability/service-engine-alerts.yaml).

## AI disclosure

The code and the documentation of this repository were generated by an AI
system (Anthropic Claude) under the direction and review of BotResources.
BotResources takes full responsibility for them. This disclosure is made in
line with the transparency obligations of the EU Artificial Intelligence Act
(Regulation (EU) 2024/1689).

License: Apache-2.0.
