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

The 0.1.0 rework lands in units. The multi-pod delivery core (`transport`,
`render`, `session`, `cohort`, `population`, `accumulator`, `housekeeping`,
`relays`, `mirror`, `principal`, `projector`) is implemented and battery-backed.
The author-facing surface is laid out one module per remaining unit, so each
fills its part by adding module files and one method body.

| Module | Responsibility | Unit |
|---|---|---|
| `engine` | `Engine::boot` and the `register_*` / `declare_scopes` / `erase` surface | U1 (skeleton) |
| `nats` | Engine-owned NATS: stream/bucket bind, KV read/write/watch, outbox publish | U1 |
| `inbound` | Inbound NATS loop: durable consumer, poison/dead-letter, `Disposition` | U2 (done) |
| `pipeline` | Direct write pipeline; `Mutation` / `Reaction` / `Bulk` contexts; `OneShot` | U3 (done) |
| `persistence` | `Persistence` trait + `Aggregate`; CRUD, soft-EDA and full-EDA behind one trait; log-style events reach `save` via `Aggregate::pending_events` | U3 (CRUD) / U4 (soft + full) |
| `gate`, `visibility` | `Gate`/`Reason`, `Affordances`, the `gated!` macro and `check_gates_match_affordances` (affordance == mutation check, one function); `Visibility` cohorts/memberships deriving the `visible` filter and the `window` membership from one declaration, with `check_window_matches_visibility` | U5 (done) |
| `presence` | Presence lane: `EPHEMERAL_*` bucket, `register_presence`, `cx.present` | U6 (done) |
| `offer` | `Offer` trait, `register_offer`, versioned watermark and reconcile | U7 |
| `mirror` | `register_mirror` over the direct KV watch into `known_*` | U8 (done) |
| `blobs` | Object-storage references, `register_blobs`, presigned URLs, reaper | U9 |
| `scopes` | `declare_scopes` handshake gating readiness | U10 (done) |
| `erase` | `Erasable` and `engine.erase(person)` | U11 |
| `graphql` | async-graphql kit; delta (`Reset`/`Upsert`/`Remove`) to subscription union | U12 |

The `register_*` methods that a later unit fills return `EngineError::NotYet`
until then — today only `register_offer` (U7), `register_blobs` (U9) and
`erase` (U11). `register_reaction` (U2) is live: it records a reaction and
derives its inbound subscription, and the engine-owned inbound loop (durable
consumer, ack-after-durable, `Disposition` routing, poison budget with the
`service_engine.dead_letter` table and its retry/discard gestures, the
per-(producer, key) sequence guard beside the idempotency claim) runs over it.
`register_mutation` and `register_bulk` (U3) are live: a GraphQL mutation and a
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
All three persistence styles (U4) fill the same `Persistence` trait behind the
one-arg `cx.save` / `cx.create`, so one mutation handler runs unchanged over
CRUD, soft EDA (the state row plus an appended fact per change) and full EDA (an
event log plus a synchronous snapshot that is the locked state row). The
command's events reach `save` through the default `Aggregate::pending_events`
(`&[]` for CRUD), never through the pipeline. A style writes the state row (or
snapshot) and its events (or facts) in the one transaction the pipeline opened
and never opens its own, so a foreign-key, unique or check-constraint failure on
either table rolls the state row and its events back together. The reference
stores take the row (or snapshot) lock at `load` with `SELECT … FOR UPDATE`, so
concurrent commands on one key serialize in every style — the engine cannot
inject that lock into author-owned load SQL, so the sample locks it and slices
copied from it inherit it, while the render-side `Projector::load` stays
lock-free. Full EDA hydrates
on `load` by replaying the events above the snapshot and running the aggregate's
hydration check as the second barrier, and owns the log's two gestures —
upcasting an older event version at read time, and erasure, which rewrites a
person's events in place and re-snapshots from the rewritten log in the same
transaction. On the engine's own authority, an integrity (SQLSTATE class 23) or
data (class 22) violation raised inside a handler's `cx.save` / `cx.create` is
classified terminal whatever the handler's `Disposition` says, so a coarse
`Retry` cannot nak a constraint violation forever; a raw `cx.connection()` write
stays the handler's to classify. The render-side `Projector::load` and the
write-side `Persistence` read one committed store — for full EDA the snapshot is
the state row the projector reads — so a `fetch`, a session `Upsert` and a
write-side `load` return the same committed truth.
`register_presence` (U6) is filled: it binds the `EPHEMERAL_{service}` bucket at
boot (bind-only, fail-loud), every pod watches it, and put/expiry reach sessions
as `Upsert`/`Remove` through the same session/render machinery as every other
lane; name the bucket with `EngineConfig::with_service`. `register_mirror` (U8)
projects a consumed KV offer into `known_*` through the direct lane, and
`declare_scopes` (U10) runs the boot scope-declaration handshake that gates
readiness until Identity confirms.

The `graphql` module (U12) is the async-graphql surface kit. A service composes
its slices' root objects into one schema with `engine_schema`, mounts it with
`app` (`POST /graphql`, the GraphQL-over-WebSocket subscription on
`GET /graphql/ws`, and `/readyz`) and runs it with `serve`;
`Engine::graphql_state` wires the executor, the render runtime and the pool into
it. Mutation resolvers run on `Engine::mutation_executor` (`execute` /
`ack` and their bulk forms), answering `{ success }` or a typed error carrying
the gate's `Reason` code, and returning a `OneShot`'s inner value only in the
mutation response. Query resolvers read rendered views through `fetch` /
`fetch_window` and never the database. The subscription maps the engine's
`Reset`/`Upsert`/`Remove` wire to the `EngineDelta` union with the contiguous
revision and the causing event, and the axum layer resolves the principal from
the trusted `X-Passport` header (`PassportPrincipal`) before the executor runs —
the kit does authZ only, never authN.

## Conformance battery

The battery needs real infra: a PostgreSQL admin URL in `E2E_PG_ADMIN_URL`
(fallback `DATABASE_URL`) and `nats-server` on `PATH` (it spawns its own broker
per test).

```bash
E2E_PG_ADMIN_URL=postgresql://postgres:postgres@localhost:5432/postgres \
  cargo test -p conformance-service-engine --all-targets
```

## Deployment constraint

No transaction-mode pooler in front of an engine service: the realtime
transport holds a session-level `LISTEN`, which such a pooler drops silently —
the engine proves the path with a boot probe and holds readiness DOWN when it
fails, so a mispooled service never becomes ready.

## Configuration, degradation and observability

`EngineConfig` carries one clock and a handful of bounds, every one validated
at `Engine::boot`: durations are non-zero, `listener_queue_threshold` lies in
`(0.0, 1.0]`, the `lease` outlasts the `beat`, and `session_max_age` outlasts
the idle `session_ttl`. A session lives at most `session_max_age`; when it does
the engine ends it with the same stream-closing signal as a shutdown, so the
client reconnects with a fresh passport — distinct from `session_ttl`, which
reaps a session that has lost its consumer.

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
counter watched at the Postgres-cluster level. The four shipped alerts are in
[`observability/service-engine-alerts.yaml`](observability/service-engine-alerts.yaml).

## AI disclosure

The code and the documentation of this repository were generated by an AI
system (Anthropic Claude) under the direction and review of BotResources.
BotResources takes full responsibility for them. This disclosure is made in
line with the transparency obligations of the EU Artificial Intelligence Act
(Regulation (EU) 2024/1689).

License: Apache-2.0.
