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
| `persistence` | `Persistence` trait + `Aggregate`; CRUD shipped, soft-EDA / full-EDA behind the same trait | U3 (CRUD) / U4 |
| `gate`, `visibility` | `Gate`/`Reason`, `Affordances`, the `gated!` macro and `check_gates_match_affordances` (affordance == mutation check, one function); `Visibility` cohorts/memberships deriving the `visible` filter and the `window` membership from one declaration, with `check_window_matches_visibility` | U5 (done) |
| `presence` | Presence lane: `EPHEMERAL_*` bucket, `register_presence`, `cx.present` | U6 (done) |
| `offer` | `Offer` trait, `register_offer`, versioned watermark and reconcile | U7 |
| `mirror` | `register_mirror` over the direct KV watch into `known_*` | U8 (done) |
| `blobs` | Object-storage references, `register_blobs`, presigned URLs, reaper | U9 |
| `scopes` | `declare_scopes` handshake gating readiness | U10 (done) |
| `erase` | `Erasable` and `engine.erase(person)` | U11 |
| `graphql` | async-graphql kit; delta (`Reset`/`Upsert`/`Remove`) to subscription union | U12 |

The `register_*` methods that a later unit fills return `EngineError::NotYet`
until then — today only `register_offer` (U7) and `erase` (U11).
`register_reaction` (U2) is live: it records a reaction and
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
`register_presence` (U6) is filled: it binds the `EPHEMERAL_{service}` bucket at
boot (bind-only, fail-loud), every pod watches it, and put/expiry reach sessions
as `Upsert`/`Remove` through the same session/render machinery as every other
lane; name the bucket with `EngineConfig::with_service`. `register_mirror` (U8)
projects a consumed KV offer into `known_*` through the direct lane, and
`declare_scopes` (U10) runs the boot scope-declaration handshake that gates
readiness until Identity confirms.
`register_blobs` (U9) is filled: it records a `BlobPolicy` per blob kind and, at
boot, binds the service's S3-compatible object-storage bucket (bind-only,
fail-loud, never created — configured with `EngineConfig::with_blob_storage`).
`cx.blob::<Kind>(name, content_type)` stages a blob **reference row**
(`service_engine.blob`: reference, object key, kind, content type, file name,
owner, size and state) inside the pipeline transaction, so it commits with the
referencing aggregate and a rollback leaves no row; it returns a typed
`UploadUrl`, and `Engine::download_url` a `DownloadUrl`, both S3 SigV4 presigned
and short-lived. The bytes flow client-to-storage directly, so `size` is unknown
at commit and is recorded when the reaper sees the completed upload and promotes
the row. `UploadUrl`/`DownloadUrl` are not `Serialize`, so — like `OneShot` — a
URL is structurally unable to enter a view, an impact, an offer, an outbox row or
a chunk; only the opaque reference travels. The beat runs a reaper that, per
`BlobPolicy`, promotes a completed upload (recording its size), deletes an
abandoned upload (a `pending` row past `orphan_after` whose object never landed)
or one whose object exceeds `max_bytes`, and deletes an orphan (a reference
released via `cx.release_blob` past `orphan_after`). `max_bytes` is a
**best-effort** cap, not a hard limit: an S3 presigned PUT cannot bound the size
at upload, so the reaper is the only lever, and it acts *after* the fact — an
over-cap object is promoted first and reaped on a later sweep, and reaping an
object whose reference an aggregate already committed leaves a live `BlobRef`
that then 404s on download. A slice that needs a hard cap must enforce it out of
band (a bucket policy or an ingress limit), not rely on `max_bytes`. Orphan
reaping is signal-driven: a slice releases the reference when it drops the owning
row; the engine does not reference-count slice-owned tables.
`cx.blob::<Kind>(name, content_type)` records no owner, so its row is **not**
reached by `purge_person_blobs`; a personal file that must be erasable with its
owner MUST be attached with `cx.blob_owned::<Kind>(name, content_type, person)`.
`Engine::purge_person_blobs` is the erase hook U11 calls to drop a person's blobs
from storage and the reference table. Presigning uses the sans-IO `rusty-s3`
crate for SigV4 and `reqwest` (rustls) as the thin HTTP client for the engine's
own bucket HEAD/DELETE — no cloud SDK.

## Conformance battery

The battery needs real infra: a PostgreSQL admin URL in `E2E_PG_ADMIN_URL`
(fallback `DATABASE_URL`), `nats-server` on `PATH` (it spawns its own broker
per test), and — for the blob scenarios — `minio` on `PATH` (it spawns its own
S3-compatible server per test).

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
