# Changelog

All notable changes to `br-service-engine` are documented here. The whole
workspace ships **one version**: every crate inherits `version.workspace = true`,
and a single git tag `v{version}` releases the set. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver.
The library chart `br-engine-service` has its own version line (major = ops
contract version) and its own tag `chart/br-engine-service/v{version}`; a
chart-only release is a `## chart br-engine-service {version}` section.

## 0.3.4 - 2026-09-24

Engine services become reachable for subscriptions through the gateway. The
gateway reaches a subgraph for a subscription only as `POST /graphql` with
`Accept: text/event-stream` (graphql-sse over HTTP), never over a WebSocket;
through 0.3.3 the engine served subscriptions on `GET /graphql/ws` alone and
answered that request with the JSON error "Subscriptions are not supported on
this transport.", which the gateway reports as `SUBGRAPH_REQUEST_ERROR` — seen on
dev for every engine service. Additive: no adopter code changes.

### Added

- **The gateway's subscription transport on `POST /graphql`.** A request whose
  `Accept` names `text/event-stream` is authenticated and its body read exactly as
  before — the passport from the headers first (`401` with the body never read,
  the same status and body a JSON request gets), then the bounded body — and is
  then answered as a graphql-sse "distinct connections" stream (`graphql/sse.rs`):
  `Content-Type: text/event-stream`, `Cache-Control: no-cache`, one `event: next`
  per GraphQL response (`data:` the response JSON on one line), one
  `event: complete` (`data:` empty) at the end, and a `:` comment after 15 s
  without a frame. Any operation may be sent this way: a query or a mutation
  answers one `next`, then `complete`; a coded refusal rides a `next`, then
  `complete`, as on the WebSocket. A response that cannot be serialized is logged
  and sent as a coded `INTERNAL` response, never as an empty frame. A request
  without `text/event-stream` in `Accept` (a wildcard does not count) is answered
  as before.
- **Bounded like a WebSocket session, on the same session machinery.** The stream
  runs the same schema, so a subscription attaches the same engine session as on
  `/graphql/ws`: same `Reset`/`Upsert`/`Remove` wire and revision, same cohorts,
  same principal-facts refresh. At `session_max_age`, measured from the request,
  and when the engine shuts down (the signal that closes the WebSockets), it sends
  `complete` and ends, so the gateway re-subscribes with a fresh `X-Passport` and
  an open stream never holds graceful shutdown; a stream requested during shutdown
  answers `complete` without executing. A client that goes away drops the
  response, and with it the operation and its session.
- Conformance: `bb16` (black-box, the real `example-service` binary) — over the
  gateway's transport a subscriber gets the `Reset` then the `Upsert` on a
  contiguous revision, read with `br-test-harness`'s `SseSubscription` (the client
  service e2e suites use); a passport that is absent, undecodable or not a
  passport is refused with the very `401` a JSON request gets, before a byte of
  the body is read (a body the client never finishes sending is answered at once,
  while an authenticated control is waited for); and past `session_max_age` the
  stream sends `complete`, ends, and a new request attaches afresh. The `Reset`
  and max-age scenarios fail on 0.3.3 with the dev error; the `401` scenario
  pins that the refusal still precedes the transport choice. Unit tests pin the
  framing, a query over the stream, a refusal as a `next`, the keep-alive
  cadence, the max-age and shutdown completions, the refusal to start during
  shutdown, the release of the operation when the response is dropped, and which
  `Accept` values ask for a stream.

### Changed

- `conformance-service-engine` dev-depends on `br-test-harness` 1.2.0 (the
  `br-e2e-harness` repository, `sse` feature only); `deny.toml` allows that git
  source.
- README: the ops contract names the WebSocket route `/graphql/ws` (it read
  `/ws`) and documents both subscription transports; the black-box command list
  names `bb12` to `bb16`.

### Adopter migration

- None. Move the pin to `v0.3.4`: `app` serves the event stream with no new
  configuration and no environment variable, and a service's schema and SDL are
  unchanged. Its subscriptions become reachable through the gateway as soon as
  the new image rolls.

### Known limitation

- **Paging a gateway (SSE) session requires the session's pod.** The `page`
  gesture serves only a live session this pod holds. Through the gateway the
  session rides an event stream and the `page` mutation is a separate `POST`
  that may reach another replica, where it is refused
  (`EngineError::NoLiveSession`, `NOT_FOUND`). Paging a gateway (SSE) session
  requires the session's pod (one replica) until 0.4.0 moves paging to
  subscription arguments
  ([#130](https://github.com/BotResources/ws-cc-platform.botresources.ai/issues/130)).
  Engine services run one replica today.

## 0.3.3 - 2026-09-23

A security patch over 0.3.2. `POST /graphql` now resolves the passport before it
reads the request body, and then reads every body within explicit bounds: its size
(JSON as well as a GraphQL multipart request) and the time it takes to arrive. No
adopter code changes; one new optional configuration group and one timeout, both
with defaults.

### Security

- **An unauthenticated multipart request could make the pod write to disk.**
  Through 0.3.2 the handler took async-graphql-axum's `GraphQLRequest` extractor,
  which parses the body — and, for a GraphQL multipart request (`operations` +
  `map` + file parts), spools every file part to a temporary file — before the
  handler looked at `X-Passport`. Any client that reached a pod could therefore
  fill its writable filesystem (on a read-only root the request failed `400` and
  nothing was written), and a JSON body of any size was buffered in memory before
  the same `401`. The parser also bounded nothing: no body, file or file-count
  limit, and a file part spooled even when `map` did not name it or the schema
  declared no `Upload`. Found while verifying the engine under a read-only root
  filesystem (chart `br-engine-service` 1.1.0, PR #17).
- **The fix: authentication first, then a bounded read.** The handler resolves the
  principal (passport decode, `PassportPrincipal::from_passport`, principal facts)
  from the headers alone; a request whose passport is absent, does not decode or
  is rejected is answered `401` with its body never read, whatever its content
  type. The `401` status and body are those JSON clients already received;
  principal facts the pod cannot load are the `500` `INTERNAL` of 0.3.2, now also
  answered before the body is read. Only then is the body read: a `multipart/*` body through the engine's own receiver
  (`graphql/multipart/`), every other body streamed to the function
  async-graphql-axum's extractor calls, so it is parsed exactly as before (same
  content-type dispatch, same single-request rule, same rejection).
- **An authenticated body is bounded in memory and in time.** Through 0.3.2 an
  authenticated JSON body of any size was buffered whole, and a client that sent
  its body slowly (or never finished it) held the request, its buffer and any
  spooled file open for as long as it kept the connection. Every authenticated
  body — JSON and multipart alike — is now capped by `max_body_bytes` (a declared
  `Content-Length` above it is refused before the body is read, a chunked body is
  cut on the chunk that crosses it) and must arrive whole within
  `body_read_timeout`; past it the read is abandoned and whatever was received,
  buffered bytes or spooled files, is dropped with it.

### Added

- **Multipart bounds** — `EngineConfig::multipart: MultipartConfig`
  (`with_multipart`; the type at `service_engine::` and `service_engine::graphql`),
  validated at boot:
  `max_body_bytes` (default 16 MiB; a declared `Content-Length` above it is refused
  before the body is read, a chunked body is cut), `max_file_bytes` (default 8 MiB,
  any single part), `max_files` (default 4, the uploads `map` binds — every path
  counts; `map` itself may weigh at most 1 KiB per allowed upload plus 1 KiB, so a
  padded `map` is refused before it is parsed), `spool_dir` (default unset:
  `std::env::temp_dir()`, i.e. `$TMPDIR`, else `/tmp`). No environment variable is
  added to the ops contract.
- The receiver enforces the spec's order (`operations`, `map`, then the files `map`
  names), so the upload count and each file part's binding are judged before
  anything is spooled; a part `map` does not name is refused unspooled. A schema
  that declares no `Upload` scalar (read from its SDL) accepts no file (its
  effective `max_files` is 0): multipart stays accepted, and such a service never
  writes to disk. Files spool to anonymous temporary files, freed when the request
  ends.
- Coded refusals, a GraphQL-shaped body with `errors[0].extensions.code`:
  `413 MULTIPART_TOO_LARGE`, `413 MULTIPART_FILE_TOO_LARGE`,
  `413 MULTIPART_TOO_MANY_FILES`, `400 MULTIPART_MALFORMED`,
  `500 MULTIPART_SPOOL_UNAVAILABLE` (the path and the OS error are logged, never
  returned). The codes are public constants `graphql::MULTIPART_*_CODE`.
- **Every authenticated body** — `MultipartConfig::max_body_bytes` (default
  16 MiB) now bounds a JSON body too, refused `413 BODY_TOO_LARGE`; and
  `EngineConfig::body_read_timeout` (`with_body_read_timeout`, default 30 s,
  `config::DEFAULT_BODY_READ_TIMEOUT`, validated non-zero at boot) bounds the time
  from the passport resolving to the last byte of the body, for every content type,
  refused `408 BODY_READ_TIMEOUT`. Both are GraphQL-shaped like the multipart
  refusals; the codes are public constants `graphql::BODY_TOO_LARGE_CODE` and
  `graphql::BODY_READ_TIMEOUT_CODE`. No environment variable is added.
- Conformance: `s241` — an unauthenticated multipart request (passport absent,
  undecodable, or not a passport) is `401` before a byte of its body is read,
  proven on a body the client never finishes sending (an authenticated control
  shows the same stalled body is waited for) and on a spool directory an
  authenticated control shows the request would otherwise reach; an authenticated
  upload within bounds reaches the resolver intact; each bound — declared and
  chunked body, part, upload count — and the malformed order are refused with
  their code; a schema without `Upload` accepts multipart without files and
  refuses a file. `s115` pins the anonymous JSON `401` and its body. Unit tests pin
  every bound (fed in small reads, so a bound trips mid-part), the `map` weight,
  the order rules, the no-`Upload` policy and its SDL detection, the spool failure,
  the content-type dispatch (any case of `multipart/form-data` reaches the bounded
  receiver; an unparseable type is refused unread) and the config validation.
- Conformance: `s242` — an authenticated JSON body over `max_body_bytes` is
  `413 BODY_TOO_LARGE`, by its declared length (answered at once, the body never
  awaited) and as a chunked body cut in flight, while an anonymous oversized body
  is still `401`; a JSON body and a multipart body stalled inside a file part being
  spooled are `408 BODY_READ_TIMEOUT` at the read timeout, not before it, leaving
  no named file in the spool; after every refusal the pod serves JSON and uploads
  within bounds. Unit tests pin the declared and streamed JSON bound (nothing past
  the crossing chunk is read), the inclusive limit, the deadline for both body
  kinds, and the rendered codes.

### Changed

- An anonymous request is now refused before its body is parsed, so an anonymous
  request whose body is also malformed answers `401` where 0.3.2 answered `400`.
- A multipart request must follow the spec's part order and bind every file part
  it carries (0.3.2's parser accepted parts in any order and silently spooled
  unmapped ones); a `multipart/*` type other than `form-data` is `400
  MULTIPART_MALFORMED`.
- An authenticated JSON body over 16 MiB is refused `413 BODY_TOO_LARGE` (0.3.2
  buffered any size), and an authenticated body that has not fully arrived 30 s
  after its passport resolved is refused `408 BODY_READ_TIMEOUT` (0.3.2 waited with
  no end).

### Adopter migration

- None required. A service whose schema declares `Upload` and runs under a
  read-only root filesystem mounts a writable `emptyDir` (with a `sizeLimit`) at
  `/tmp` — or at its `MultipartConfig::spool_dir` — through the chart's
  `extraVolumes` / `extraVolumeMounts`; no engine service declares `Upload` today
  (blobs use presigned URLs), so none needs it. A service that expects larger
  uploads raises the bounds with `EngineConfig::with_multipart`.
- A service whose clients send JSON documents over 16 MiB inline raises
  `MultipartConfig::max_body_bytes` (the name predates its reach: it bounds every
  body); one whose clients reach the pod unbuffered over slow links raises
  `EngineConfig::with_body_read_timeout`. Behind the gateway, whose nginx buffers a
  request body before it proxies it (its default), the defaults leave a wide margin.
- `EngineConfig` gains the `multipart` and `body_read_timeout` fields (the struct is
  `#[non_exhaustive]`).

### Follow-up (documented, not implemented)

- **No limit on concurrent uploads.** The bounds above are per request: an
  authenticated client can keep several uploads spooling at once, each up to
  `max_body_bytes` for up to `body_read_timeout`. No engine service declares
  `Upload` today, so no pod spools anything; the first service that adopts
  `Upload` adds a concurrent-upload limit (a spool semaphore, with its own coded
  refusal) sized together with its `emptyDir` `sizeLimit`.

## 0.3.2 - 2026-09-23

A patch: `migrate` upgrades a database that the 0.2.0 engine migrated, and every
failure the engine answers reaches the client with a code.

**Adopter note: nothing to do.** Bump the pin. A database migrated by engine
0.2.0 (svc-runners 0.1.0 on the dev stage, svc-accounts) now upgrades on the next
`migrate`; before this release `migrate` on 0.3.0 or 0.3.1 exited 1 there with
`VersionMismatch(9113000023)` (`sqlx migrate info`: `installed (different
checksum)`).

### Fixed

- **`migrate` adopts the checksum a released engine applied for a migration that
  was later edited.** v0.3.0 (`e667eb9`) removed the four-line header comment of
  `9113000023_mirror_stream_identity.sql`, which v0.2.0 had applied; sqlx hashes
  the whole file, so every database 0.2.0 migrated refused the 0.3.x engine set
  (`ignore_missing` does not cover a changed checksum). Audit of every engine
  migration across `v0.1.0`, `v0.2.0`, `v0.3.0`, `v0.3.1` and `main` (and every
  commit on `main`): `9113000023` is the only file whose bytes changed —

  | Migration | v0.1.0 | v0.2.0 | v0.3.0 / v0.3.1 / main |
  |---|---|---|---|
  | `9113000001`–`9113000022` | shipped | same bytes | same bytes |
  | `9113000023_mirror_stream_identity.sql` | — | SHA-384 `74808ac1…daae46f` | SHA-384 `ea7d3984…d84720fd` |
  | `9113000024`, `9113000025` | — | — | added |

  `schema/released.rs` holds a data table, `EDITED_AFTER_RELEASE:
  &[(version, &[released SHA-384])]`, with the one entry `9113000023 →
  [v0.2.0's checksum]`. `schema::migrate` takes the sqlx migrator's own advisory
  lock on one connection, rewrites in one transaction every stored checksum that
  the table registers for its version to the embedded file's checksum, logs each
  at `info` with the version and the old and new checksums, then runs the
  migrator on the same locked connection. An unknown checksum is never touched
  and still fails as `EngineError::Migrate(VersionMismatch(v))`, exactly as
  before. Only the versions in the table — all inside the engine's reserved band —
  are read or written, so library and service rows on the shared
  `_sqlx_migrations` ledger are never touched. A failed run closes its connection
  instead of pooling one that still holds the session advisory lock.
- **Every failure reaches the client with a code (constitution principle 22).**
  A mutation whose transaction did not begin or commit answered an uncoded
  `mutation failed: database`; a query, a subscription attach or a page that
  failed answered an uncoded message too. Now everything the client cannot fix —
  a database or NATS fault, a begin or commit failure, an engine wiring fault, a
  handler error whose `MutationFault::reason()` is `None` — is `INTERNAL`
  (`extensions.code`), with the fixed message `internal error` and no database
  detail. The few failures a client can act on keep a specific code: `NOT_FOUND`
  for a page on a session the caller does not hold or a window it never attached,
  `CONFLICT` for an attach under a session id a live session holds,
  `UNAUTHENTICATED` for an attach whose principal no longer exists. A failure to
  load the principal's facts is a `500` with the same `INTERNAL` body, no longer a
  `401` carrying the error text. Refusals that carry a reason are unchanged: same
  code, same `mutation refused: …` message.
- **The engine's logs keep the whole cause chain.** `EngineError::Db` displayed
  `database`, `Migrate` `migrations`, `Service` `service` (and `CronError::{Db,
  Job}`, `RelayError::{Db, Relay}` likewise), so every `%error` log and every
  adopter that logged an engine error saw the label, not the cause. These pure
  wrappers are now `#[error(transparent)]`: their `Display` is the wrapped error's
  and the chain does not repeat. Every engine log site that logged an error by
  `Display` now logs its whole `source()` chain, as do the conversions to text the
  engine stores or forwards (dispatch errors and dead-letter reasons, object
  storage and NATS details). Where the engine converts a failure for the client it
  logs the chain at `error` first: an engine fault in the mutation pipeline with
  its context, a handler fault without a reason with the mutation name (and the
  fault's own chain when it returns `Some(self)` from the new
  `MutationFault::as_error`), a query, attach or page fault with its context.

### Added

- `graphql::{INTERNAL_CODE, INTERNAL_MESSAGE, NOT_FOUND_CODE, CONFLICT_CODE,
  UNAUTHENTICATED_CODE}`, `graphql::internal_error(context, &cause)` (logs the
  chain, answers `INTERNAL`) and `graphql::internal_fault(detail)` for a fault
  with no error value behind it.
- `MutationFault::as_error`, a provided method (default `None`): a fault that is a
  `std::error::Error` returns `Some(self)` so the engine logs its chain.
- `error::describe(&dyn Error) -> String`, the `top: cause: root` rendering the
  engine logs, for a service that logs an engine error or keeps it as text.
- CI guard `.github/scripts/check-released-migrations.sh`, run in the
  `fmt-clippy-test` job (checkout now `fetch-depth: 0`): it exports the engine
  migrations of every `v*` tag and fails when a released file is gone from HEAD or
  differs from it without its released SHA-384 registered in
  `EDITED_AFTER_RELEASE`. Every tag is checked, not only the last, so a registered
  checksum cannot be dropped silently.
- `s241` (conformance, in-crate, real Postgres): a store migrated by the real
  v0.2.0 engine files plus a library and a service set upgrades through the
  migration chain, its engine ledger then equals a fresh store's and its library
  and service rows are untouched, and a second `migrate` changes nothing; a forged
  unknown checksum still fails with `VersionMismatch` and is left as it was; a
  fresh store migrates as before. The first scenario fails with
  `Migrate(VersionMismatch(9113000023))` on 0.3.1.

### Changed

- The `Display` of `EngineError::{Db, Migrate, Service}` is now the wrapped
  error's message, database text included. A service that forwards an engine
  error's text to its clients should answer `INTERNAL` instead; both platform
  adopters already do (svc-runners renders its code, svc-accounts' reasonless
  faults now reach the client as `INTERNAL`).
- `example-service`'s `AppFault` is the reference shape: `NotFound` carries the
  reason `NOT_FOUND` (a refusal the client can act on, no longer an uncoded
  failure), `Store` keeps the engine error's whole chain as text (rendered with
  `error::describe`, so the type keeps its auto traits) and `as_error` returns
  `Some(self)`; its `ReactionFault` keeps the chain the same way.
- The engine now owns one lowercase-hex encoder (`hex::lower`); the four copies in
  blob signing, blob digests, seal hashes and NATS subject tokens use it, and the
  `unwrap` in the subject-token copy is gone.

## chart br-engine-service 1.1.0 - 2026-09-23

A minor: new optional fields, every one with a default; ops contract v1 is
unchanged (entry points, env names, probe paths, `Recreate`). The engine crates
do not change.

### Changed — the pod is hardened by default

**1.1.0 renders a different pod than 1.0.0 from the same values.** With no new
key set, the Deployment now carries:

- pod `securityContext`: `runAsNonRoot: true`, `runAsUser`/`runAsGroup`/
  `fsGroup: 65532`, `seccompProfile: RuntimeDefault` — the engine image sets no
  `USER`, so the UID is explicit;
- on **both** containers (`migrate` and `serve`): `allowPrivilegeEscalation:
  false`, `readOnlyRootFilesystem: true`, `capabilities.drop: [ALL]`;
- `automountServiceAccountToken: false` — the engine never calls the
  Kubernetes API;
- a `startupProbe` on `serve`: `GET /livez`, `periodSeconds: 5`,
  `failureThreshold: 30` (150 s). `serve` binds its listener last, so `/livez`
  answering is the end of boot; liveness is suspended until then.

**Adopter impact.** A thin chart that bumps 1.0.0 → 1.1.0 and changes nothing
gets the hardened pod. The engine binary runs that way: `migrate` and `serve`
were verified against a real Postgres and NATS under a sandbox that refuses
every file write (`migrate` exits 0; `serve` answers `/livez`, `/readyz`,
`/sdl`, `/metrics`). An image whose **own** code cannot run that way sets the
override — key by key, the rest of the hardening stays:
`podSecurityContext.runAsUser: <uid>` for an image that needs a given UID,
`extraVolumes` + `extraVolumeMounts` (an `emptyDir`) for a path the service's
code writes, `containerSecurityContext.readOnlyRootFilesystem: false` only as a
last resort, `automountServiceAccountToken: true` for code that calls the
Kubernetes API, `probes.startup.failureThreshold` for a boot longer than 150 s.
A key set to `null` removes that default key.

### Added — neutral fields

Absent by default; a key left out renders exactly as 1.0.0 did. The library
gives none of them a platform meaning — the service chart sets the value.

- `deploymentAnnotations`, `podAnnotations`, `podLabels`.
- `service.labels`, `service.annotations` on the library Service.
- `imagePullSecrets` (Kubernetes shape, `[{name: …}]`) on the pod.
- `nodeSelector`, `tolerations`, `affinity`.
- `migrate.resources` for the init container (`resources` stays `serve`'s).
- `extraVolumes` (pod) and `extraVolumeMounts` (`serve`).
- `probes.{startup,readiness,liveness}` — timing keys only
  (`initialDelaySeconds`, `periodSeconds`, `timeoutSeconds`,
  `successThreshold`, `failureThreshold`); readiness and liveness keep the
  kubelet defaults unless set.
- `objectStore.publicEndpoint` → `S3_PUBLIC_ENDPOINT` (ops contract v1's
  optional S3 variable, until now passed through `env`).

### Guards

- `podLabels` / `service.labels` cannot replace a library label
  (`app.kubernetes.io/name`, `/instance`, `/managed-by`, `/part-of`): the
  render fails.
- `probes.*` accept only the five timing keys; a path or port override fails
  the render — the probe paths are ops contract v1.
- An absent `objectStore` group no longer fails the render (read as disabled).
- The env helpers no longer leave whitespace-only lines in the manifest.
- `.helmignore` keeps `ci/` (the fixture thin chart) out of the published
  package: 1.0.0 shipped it; 1.1.0 ships the library only.

### Extension point, documented

A service chart ships its own templates beside the include-only template and
reuses the library helpers (`fullname`, `labels`, `selectorLabels`, `port`);
the README shows a second, labelled Service written that way. Such resources
belong to the service chart, never to the library.

### CI

The `chart` job also asserts the hardened defaults on the fixture's default
render (both containers, `startupProbe`, no pull secret, no annotations), and
renders `ci/thin-example/values-all-fields.yaml` to prove each neutral field
reaches the manifest and that the two guards fail the render.

### Release

`chart-release.yml` packages and pushes
`oci://ghcr.io/botresources/charts/br-engine-service:1.1.0` and tags
`chart/br-engine-service/v1.1.0` on the merge to `main`.

## 0.3.1 - 2026-09-23

A patch over 0.3.0: a mirror accepts the key grammar a published language really
uses, and `migrate` asserts the owner posture the ops contract already required.
Both are additive for an adopter on 0.3.0 — every `/`-terminated consumption and
offer keeps its behaviour and its manifest key — and the second only refuses a
deployment that was already outside the contract.

### Fixed

- **A mirror consumes a `.`-terminated prefix and a single exact key.** 0.3.0
  refused at registration every `Consumed::PREFIX` that did not end in `/`
  (`invalid configuration: mirror … consumes prefix … which must end with '/' so its
  manifest key sits outside the data prefix`), so a service consuming a producer
  whose keys use `.` as the segment separator, or one singleton configuration key,
  could not boot. The rule protected the manifest placement: `manifest_key(prefix)`
  strips the trailing separator and appends `_manifest`, and under the mirror's
  `starts_with` matching a prefix that ends mid-segment (`catalog/items`) would take
  its own manifest (`catalog/items_manifest`) and any sibling sharing its
  characters as data. 0.3.1 parses `Consumed::PREFIX` once into one of two forms
  (`mirror/scope.rs`, crate-private `KeyScope`):
  - a **prefix** ends in a segment separator, `/` or `.` (`catalog/items/`,
    `catalog.item.`); the mirror reads every key that starts with it, and its
    manifest key is the sibling `{prefix minus the separator}_manifest`
    (`catalog/items_manifest`, `catalog.item_manifest`), which never starts with
    the prefix, so a manifest is never read as data and a data key is never judged
    as a manifest;
  - any other string is **one exact key** (`catalog.settings`), read with one GET at
    scan and matched by equality at watch — a key beneath it or sharing its
    characters is not read. A single key is judged against no manifest: only an
    engine offer writes a manifest, and an offer publishes a family under a prefix,
    so no engine producer ever writes one beside a single key. A non-engine
    producer's single key is versioned through `wire_version` (N9), as before.

  `Mirror::validate` refuses, as `EngineError::Config` naming the mirror and the
  string, only the forms that are ambiguous or malformed: a string ending in `_` or
  `-` (a prefix cut mid-segment — it would otherwise silently read as a key nobody
  publishes), a single key ending in `_manifest` (it would read an offer manifest as
  data), a leading separator or two separators in a row (an empty segment), and a
  character outside `[A-Za-z0-9_./-]`. `require_key::<C>(key)` accepts the consumed
  single key itself; a key beneath a single key is `RequiredKeyOutsidePrefix`.
- **An offer publishes under a `.`-terminated prefix too.** `register_offer` applies
  the same grammar through `manifest_key`, so a producer and its consumers compute
  one manifest key from one rule; an offer still refuses a single key. Because `x/`
  and `x.` share the manifest key `x_manifest`, an engine refuses a second offer
  whose manifest key another registered offer already claims; across services the
  manifest carries its `prefix`, so such a collision reads as
  `ManifestMismatch::Offer` and dead-letters instead of passing silently.
- `manifest_key(prefix)` (public) accepts a `.`-terminated prefix and refuses a
  single key or a malformed prefix with a message that names the form.

### Added

- **`migrate` asserts the owner posture.** Constitution principle 30 and the ops
  contract require the owner role to carry `BYPASSRLS`: a data migration run by a
  role subject to row-level security touches no row of a `FORCE ROW LEVEL SECURITY`
  table and still reports success. 0.3.0's `migrate` never checked it, so an adopter
  had to add its own guard. `engine::boot::assert_owner_posture(&PgPool)` (public)
  passes a superuser or a `BYPASSRLS` role and refuses any other with the new
  `EngineError::OwnerSubjectToRls { role }`; `apply_migration_chain` calls it right
  after the pure library validation and before the first migration, so `migrate`
  (the chart's init container) logs the refusal and exits non-zero with nothing
  applied. The engine's own black-box and example harnesses now declare their owner
  roles `BYPASSRLS`, as production does.
- Conformance: `s238` (a `.` prefix and a single key read exactly their keys at scan
  and at watch, with sibling, child, near-miss and manifest keys published beside
  them), `s239` (a `.` prefix is judged against its sibling manifest, a single key
  against none), `s240` (the owner posture matrix, and the chain refusing before it
  applies anything), `bb15` (the real binary's `migrate` refuses an owner without
  `BYPASSRLS` and exits non-zero). Unit tests pin the grammar, the manifest
  placement for every accepted form, the refused forms, and the offer-side claim.

### Adopter migration

- mirror (additive): a consumption of a `.`-separated producer or of one
  configuration key now registers as written — declare `Consumed::PREFIX` as the
  prefix with its trailing `.` or `/`, or as the exact key. Nothing changes for a
  `/`-terminated prefix. A string without a trailing separator is now one exact key:
  if you meant a prefix, add the separator (0.3.0 refused that string, so no 0.3.0
  adopter reads differently).
- migrate (fail-loud): declare the owner role `BYPASSRLS` (or run `migrate` as a
  superuser) — the ops contract already required it, and a `migrate` that now exits
  with `OwnerSubjectToRls` names the role to fix in GitOps. A service-owned owner
  posture guard in `main` is redundant and can be deleted.
- `EngineError` gains `OwnerSubjectToRls` (the enum is `#[non_exhaustive]`).

## 0.3.0 - 2026-09-22

### lane: reset

#### 2. Reset paths — the reported race was not reproduced

The reported "lost delta" race — a queued delta not yet read, then a projector
reset that hands back a stale view — was not reproduced on 0.2 (`s196` green on
0.2). `render/deliver.rs` writes every rendered upsert/remove into `last_sent`
before discarding it under a reset, so a reset always carries the latest rendered
view; the projector-reset path re-renders the dirtied members from the store, so
the `Reset` carries the committed view, never the stale one the unread delta
held. The three reset paths (`deliver.rs` incremental, `repair.rs::resnapshot`,
`connect.rs` attach) were **left as is** — collapsing them onto `resnapshot` was
not rated worth the churn on the delivery hot path this release. Every reset
still costs a full window render — watch `service_engine_resets_total`. No API
change; the write-set check this item once carried is dropped in favour of the
change-detecting mirror kit (see `N3.`, lane: mirror).

#### 12. Dead-letter at the render choke point

A projection failure is now dead-lettered at **every render entry point**, not
only during a normal render pass. The `Renderer` records the poison document
(`DeadLetterSource::Render`, keyed on projector plus key, deduped) the moment
`project` fails, so the attach snapshot (`runtime/connect.rs`), the page render
(`runtime/paging.rs`) and the repair re-snapshot (`render/repair.rs`) all land
the ops row that only the pass path recorded before — those three built a
dead-letter-less renderer. `render/pass.rs::dead_letter_projection` is gone; the
choke point is the `Renderer` itself. No API change.

### lane: write

#### 3. Transition-aware policies, delete policies

`Aggregate` now requires `Clone`, and the pipeline retains the loaded image of
every aggregate it loads. A post-save policy takes `Saved<'_, A> { prior, next,
events }` instead of the bare saved aggregate: `next` is what was just saved,
`prior` is the stored image (`None` on a create), `events` its pending events, and
`Saved::transitioned(|a| …)` reports whether a projection of the two differs. The
same shape drives a new **post-delete** policy: `register_post_delete_policy::<A>`
/ `require_post_delete_policy::<A>`, run before a hard delete, refusing or allowing
it exactly like a save policy. One `PostSave` context serves both hooks; the seams
are declared and boot-checked together (`EngineError::UnhonouredSeam`).

#### N5. `Persistence::delete` and `cx.delete`

`Persistence` gains `delete(conn, key)`, defaulting to a refusal
(`EngineError::DeleteUnsupported`) so a store that never deletes writes nothing.
`cx.delete(&aggregate)` runs the delete policy, issues `Persistence::delete`, then
stages the aggregate's offer and blob reconciliation; a refusal or a delete failure
rolls the transaction back like a save. A raw `DELETE` outside `cx.delete` is a
lock-domain violation (see N6), never an engine path.

#### 6. Offer trigger types

`OfferTrigger<O>: Aggregate` with `key_from(&self) -> Result<KvKey, EngineError>`
(the offer's KvKey) and `row_key(&self) -> Result<…::Key, EngineError>` (the offer
row's typed store key the drain loads), registered by
`Engine::register_offer_trigger::<O, T>()` (refused when `T` is the offer's own
row). A change to a trigger aggregate stages the offer's dirty key, so the offer
re-publishes when a fact it derives from — not its own row — changes; because
`row_key` is typed as the offer row's store key, a trigger on a different
aggregate can no longer silently retract the offer by staging its own key. The
trigger must live in the offer's slice. `Offer` gains `const VERSION: u16 = 1`.
Retires the hand-written `dirty_service` class. Additive.

#### 15. Engine-owned integration outbox

`OUTBOX_TABLE` is now `service_engine.integration_outbox`, created by the engine
migration `9113000024` and owned like every `service_engine.*` table; the six SQL
sites read the constant. A migrating pod adopts any legacy `public.integration_outbox`
rows through `adopt_legacy_outbox`, an idempotent post-migration step (not folded
into the migration: the fresh-database order would leave an orphan `public` table),
which drains the rows and drops the legacy table. The whole adoption runs in one
transaction holding a constant `pg_advisory_xact_lock`, so concurrent `migrate` init
containers on a multi-replica roll serialize: the second pod waits for the first to
commit, then sees no legacy table and returns without a double-`DROP` crash. The
sample and example outbox DDL is gone.

#### N2. `cx.create` refuses an existing key

`cx.create` now loads under the aggregate advisory lock and refuses an already-held
key with `EngineError::KeyReused`, surfaced as the stable code `KEY_REUSED`, instead
of overwriting or hitting a raw unique violation. Two concurrent creates leave exactly
one winner. Removes the principle-5 guard every service hand-wrote.

#### N6. One lock domain; `Persistence::row_lock`

`Persistence::row_lock(conn, table, key)` is a defaulted helper issuing the common
`SELECT … FOR UPDATE` on the aggregate row — the one-line `lock` implementation for a
store that wants a row lock beside the engine's advisory lock. `Persistence::lock`
keeps its no-op default (the advisory lock already serializes every pipeline load).
The one-lock-domain rule holds: every write to an aggregate's rows goes through
`cx.load`/`cx.save`/`cx.delete`, which take the per-key transaction advisory lock,
so two commands on one aggregate serialize even with the no-op `lock`.

### lane: boot

**4.** The boot kit splits into three argv entry points behind one `run_service` /
`BootPlan`. `engine/boot.rs` becomes `engine/boot/{mod,migrate,serve,env}.rs`.
`migrate` runs under the owner role, from `DATABASE_URL_OWNER` **only** (no fallback
to `DATABASE_URL`), retries the owner connection with backoff up to
`EngineConfig::migrate_connect_timeout` (default five minutes), applies the engine
set then the service set on one shared `_sqlx_migrations` ledger — the kit sets
`ignore_missing` on both migrators — waits for the app role to exist, then
`grant_engine_access` + `grant_app_access`, and exits. `serve` runs under the app
role: it refuses to run — `EngineError::MigrationsPending { engine, service }`, logs
`REASON_MIGRATIONS_PENDING` and exits non-zero before it binds any port — while
either set is unapplied (checked through the app role against the shared ledger), then boots
the engine and serves. `schema` prints the SDL and touches no infra. `run_service`
dispatches on argv (`migrate` / `serve` / `schema`; no argv serves).
`EngineConfig::from_env()` reads the ops contract in one place and dispatches on the
same argv the boot kit does: `serve` reads the whole app-env group — `ENGINE_CHANNEL`,
`HOSTNAME` (replaces `POD_ID`), `PORT`/`HOST` (replace `HTTP_ADDR`), `NATS_URL`,
`APP_ROLE`, and the optional session/lease/beat timings; `migrate` reads only
`APP_ROLE` (placeholder channel/pod, default connect timeout) so the migrate init
container needs the owner Secret and `APP_ROLE` alone, never the serve env; `schema`
reads nothing. A service `main` reads no engine env var by hand; `BootPlan.config` is
`from_env()?` plus the service's own `with_service` / `with_blob_storage` /
`with_session_ttl`. `BootPlan`
loses `nats_url` and `app_role` (both now read by `from_env` into `EngineConfig`).
`with_edge_observability` is crate-private: `serve` is the one boot door, and no
observability helper is re-exported at the crate root.

**N1.** `serve` derives `message_retention` from the bound streams' `max_age`
(`inbound::derive_message_retention`, `Nats::stream_max_age`) after the reactions
are registered, taking the max over the subscription streams and refusing an
unlimited stream. A service no longer sets `with_message_retention` by hand, and the
pre-boot second NATS connection every adopter opened to read the stream `max_age` is
gone. An explicit `with_message_retention` still wins when larger. The existing
retention-mismatch boot check (`s174`) is unchanged.

**N11.** Shutdown is latching, so a stop can no longer be lost. The engine's internal
stop signals were bare `tokio::sync::Notify`: `notify_waiters()` wakes only the tasks
already parked on the signal and stores nothing, so a worker that had not yet reached
its first poll when shutdown was raised never saw it, and `run` then blocked forever
joining it. A pod shut down shortly after boot could hang until the kubelet's
`terminationGracePeriod` expired and SIGKILL landed; the scheduled-message loop was
the one this bit most often, because it is spawned last and joined first.
`service_engine::stop::Stop` replaces `Notify` on every internal stop path — an
atomic latch beside the notify, where `stop()` raises the latch before waking and
`stopped()` reads it on its first poll — so the signal is level-triggered and the
order of stop and first poll no longer matters. Public signatures that take a stop
handle move from `Arc<Notify>` to `Arc<Stop>`: `AccumulatorRuntime::run`,
`Beat::run`, `MirrorSupervisor::start`, `SessionRuntime::run` and `graphql::serve`.
`Engine::shutdown_handle()` is unchanged.

**14.** `readiness` re-exports `Readiness` / `ReadinessHandle` / `readiness_route`
from `br-util-axum-readiness = "v1.3.0"`; the engine holds no copy. Paths are kept,
and the shared crate's `readiness: UP` / `readiness: DOWN` tracing wording — the one
the black-box battery greps — is the wording the engine now emits.

### lane: chart

- **Library chart `br-engine-service`, ops contract v1.** The engine now
  publishes the shared deployment topology as a Helm **library** chart
  (`charts/br-engine-service/`, `type: library`) on its own version line starting
  at `1.0.0` — the crate version appears nowhere in the chart. Named templates
  render the topology from a thin chart's values: `br-engine-service.deployment`
  (`Recreate`; init container `migrate` = the service image with argv `migrate`
  and the owner Secret + `APP_ROLE` mounted there only; main container argv
  `serve` with the app env only; `readinessProbe /readyz`, `livenessProbe
  /livez`, one `http` port bound to `PORT`; `HOSTNAME` from `metadata.name`),
  `.service`, `.serviceaccount`, `.pdb`, and `.networkpolicy` (ingress selectors
  are values; the chart names no namespace). Postgres DSNs are read whole from a
  Secret (`DATABASE_URL`, `DATABASE_URL_OWNER`) — no password interpolation, a
  role password with a URL-reserved character must never be interpolated into a
  DSN — and `TRUSTED_NETWORK_HOSTS` carries the per-host plaintext opt-out. `charts/br-engine-service/ci/thin-example/` is the fixture
  thin chart (one dependency on the library plus values) and the shape a
  downstream thin chart takes.
- **Chart major = ops contract version, under its own name.** Chart major 1 is
  ops contract v1. A change to any contract row (an entry point, an env var name,
  a probe path, the roll strategy) is a chart **major** shipped under a **new
  chart name** (`br-engine-service-v2`); the old chart keeps serving old images.
  `check-chart-version.sh` fails a `charts/**` change that does not bump
  `Chart.yaml` `version`, but it cannot tell a minor from a contract-breaking
  major — the README states the rule and the reviewer enforces it.
- **CI.** `ci.yml` gains a `chart` job (`helm lint` the library, `helm dependency
  build` + `helm lint` + `helm template` the fixture — asserting a Deployment with
  init `migrate`, main `serve`, probes `/readyz`/`/livez`, `Recreate` — then
  `check-chart-version.sh`). New `chart-release.yml` packages and pushes the chart
  to `oci://ghcr.io/botresources/charts/br-engine-service` and tags
  `chart/br-engine-service/v<version>` on the first `main` push that changes
  `Chart.yaml` `version`, independent of `release-tags.yml`. The README gains the
  "Ops contract v1" section.
- **Engine crate: no change.** This lane ships no Rust; there is no semver break
  and no adopter code migration. The GitOps follow-up (thin charts per engine
  service, the Warehouse chart-path and library-OCI subscriptions, the `helm-update-chart`
  promotion steps, deletion of the hand-written templates) is a separate,
  sequenced-after change; service crates ship no chart.

### lane: cohort

#### 5. Structured cohort descriptor

- `Cohort { dimension, value }` with `CohortValue::{Uuid, Text, Bool, Int}` (`#[non_exhaustive]`) replaces `CohortKey::of`. A cohort is a `(dimension, value)` the service names — `Cohort::uuid("manager", id)`, `Cohort::text`, `Cohort::flag("public", true)`, `Cohort::int` — and `Cohort::key()` is its routing image (tag + dimension + typed value bytes).
- `Visibility::{cohorts, memberships}` return `Vec<Cohort>`, and `CohortIndex::keys_in_cohorts` takes `&[Cohort]` and binds against each row's **natural columns** through the helpers `Cohort::uuids`/`texts`/`holds` (`WHERE manager_id = ANY($1) OR $2`) — the shadow `cohort_key bytea` column and its writer are gone from the sample.
- `CohortKey::of` is removed; `CohortKey::principal` (the RLS render group) stays. The cohort-column rule text moved out of `persistence.rs` into the README H5 paragraph.
- Honest line: this does not change the windowed views of a service whose visibility is clock-windowed — a time-windowed active link is a service-side write (`end_date IS NULL`), not an engine cohort, and the engine will not schedule a midnight wake for one.

#### N8. DB-backed inverse for link-table dependencies

- `Inverse` gains `Lookup(InverseLookup<K>)`: the store answers "which keys depend on this foreign key" with a query, so a dependency held in a **link table** — not derivable from the key (`Keys`) nor a predicate over the window (`Query`) — declares its inverse instead of falling back to a `projector_reset`. The engine resolves each lookup once per impact with a pooled connection at the pass boundary (`render/pass.rs`), then routing stays synchronous and treats it as `Keys`.
- The four inverses are now `Keys`, `Query`, `Lookup`, `None`. A `view::Projector` declares one through `fn inverse(foreign) -> Inverse` (default `None`); the low-level `projector::Projector` already carried the hook. The engine names no producer — the mirror namespace is the service's.

### lane: mirror

- **7. Engine offers publish a manifest; consumer verdict at scan.** An offer leader now writes `OfferManifest { prefix, version }` under `manifest_key(prefix)` — a sibling key outside the data prefix — at every reconcile (CAS, idempotent), carrying the new defaulted `Offer::VERSION`. A consuming mirror reads that manifest at scan (boot and every periodic reconcile) against `Consumed::manifest()`: a version or prefix mismatch dead-letters the whole prefix once (`DeadLetterSource::Mirror`, keyed `(mirror, prefix)`) and leaves that prefix's shadow empty (`known_*` follows to empty) — at scan and, once rejected, the watch path drops that prefix's puts too so it stays empty until the next reconcile; an absent manifest is the pre-0.3 producer case and is applied silently; readiness is untouched. A consumed prefix must end with `/` (refused at registration through `Mirror::validate`) so `manifest_key` is a true sibling outside the data prefix, and the manifest is read from the consumption's own bucket, not always `PUBLISHED_LANGUAGE`. The consumption carries the manifest key as a `Result`, so a non-`/` prefix surfaces `EngineError::Config` from `validate` rather than panicking at `consume`. This closes the 0.2 disclosure that the manifest verdict was declared but unwired. It removes no guard on a non-engine producer, which has no engine manifest — those are covered by `wire_version` (N9).
- **N9. Per-value `wire_version` on `Consumed`.** `Consumed::wire_version(&value) -> Option<u16>` (defaulted `None`) lets a consumer of a non-engine producer declare the producer's per-value version field. The engine compares it to `Consumed::VERSION` at scan and at watch when it returns `Some`; a mismatch dead-letters that one value (keyed `(mirror, key)`), leaves its shadow row absent, and projects the rest of the prefix; `None` is never judged. Readiness is untouched. Not a manifest — the producer's shape is the consumer's declaration, so nothing producer-specific enters the engine.
- **8. `require_key` on a mirror.** `Mirror::require_key::<C>(key)` names one key under `C::PREFIX` as configuration; `validate` refuses a key outside the prefix (`EngineError::RequiredKeyOutsidePrefix`), a required key whose prefix names no `consume::<C>` on the mirror, and a syntactically invalid key (`EngineError::Config`), so a typo'd generic or key fails boot instead of holding readiness DOWN forever. The mirror is not ready (`REASON_REQUIRED_KEYS`, `/readyz` naming `mirror: key`, `service_engine_dependency_up{dependency="required_keys"}`) until the key is present in the shadow, evaluated after every scan and watch event through a `watch::Receiver` beside the mirror-health board. Nothing is dead-lettered, no restart budget burns, the mirror stays converged and keeps projecting. A mirror that declares no required key is never affected.
- **N3. Change-detecting `upsert` / `replace` in the mirror kit.** `Projection::upsert` compares the stored row (`INSERT … ON CONFLICT DO UPDATE … WHERE row IS DISTINCT FROM excluded RETURNING (xmax = 0)`) and returns `Written::{Inserted, Changed, Unchanged}`; `Unchanged` stages no impact. `replace` diffs the incoming key set against the scope's previous keys and stages an impact only for a key that entered or left the set: its granularity is the key set, so a `replace` scope carries keys-only link rows and per-value diffing is `upsert`'s job. This retires the hand-written roster comparison every adopter carried and is the replacement for the dropped write-set check of item 2.
- **N10. The watch advances the per-bucket read boundary over sibling keys.** The mirror watch now advances its committed watermark to the stream revision of every message on the bucket, projecting only the keys under `C::PREFIX`. Boot already adopted the whole-stream `last_sequence`; the watch previously advanced only for its own prefix, so any sibling write in the same bucket — the item 7 `OfferManifest` on a bucket the service both offers into and consumes from — pushed `last_sequence` past a boundary the leader's watermark never reached, and a standby that read afterward stayed `NotReady { reason: "waiting for the registered mirrors to converge" }` forever. A wire-version-rejected value now advances the same boundary. Convergence (s191) held in-process; this closes it for a second pod on such a bucket.

### lane: graphql

- Subscription-union envelope types are derived from the SDL (item 10). `SchemaSlices::verify` and `SliceFragment::derive` exempt the object members of any union that also lists `LanesPaused` and `LanesResumed` — the reactive delta envelope the `subscription_union!` macro always emits — so two subscription slices sharing one delta union compose with no synthetic slice claiming the `*Payload` types. The synthetic `reactive` slice is gone from the sample.
- Coded refusals on queries and subscriptions (item 11). `graphql::coded_error(code, message)` and `graphql::forbidden()` (code `FORBIDDEN`) are new, re-exported at `service_engine::` and `service_engine::graphql::`; `mutation_error` is now implemented over `coded_error`. A resolver refusal rides async-graphql's own framing — HTTP `200` with `errors[].extensions.code` on a query, and a `next` payload carrying the error then `complete` on a subscription open, never a transport `error`.

### lane: metrics

- **13. Leader gauge per leased loop.** One `service_engine_leader{kind,name}` gauge (`metrics::LEADER`) reports whether this pod holds a leased loop's lease: `kind` is `relay`, `cron`, `offer` or `mirror` and `name` is the slot, `1` on the holder and `0` on a standby, carrying the usual `service`/`pod` identity labels. `observe::record_leader(kind, name, holder)` sets it. It replaces the three unprefixed per-loop names proposed in #128 with one fact keyed by `kind`. The relay and cron loops record it from the shared slot claim (`housekeeping/leader/mod.rs`: a won claim is `1`, a lost claim `0`, so a standby that competes reads `0` and a leader that stops winning a slot reads `0` on its next claim); the offer records it at its singleton claim and every renewal (`offers/leader.rs`); the mirror records it every beat from `on_beat` (`mirror/runtime/lead.rs`), so a standby holds `0` and a failover moves the `1` to the pod that takes the expired lease. The gauge is a level, so a stale value self-corrects on the next claim or beat and a gracefully finished process stops emitting when its scrape stops.

### lane: prefix

- **Mandatory root-field prefix.** `compose_service!` now takes a mandatory `prefix = <snake_ident>;` after `principal`; a block without it does not compile. Every root field a service exposes must be `<prefix><UpperName>` — `RootPrefix::from_snake` (re-exported at `service_engine::` and `service_engine::graphql::`) validates the ident (non-empty, lowercase-first, `[a-z0-9_]`, no leading/trailing/double `_`, ≤ 40 chars) and derives the lowerCamel prefix the field carries; `RootPrefix::owns(field)` is true iff `field` is the prefix followed by one uppercase ASCII letter and any tail (a bare-prefix field is not owned). `Engine::declare_root_prefix` records it (a second, different value → `EngineError::RootPrefixRedeclared`), `compose_service!`'s generated `register` declares it before any slice, and a service that hand-registers fragments must call it itself. `SchemaSlices::assemble(fragments, prefix)` and `verify(sdl)` refuse a root field outside the prefix (`RootFieldOutsidePrefix`) and refuse registered fragments with no prefix declared (`RootPrefixUndeclared`). New boot errors: `RootPrefixInvalid`, `RootPrefixUndeclared`, `RootPrefixRedeclared`, `RootFieldOutsidePrefix` — all fail the boot before the pod serves. The gate is the only defense against two plain-SDL services shadowing one root: the gateway composer merges a duplicate plain-SDL root silently and routes it to one graph (probe recorded in the plan), so the engine refuses the duplicate at boot instead.
- **Library-slice embed.** `compose_service!` gains a `slice <name> ["feat"] from <lib>::<macro> { … }` arm: the named library macro is invoked at `(prefix = <prefix> ; principal = <P>)` inside `pub mod <name>`, contributing its own root objects and `register`. `service_engine::pastey` is re-exported so a library writes `::service_engine::pastey::paste!` without its own dependency.
- **Generic `gated!` and `subscription_union!` arms.** `gated! { generics [P: Bound] ; Aggregate<P>, Principal ; … }` emits `impl<P: Bound>` blocks so a library writes its affordance gate once, generic over the principal. `subscription_union! { generics [P: Bound] ; view = … ; delta = … ; … }` makes `from_erased`/`from_delta` generic over the principal (the emitted GraphQL types stay non-generic, one name each); a callback resolver calls `Delta::from_delta::<P>(&delta)`. The non-generic arms are unchanged.
- Conformance: `s210` (a fragment root field outside the declared prefix fails the boot loud with slice/field/prefix), `s211` (fragments registered with no prefix declared → `RootPrefixUndeclared`), `s212` (a prefixed service boots, serves `sampleWidget`/`sampleCloseWidget`/`sampleWidgets`, and every composed root field is under the prefix). Every hand-registered root field, resolver method and GraphQL literal in the conformance sample moves under the `sample` prefix, and the example-service under `example`.

### lane: migrate

- **Library migration chain.** `BootPlan` gains `libraries: Vec<LibraryMigrations>`
  (`vec![]` when a service embeds no library). A `LibraryMigrations` names the library,
  the Postgres **schema** it owns, a reserved version **band** (`RangeInclusive<i64>`)
  and its `sqlx::migrate!` set. `migrate` applies engine → each library (in `Vec` order)
  → service on the one shared `_sqlx_migrations` ledger, then grants the app role every
  schema (engine, each library, public); `grant_engine_access` is generalized to
  `grant_schema_access(pool, schema, role)`. A library's first migration creates its own
  schema in the service database (`CREATE SCHEMA IF NOT EXISTS …`) — a schema inside the
  service's own database is not infra. A cross-schema foreign key from a service table
  into a library schema is supported by the fixed order.
- **Boot-time validation before any SQL** (`libraries::validate`, run by both `migrate`
  and `serve`): a library schema that is not a lowercase Postgres identifier or that
  shadows `public`/`service_engine` → `EngineError::InvalidSchemaName`; a duplicate
  library name or schema → `EngineError::DuplicateLibrary`; the engine's reserved range
  and every library band must be pairwise disjoint → `EngineError::MigrationBandOverlap`
  naming both; a library migration outside its band → `EngineError::MigrationOutsideBand`;
  a service migration inside a reserved band → `EngineError::ServiceMigrationInReservedBand`.
- **`serve` names the pending set.** `EngineError::MigrationsPending` gains a
  `libraries: Vec<&'static str>` field and its message and `REASON_MIGRATIONS_PENDING`
  now name the engine, library and service sets; `serve` refuses a store where any
  declared library set is unapplied and reports the pending library.
- **Ledger fact (upgrade path).** All sets share one ledger with `ignore_missing`, so
  sqlx applies any set's unapplied versions regardless of `max(applied)`: a 0.2 adopter
  that later declares a library at a low band gets those versions applied below the
  highest applied version on the next `migrate`, nothing to renumber (`s217`).
- `apply_migration_chain` and `ensure_migrated` are public on `engine::boot` for
  black-box conformance (`s213`–`s217`, real Postgres).

### lane: blobs

#### C1. Verified upload — checksum and exact size pinned in the presign

`cx.blob_verified::<Kind>(name, content_type, UploadExpectation { size, sha256 })`
and `blob_owned_verified` stage a blob with a per-call `UploadExpectation`
(`Sha256Digest` — hex/base64, serialized as hex). The POST policy then carries
`{"x-amz-checksum-algorithm":"SHA256"}`, `{"x-amz-checksum-sha256": <base64>}` and
`["content-length-range", size, size]`, and the form carries both `x-amz-checksum-*`
fields, so object storage refuses a mismatching upload (`XAmzContentChecksumMismatch`,
`EntityTooSmall`/`EntityTooLarge`) and the object never lands. `expect.size >
max_bytes` is refused at stage as `EngineError::BlobOverPolicy` before any presign.
Single-part only (≤ 5 GB); no composite/multipart checksum. `BlobPolicy` is
unchanged — the expectation is per call. The unverified `cx.blob` /
`cx.blob_owned` path is byte-for-byte unchanged (open `[0, max_bytes]` range, no
checksum).

#### C2. `BlobReader::head` — a live storage HEAD from a mutation

`engine.blob_reader().head(reference) -> Option<BlobHead>`. `BlobHead { state,
expected, size, etag, sha256 }` fixes its provenance: `state` and `expected` come
from the blob row; `size`, `etag` and `sha256` come **always** from a live
`ObjectStore::head` (never the row's recorded columns), so a mutation running in
the pending window (before the reaper sweeps) reads the storage checksum
immediately, with `state: Pending` and `verified() == Some(true)`. `verified()` is
`Some(size == expected.size && sha256 == Some(expected.sha256))` when an
expectation exists, `None` otherwise. `None` = no row, or no object yet, or the
store unbound. `head` never mutates. `ObjectStore::head` sends
`x-amz-checksum-mode: ENABLED` and reads `x-amz-checksum-sha256`.

#### C3. Post-upload policy — run inside the promotion transaction

`register_post_upload_policy::<Kind>(|Uploaded, &mut PostSave| -> Result<(),
PostUpload>)` / `require_post_upload_policy::<Kind>` (boot-checked like save/delete,
`EngineError::UnhonouredSeam`). One policy per kind (a second is
`EngineError::Config`). The reaper runs it on the promotion connection through a
`PolicyRunner` that builds the same `PostSave` context a reaction gets; the blob
row carries no aggregate key, so the policy finds its referencing key
(`ps.connection()`) and impacts its own view (`ps.impact_caused`), reaching
subscribers as any post-save impact. **The promotion connection carries no
principal and no RLS session context** — a policy reading its referencing row
over an RLS-scoped table must query it with an unscoped statement, or it will see
no row and silently promote with no impact. **Outbound identity of the reaper:** the
reaper holds no principal, so `emit`/`command` from a policy go out as
`service_actor(service)`, correlation = the blob row id, no causation, producer =
service.

**The policy result is now `Result<(), PostUpload>` with two error variants — a
terminal refusal and a retryable fault:**
- `PostUpload::Refused` (via `ps.refuse(reason).into()`) is **terminal**: the
  promotion transaction rolls back, the row goes `failed` with `failed_reason =
  policy:<code>`, the object is deleted, and no impact or outbox row commits.
- `PostUpload::Fault(EngineError)` is a **transient infra fault**: any DB or
  connection call inside the policy propagates with `?` into `Fault` (there is no
  honest reason to `.expect()` an infra read any more). The promotion transaction
  rolls back, the row **stays `pending`**, the sweep counts it in
  `ReaperRound.failures` (logged with the blob id), and the next sweep retries it.
  A panic is folded into the same retry path by `catch_unwind` so the beat is never
  taken down. **There is no attempt budget in 0.3.0**: a deterministic fault
  retries every sweep indefinitely rather than resolving to `failed`; a finite
  delivery budget is a follow-up.

#### C4. Inline vs attachment disposition on the download

`Query::download::<View>(key, reference, Disposition)` and
`BlobReader::download_url(reference, Disposition)` (test-support only; the
production mint is `Query::download`) take a
`blobs::Disposition { Inline, Attachment }` (a GraphQL enum), rendering
`response-content-disposition` as `inline; filename="…"` /
`attachment; filename="…"`. (Named `blobs::Disposition` at the module path — the
crate root `Disposition` is the inbound message-budget one.)

#### C5. Reaper under a leader slot, transactional promotion

The reaper runs under a `reaper:blob` leader slot (`SlotName::Reaper`,
`SlotKind::Reaper`) claimed per interval with the cron idiom
(`claim_current_slot` on a pooled connection, `complete_slot` after), so exactly
one pod sweeps per interval; `ReaperRound` gains `skipped` and `failed`. There is
**no advisory xact lock** — the slot row claim is the whole guard. Promotion runs
the HEAD **outside** the transaction, then a short transaction holds only the
`state='pending'`-guarded `UPDATE … RETURNING` plus the post-upload policy; no
`FOR UPDATE`. The new state model of `service_engine.blob` (`9113000025`) adds
`expected_size`, `expected_sha256`, `etag`, `sha256`, `failed_reason`,
`failed_at`; states are `pending | uploaded | orphaned | failed`. A `failed` row
resolves to no download URL. Failed and orphaned rows are reaped after
`orphan_after` (`reap_detached` generalized): the object is deleted first (an
idempotent DELETE, a no-op on a missing key), the row second, so a crash between
the two leaves a `failed`/`orphaned` row the next sweep finishes — nothing leaks
past the orphan window. The two steps are not one transaction.

#### C6. `BlobConfig.public_endpoint` — browser-facing URLs

`BlobConfig::with_public_endpoint(url)` (validated at boot). `ObjectStore` holds a
second bucket on the public host; the upload POST URL and the download presign use
the public bucket (SigV4 signs `Host`, so a browser needs the public host), while
`ensure_bucket`, `head` and `delete` stay on the internal in-cluster host. The
engine reads no env — the service binary maps `S3_PUBLIC_ENDPOINT` into the config.
For an **unverified** kind the presigned POST stays valid for its full `upload_ttl`
even after the reaper promotes the row, so the object it points at can be replaced
after promotion (a plain property of an S3 presigned POST). This is not defended in
0.3.0: pin anything approval-sensitive with a verified expectation (the checksum is
frozen in the presign, so a replacement cannot match), or keep `upload_ttl` short.
The verified path is immune; nothing in these docs claims an unverified object is
immutable.

#### C7. A pending row is downloadable only once nothing is left to judge

`download_url` (and the gated `Query::download`) mints a URL for a `pending` row
**only** when the row's kind has **no** post-upload policy **and** the row carries
**no** upload expectation; otherwise it returns `None` until the row is `uploaded`.
A verified row (expectation) or a policy-bearing kind therefore hands out no
download in the pending window — before the reaper has verified the checksum or the
policy has judged the upload — and resolves normally once promoted. A plain
unverified kind with no policy stays downloadable the moment its object lands, as
before.

#### C8. `orphan_after` must cover the upload window

At bind, a blob kind whose `BlobPolicy::orphan_after` is shorter than the store's
`BlobConfig::upload_ttl` is refused with `EngineError::BlobOrphanWindowTooShort`
(readiness reason `blobs.policy`). A shorter orphan window would let the reaper
delete an incomplete pending row while its presigned POST is still valid, so a late
upload would land an object no sweep ever sees. Set `orphan_after >= upload_ttl`.

### lane: example-lib

- **The roster slice is extracted into `crates/example-lib-roster`, a standalone
  library crate**, and embedded by `example-service` through the host prefix
  `example` and `BootPlan.libraries` — the first end-to-end proof of the whole
  library path: root fields prefixed by the host, value types unchanged, migrations
  chained, grants and cross-schema access, with the `roster` feature still
  removable.
- **`roster_slice!`** (`#[macro_export]`, arm `(prefix = $prefix:ident ; principal =
  $p:ty)`) emits the root objects, the root methods (`<prefix>_person` →
  `examplePerson`, `<prefix>_roster_deltas` → `exampleRosterDeltas`) and the slice's
  `register`, reaching `pastey` through `::service_engine::pastey`. `example-service`
  embeds it with `slice roster ["roster"] from example_lib_roster::roster_slice { … }`
  and `roster = ["dep:example-lib-roster"]`.
- **`RosterUsers<P>`** is generic over a `RosterPrincipal` bound the host implements
  in one line, and the delta union is declared once, outside the callback macro, with
  the generic `subscription_union! { generics [P: RosterPrincipal] ; … }` arm.
- **`known_persons` moves to schema `roster`** in band
  `9_120_000_001..=9_120_999_999`; `example_lib_roster::migrations()` returns the
  `LibraryMigrations`, and `bin/service.rs` passes it in `BootPlan.libraries`. The
  library owns the read-slice (table, projector, GraphQL, migrations); the host wires
  the directory mirror that feeds `roster.known_persons` and implements
  `RosterPrincipal` — the project-specific seam.
- No engine public-API change; value types keep their names in every embed (R4).

### lane: example-blobs

The reference service (`example-service`, reply slice) now exercises the blob
additions end to end, so a service author has a copyable surface:

- `exampleAttachReply` gains an optional `expected: UploadExpectationInput { size:
  Int!, sha256Hex: String! }`. Present → the mutation stages a verified upload
  (`cx.blob_verified`); absent → the unverified `cx.blob` path, unchanged.
- `exampleReplyDownload` gains a `disposition: Disposition! = ATTACHMENT` argument
  (`INLINE` | `ATTACHMENT`), passed straight to `Query::download` — the hard-coded
  `Disposition::Attachment` is gone.
- The reply slice registers a post-upload policy on its `reply_attachment` kind
  (`register_post_upload_policy` + `require_post_upload_policy`) that finds the
  referencing reply on the promotion connection and impacts the reply view with
  cause `AttachmentUploaded`; the seam is declared, so an unhonoured policy fails
  boot.
- `example-service` e2e scenarios (`tests/scenarios/blobs/{unverified,verified,disposition}.rs`) cover the verified
  round trip against real MinIO (a `head` in the pending window reads the storage
  checksum with `verified() == Some(true)`; a swept run promotes the row to
  `uploaded`), a wrong-checksum upload refused by the store with no download URL,
  and the inline-vs-attachment download disposition on the presigned GET.
- The example `Service` exposes `blob_reader()` and the harness a
  `start_blobs_swept(pod, reaper_interval)` world so a scenario can observe a HEAD
  and a reaper sweep.

### lane: tidy

House-rule debt closed before 0.3.0 ships; no API change, no behaviour change.

- Every source file over ~300 lines across the workspace is split by capability — in `service-engine` (`pipeline/ops`, `engine/register`, `engine/run`, `config`, `pipeline/policy`, `inbound/deadletter`, `dyn_compat/projector`, `runtime`, `mirror/runtime`, `mirror/builder`) and in the `conformance-service-engine` sample library (`sample/engine_pipeline` into per-flavour boot helpers, `sample/mirror` into shared types + publish + projection); `EngineError` stays one file, being a single `#[non_exhaustive]` enum. Public paths are preserved through the module facades. Every pre-existing comment and rustdoc is removed (`///`, `//!`, `//`), including the migration and CI prose; the operative why the CI notes carried now lives in the step names.
- New conformance scenarios: `s226` proves the `Persistence::row_lock` helper takes a real `SELECT … FOR UPDATE` (a second connection cannot lock the row under NOWAIT while it is held, and can once the holder commits); `s227` and `s228` prove the two message-retention boot branches at runtime — an unbounded (max_age 0) bound stream is refused with `REASON_MESSAGE_RETENTION`, and, over a stream whose max_age (5400s) exceeds the default retention (3600s), only the `with_message_retention` override (7200s) makes boot reach ready, so the override is load-bearing (the default would be refused, the mirror of `s174`).
- Duplicate scenario numbers are renumbered so each names one file: the five `s194` files keep one at `s194` and move to `s229`–`s232`; the two `s195` files keep one and move to `s233`.

### Fixed

- conformance `s176` no longer observes the NATS-down verdict through a fixed 2.4 s window (grace starts at disconnect detection, not `nats.stop`): it captures the beat's progress, then bounded-polls housekeeping (heartbeat + leader slots advance) and the readiness handle (`REASON_NATS_UNREACHABLE`) to generous deadlines, failing only on the deadline. Test-only; the engine is unchanged and matches the readiness contract ("the `nats_grace` probe alone takes the pod DOWN").

### Adopter migration

- prefix: add `prefix = <snake_ident>;` after `principal` in `compose_service!` and rename every root method to `<prefix>_<method>` (the served field becomes `<prefix><Method>`; a method named exactly the prefix is refused). A service that hand-registers fragments (no `compose_service!`) must call `engine.declare_root_prefix(RootPrefix::from_snake("…")?)` before `run`. `SchemaSlices::assemble` takes a second `Option<&RootPrefix>` argument. Clients regenerate from the new SDL; the gateway supergraph changes once at the first deploy.
- write: `Aggregate: Clone` — derive `Clone` on every aggregate.
- write: a post-save policy takes `Saved<'_, A>` — `saved.next` is the former saved-aggregate argument and `saved.prior` the stored image (`None` on a create); read the transition with `saved.transitioned(…)`. Add `register_post_delete_policy` where you hard-delete, implement `Persistence::delete` on stores that delete and route hard deletes through `cx.delete`, and keep only the same-content idempotent ack of the hand-written `cx.create` key guard, through the locked `cx.load` — drop its different-content branch and any raw-connection read, the engine refuses a different-content reuse with `KEY_REUSED`.
- write: `OUTBOX_TABLE` is now `service_engine.integration_outbox`; never create `integration_outbox` yourself again — keep an existing `*_outbox.sql` only where a database has already applied it (its rows are adopted at migrate time, then the legacy table is dropped).
- boot: `BootPlan` loses `nats_url` and `app_role`; build `config` as
  `EngineConfig::from_env()?.with_service(..)` (which now reads `NATS_URL` and
  `APP_ROLE`) and drop those two fields from the `BootPlan` literal.
- boot: `run_service` dispatches on argv — the pod runs `migrate` as the library
  chart's init container and `serve` as the main container; e2e harnesses spawn
  `migrate` then `serve` instead of one no-argv process.
- boot: delete every hand-read engine env var from `main`; `EngineConfig::from_env()`
  reads them. Rename `POD_ID` → `HOSTNAME` and `HTTP_ADDR` → `PORT`/`HOST` in the
  deployment; `serve` reads `DATABASE_URL`, `migrate` reads `DATABASE_URL_OWNER`
  (strict, no fallback).
- boot: delete the pre-boot NATS connection that read the stream `max_age` and the
  hand-set `with_message_retention`; `serve` derives it.
- boot: delete `set_ignore_missing(true)` from the service migrator handed to the
  kit; the kit sets it on both sets.
- boot: `with_edge_observability` is no longer public; a hand-wired `main` uses
  `serve` instead.
- cohort: `Visibility::{cohorts, memberships}` now return `Vec<Cohort>` and `CohortIndex::keys_in_cohorts` takes `&[Cohort]` — replace every `CohortKey::of(&[…])` with a typed `Cohort` (`Cohort::uuid("manager", id)`, `Cohort::flag("public", true)`, `Cohort::text`, `Cohort::int`), and bind `keys_in_cohorts` against the row's natural columns with `Cohort::uuids`/`texts`/`holds`.
- cohort: `CohortKey::of` is removed — a routing `CohortKey` derived from a declared cohort is now `Cohort::…(…).key()`; a per-principal render group stays `CohortKey::principal(id)`.
- cohort: drop the shadow cohort-key column with one migration (accounts `cohort_keys bytea[]`, runners `0001_runners.sql:34`); the `CohortIndex` seam now reads the natural columns.
- cohort: `Inverse` gains a `Lookup` variant — a service matching `Inverse` exhaustively adds the arm; a link-table dependency (accounts Orgs) replaces its `projector_reset` fallback with a `Lookup` that queries the link table (defaulted, so consumers that never match `Inverse` need no change).

- migrate (break): `BootPlan` gains `libraries: Vec<LibraryMigrations>` — add `libraries: vec![]` to every `BootPlan { .. }`. `EngineError::MigrationsPending` gains a `libraries` field — exhaustive matches update. A service embedding a library declares each `LibraryMigrations { name, schema, band, migrator }` and passes them here.
- metrics (13, additive): a new `service_engine_leader{kind,name}` gauge; no adopter code change — dashboards and alerts gain the per-loop leader series.
- mirror (N3, break): `Projection::upsert` / `replace` return `Written` instead of `()`; delete the hand-written roster comparison that avoided a spurious impact set.
- mirror (7, additive): delete per-projector version guards on engine-produced offers; the engine now reads the offer manifest and dead-letters a prefix mismatch.
- mirror (N9, additive): delete the hand-written per-value guards on a non-engine producer and implement `Consumed::wire_version` instead.
- mirror (8, additive): a mirror that needs a configuration key declares `Mirror::require_key::<C>(key)` (the key must live under `C::PREFIX`); a `project`-time `Err(EngineError::Config)` guard on that key is now the mirror's readiness declaration.

- graphql (item 10): delete any synthetic slice that claimed the `*Payload` subscription-envelope types — the engine derives them from the delta union that also lists `LanesPaused`/`LanesResumed`.
- graphql (item 11): replace hand-rolled `forbidden()` copies (accounts `graphql.rs`, `context.rs`) with `service_engine::graphql::forbidden()`; a query or subscription refusal that needs another code uses `graphql::coded_error(code, message)`.

- blobs (break): `Query::download` and `BlobReader::download_url` take a third `blobs::Disposition` argument — pass `Disposition::Attachment` to keep 0.2 behaviour.
- blobs (break): `ReaperRound` gains `skipped` and `failed` — a downstream exhaustive struct pattern or literal adds `..` / the fields.
- blobs (break): `BlobConfig` gains the public field `public_endpoint` — construct through `BlobConfig::new(..)` (then `.with_public_endpoint(..)` for a browser-facing host), not a struct literal.
- blobs (additive): `BlobReader::head` reports `size`/`etag`/`sha256` always from a live storage HEAD, `state`/`expected` from the row.
- blobs (behaviour): a post-upload policy emits/commands as the **service** actor (correlation = the blob id, no causation); a message that must carry a human actor stays on the referencing mutation, not the policy.
- blobs (break): a post-upload policy now returns `Result<(), PostUpload>` (was `Result<(), Refused>`). A refusal becomes `Err(ps.refuse(reason).into())`; an infra read propagates with `?` into `PostUpload::Fault` — remove every `.expect()` on a policy's DB call. A `Fault` (or a panic) rolls the promotion back, leaves the row `pending` and retries on the next sweep (no attempt budget in 0.3.0); only a `Refused` fails the row.
- blobs (break): a `pending` row of a kind that has a post-upload policy, or a row that carries an upload expectation, mints **no** download URL until it is promoted — a viewer must wait for the reaper. Plain unverified, policy-less kinds are unchanged.
- blobs (break): a blob kind whose `orphan_after` is shorter than the store's `upload_ttl` is refused at bind (`EngineError::BlobOrphanWindowTooShort`, readiness `blobs.policy`); set `orphan_after >= upload_ttl`.
- migrate (break): a library migration that creates a relation outside its declared schema now fails migrate with `EngineError::LibraryMigrationEscapedSchema` (naming the library and the object); a library owns exactly one schema and may not touch `public` or another library's.
- graphql (additive): `with_sdl_route(app, sdl)` is public — a hand-wired `main` that serves through `app` (not `serve`) can mount the `/sdl` edge route itself.

### Replaced or dropped

- `Persistence::lock` keeps its no-op default: the engine's advisory lock serializes every pipeline load. Stores that need a row lock use the new `row_lock` helper (`SELECT … FOR UPDATE` on the row's `id`).
- `Extended` is unchanged; unknown extensions stay denied. A flattened producer is read through a typed struct with `#[serde(default)]` fields.
- No write-set check: the mirror kit stages nothing for an unchanged write — `upsert` diffs the row, `replace` diffs the key set (keys-only link rows).
- `serve` is the one boot door; `with_edge_observability` is crate-private and no observability helper is re-exported.

## 0.2.0 - 2026-09-16

The `services`-rewrite experiment and the Runners adoption proved the engine
composes across independently-authored slices but leaks at the seams: every serious defect was cross-slice wiring a
slice was meant to call and never did. 0.2 makes those seams **declarative** —
a missing interlock is now a loud boot error, not silent nothing — closes the
contained render/parse/observability bugs, reworks the mirror so that empty is a
converged state, and proves the multi-pod fleet behaviour that was untested. The
entry is organised by those items; every public API change is listed with
its one-line fix under **Adopter migration** at the end.

### A1. `Emission::Coalesced` no longer drops the delta cause

The coalesced branch of the render plan hard-coded `cause: None`, so a coalesced
view's subscribers received `Upsert`/`Remove` deltas with no causal attribution
even when the impact carried a `FactRef`. A coalesced delta now carries the cause
of the **last** impact it folds (the latest in the frame that carries one);
causeless impacts contribute none, and a fold with no cause at all stays `None`.
This makes coalesced-with-cause — one delta per key carrying the causing fact —
expressible, which `PerImpact` could not stand in for. Bug fix only, no API
change.

### A2. The boot gate parses the composed SDL with the real GraphQL parser

`SchemaSlices` verification walked the SDL line-by-line and was blind to `"""`
block strings, so a root-field description that wrapped onto a line beginning
`word (` or `word:` was read as a phantom root field and aborted boot with
`EngineError::UndeclaredSchemaMember`. The gate now parses the composed SDL with
the real GraphQL parser, so a wrapped doc-comment can no longer abort boot.
`SchemaSlices::verify_root_fields(sdl)` is renamed `SchemaSlices::verify(sdl)`.
New `EngineError::SchemaParse` is raised if the composed SDL cannot be parsed
during the gate.

### H0. Mirror rework — empty is a converged state

Per the amended doctrine (`docs/service-engine/intent.md`): a consumed prefix
that reads empty is a **converged** prefix with nothing in it — `known_*` follows
the source to empty, at boot or during a run — and a replaced bucket is
first-adoption, never a readiness failure and never an operator SQL. This
withdraws the 0.1.0 rule that an empty consumed prefix held readiness DOWN, which
made the first deploy of every consumer fail (every producer's bucket is empty
until it publishes). A mirror no longer judges the producer's content, no longer
validates the producer's bucket history (`max_messages_per_subject`), and drops
the `ALLOW_EMPTY` flag, the required marker, the stream-identity equality gate
and the history check.

- **Per-bucket stream identity + boundary watermark.** A mirror persists the
  consumed stream's creation identity and the last sequence `S` its read reached
  beside the watermark, committing both with the projections in one transaction.
  The boundary is taken from fresh `get_info()` metadata before the first scan;
  every scanned entry is applied and the watch resumes at `S + 1`. A standby
  reports converged once the leader has committed the boundary the standby's own
  boot read captured, with no broker round-trip per beat. Migration
  `9113000023_mirror_stream_identity.sql` adds the nullable column; existing
  watermark rows keep their revision and adopt an identity on their next read.
- A bucket whose stream identity changed, or whose sequence is below the held
  watermark, is the first-adoption case: full read, reconcile, adopt the new
  identity and boundary. It never holds readiness DOWN.
- Reconciling the persisted projection keys against the snapshot is now the one
  behaviour of every scan, with no opt-in; on a watch event the engine keys the
  change against the shadows both before and after it is applied, so a retract
  whose projection key lived only in the retracted payload still reaches `known_*`.
- No NATS round-trip inside the leader transaction, and no `STREAM.INFO` per beat
  while a standby waits: a slow broker no longer pins the advisory lock and the
  `leader_slot` row.
- A mirror resuming a bucket from sequence zero opens a future-only watch, then
  re-reads the bucket's metadata after the subscription exists and reconciles if
  the bucket gained a sequence — closing the window on the boot where every
  consumer's bucket is still empty.
- A standby no longer stalls a rolling upgrade on a leader watermark that carries
  no identity (a pre-column row is compared by sequence alone and takes an
  identity when this pod holds the lease), and a leader holding an identity the
  standby never read is an adoption (re-read once and wait), not an
  `EngineError::Config` that would count against the supervisor's restart budget.
- A single mirror watch that ends is logged and reopens the watches, instead of
  being dropped silently by `select_all` and leaving that bucket unwatched.
- A shadow keeps the revision it holds and refuses an older one, so the overlap
  between the scan and the watch can never write a stale value over a newer one.
  New `Shadows::put_at`/`remove_at` carry the revision guard (`put`/`remove` keep
  their 0.1.0 unconditional signatures); `KvBucket::entries_with_revisions` reads
  the revision per key.
- `/readyz` names the mirror and its own failure after the fixed operator copy,
  read from one sample of the health board rather than two.

### H1. Boot kit — one call from `main` (subsumes A3)

`run_service(BootPlan { .. })` (re-exported at the crate root) is the one call
from a service `main`. It installs structured JSON logging
(`br-util-observability::init_logging`), short-circuits a `schema` argv
subcommand by printing the composed SDL and exiting without touching infra, runs
the engine and service migration sets and grants the app role under the **owner**
role (`DATABASE_URL_OWNER`) before connecting the RLS-subject **app** pool
(`DATABASE_URL`, role `APP_ROLE`) via `br-util-postgres`, installs the
process-global Prometheus recorder (`init_metrics`), boots the engine and serves.
`BootPlan` carries `component`, `app_role`, the service `Migrator`,
`EngineConfig`, `nats_url`, the three GraphQL roots, `declare_scopes` and a
`register` closure.

- `graphql::with_edge_observability(app, sdl, metrics)` (crate root) mounts
  `/livez` (always 200), `/metrics` (Prometheus text of the engine's metric set)
  and `/sdl` (the composed schema as `text/plain`) beside a service's `app`
  router, and wraps the whole router in the HTTP metrics layer. This closes
  engine defect A3: a deployed pod previously mounted only `/graphql`,
  `/graphql/ws` and `/readyz` and installed no tracing subscriber, so it emitted
  no logs and exposed no `/livez` or `/metrics`.
- The engine gains `br-util-observability` and `br-util-postgres` dependencies
  (both at the pinned `br-rust-common` `v1.3.0`).
- `example_service::db::migrator()` is now public: the reference service hands
  its migration set to the boot kit rather than running it itself.

### H2. Fallible `Projector::project`

`view::Projector::project` and the low-level `projector::Projector::project` now
return `Result<_, EngineError>` (`Result<Out, _>` and `Result<Option<View>, _>`
respectively). A projector that hits a stored document it cannot render — a
nested blob that will not deserialize, a value the view type cannot represent —
returns `Err` instead of panicking. The render pass dead-letters that poison and
takes the existing repair-then-end-session path for the faulted sessions, so one
malformed row no longer takes the pod down.

- `EngineError::Projection { projector, key, source }` reports the projector and
  key that faulted; the erased projector wraps the implementor's error into it.
- `DeadLetterSource::Render` (`"render"`) and `DeadLetters::record_render` land a
  render-frame projection failure in `service_engine.dead_letter` with the
  projector as the row's reaction and the failing key as its subject, deduped by
  a name-based (UUIDv5) message id so a repeatedly failing key folds into one row.
- `SessionRuntime::set_dead_letters` installs the store the render pass records
  poison into; the engine wires the transport-backed store at boot.

### H3. Capability fragments over one aggregate

`SliceFragment::derive::<Query, Mutation, Subscription>(slice)` reads a
capability's root fields and owned object types from its `#[Object]` /
`#[SimpleObject]` impls (through async-graphql's type registry), replacing the
hand-maintained `root_fields` / `types` tables a single-aggregate service used to
carry. **A slice may register several capability fragments over one aggregate** —
one `#[Object]` per capability file, all naming the same aggregate; capabilities
of one aggregate share its owned types, while two *different* aggregates claiming
one type name is a collision. The example `card` slice demonstrates this, split
into `graphql/item.rs` and `graphql/board.rs`.

- `SliceFragment` is now a derived, owned value: its fields are private, it is no
  longer `Copy`, and `SliceFragment::new(slice, &[..], &[..])` is gone.
  `SliceFragment::from_claims(slice, root_fields, owned_types)` is the explicit
  primitive `derive` is built on, for a synthetic fragment whose claims cannot be
  read from a schema.
- The composed-schema boot gate now gates **object types**, not only root fields:
  `EngineError::UndeclaredSchemaType` fails boot when the composed SDL exposes an
  object type that no fragment owns and the engine does not inject. The
  engine-injected object types (mutation ack, lane payloads) are derived from the
  engine's own wrappers and allowed unclaimed.
- `SchemaSlices::add` takes `&SliceFragment` (the fragment is no longer `Copy`).
  Services on the standard boot path (`Engine::run` / `run_with`) need no change;
  the engine calls verification internally.

### H4. Typed consumption — the law and the kit

- **Raw `serde_json::Value` is refused.** A mirror that consumes the bare
  `serde_json::Value` is rejected at `register_mirror` with the new
  `EngineError::RawJsonConsumption`, so the join always reads a typed value. A
  value that is deliberately raw JSON (the producer's own column is JSON) opts in
  with `Consumed::RAW_JSON_ESCAPE_HATCH = true`; a typed struct holding a
  `serde_json::Value` *field* needs nothing. `MirrorReady::{validate, guards}` and
  `ConsumedGuard` expose the check.
- **Declarative `known_*` rows.** A mirror row that implements `KnownRow` (its
  `TABLE`, `NAMESPACE`, key columns and value columns) is written by the engine:
  `Projection::upsert` generates the `INSERT … ON CONFLICT … DO UPDATE` and
  `Projection::retire` the `DELETE … WHERE key = …`, both staging the impact under
  the same foreign key — a projector using it carries no hand-written SQL. New
  public API: `KnownRow`, `Column`, `Bind`, `col`, `Projection::{upsert, retire}`.
  The manual `Known`/`KnownScope` impls (`replace_one`/`replace`/`remove`) stay as
  the escape hatch for a write that is not a plain single-key upsert.
- **Consumed manifest / version.** `Consumed` gains `const VERSION: u16 = 1` and
  `manifest() -> ConsumedManifest`; `ConsumedManifest::accepts` is the pure verdict
  (`ManifestMismatch::{Offer, Version}`) the engine *will* apply to a
  producer-owned manifest at scan and watch — a mismatch dead-letters that key and
  never touches readiness. The scan/watch enforcement that turns a mismatch into a
  per-key dead letter is not yet wired (it lands in the mirror runtime).
- **Project-specific extension pattern.** `Extended<Core, Ext>` composes a
  producer's shared `core` with a project-owned `extension` given as the second
  type parameter; an extension shape this project does not model fails to
  deserialize — an unknown extension is denied, not mirrored as opaque JSON. A
  consumer implements `Consumed` on the concrete `Extended<Core, Ext>` (wrap it in
  a newtype when `Core` is foreign). `example-service` carries the reference
  (`slices::roster::extension`).

### H5. Principal facts → cohorts → populate

- **Read-side cohort seam (`persistence::CohortIndex`).** A store may implement
  `keys_in_cohorts(conn, &[CohortKey]) -> Vec<Key>` so a cohort view's window is
  read with **one indexed query on the cohort column**, only the caller's rows,
  instead of scanning the table and filtering in memory. The rule the seam
  enforces: a cohort key MUST be a stored column on the row (an `org_id`, an
  `is_public`, or a `(row, cohort_key)` index row written on save) — a cohort
  needing a per-row lookup is not a cohort. `CohortKey::as_bytes` exposes the
  lossless key image for binding.
- **`view::cohort_window` / `view::windowed`.** `cohort_window::<V>(cx)` populates
  a view through the `CohortIndex` seam from the caller's `Visibility::memberships`
  (rebuilt from freshly loaded principal facts) and derives the window shape from
  the declaration; `windowed::<V>(keys)` turns a key set into that inferred shape.
- **Inferred window shape (`Visibility::LIVE` + `Visibility::DEPS`).** The
  `Keys`-vs-`Query` choice is derived from the declared visibility, not hand-picked
  per `populate`: a `LIVE` visibility (the default) yields a `Population::Query`
  carrying an `Interest` on the view's noun and `DEPS`, so a newly created
  in-cohort row reaches an open session and a membership change repopulates the
  window; `LIVE = false` is the explicit override for a closed `Keys` snapshot.
  Returning a `Population` directly from `populate` still bypasses inference. This
  closes issue defect #4 (a freshly created key never reached open subscribers
  under a `Population::Keys`/`Fixed` window) — a bug class the engine introduced —
  and is now the engine's own presence-projector idiom.

### H6. `Unrestricted` visibility requires a stated reason

A view that renders every row must name why it needs no cohort gate.
`visibility::Unrestricted` now takes a third type parameter `Why: AccessReason`;
declare the marker with `service_engine::open_access!(pub MyReason = "why no
cohort gate applies");`. The reason is surfaced through
`Visibility::OPEN_ACCESS_REASON` and **refused non-empty at registration**
(`EngineError::EmptyAccessReason`). The guard sits in
`RenderRegistry::register_projector` — the common sink every registration path
funnels through, via the new `projector::Projector::open_access_reason` method
(default `None`, overridden by `ViewProjector` to surface its view's
`OPEN_ACCESS_REASON`) — so a `ViewProjector` handed to `register_projector`
directly is checked exactly like one registered through `register_view`. Opting
out of the cohort gate is now a deliberate, reviewable statement: the second
visibility layer is the engine's Visibility declaration or RLS, never a
hand-written filter.

### B. Declarative cross-slice seams

The design change that decides whether this engine reduces bugs over time: make
missing wiring a boot error rather than silent nothing.

- **Post-save policies (declared subjection).** `Engine::register_post_save_policy::<A>(f)`
  registers a policy the engine runs after every `save`/`create` of aggregate `A`,
  inside the transaction and before the commit. The policy (`Fn(&A, &mut PostSave)
  -> Result<(), Refused>`) is pure domain logic over the just-saved aggregate: it
  stages impacts/commands/events through `PostSave`, or `PostSave::refuse(reason)`
  to roll the write back and answer the mutation with that `Reason` code (a
  refusing reaction is dead-lettered with it). It cannot save, so it cannot
  recurse. This generalises the cross-slice interlocks the 0.1 rewrite left
  unwired — the two known uses are the Services breach interlock and the Runners
  reconcile-journal impact. New public exports: `service_engine::{PostSave,
  Refused}`; new `EngineError::PolicyRefused { code }`.
- **Seam completeness at registration.** `Engine::require_post_save_policy::<A>()`
  declares aggregate `A` *subject* to a post-save policy; `Engine::run` fails at
  boot with `EngineError::UnhonouredSeam { aggregate }` unless some slice
  registered one — the same registration gate the schema type check applies, so a
  missing interlock is a loud boot error, not a silent absent call.

(The read-side declarative seams that belong with B — typed consumption,
principal-facts cohorts, the `Unrestricted` reason, and the inferred window shape
that removes the `Keys`-vs-`Query` choice — ship under H4, H5, H6 above.)

### D. Operator decisions settled

- **Multi-aggregate transaction (not a cascade).** `Ops::load_many::<A>(&keys)`
  loads several aggregates of one noun in one pipeline transaction, each locked for
  the transaction, so a service mutates several aggregates atomically **without a
  global advisory lock of its own**. The keys are deduplicated and locked in
  ascending order of their encoded bytes; that ascending `(store type, encoded
  key)` order is the engine's documented global aggregate-lock discipline (call
  `load`/`load_many` in it to hold different nouns in one transaction), and it
  makes two concurrent multi-aggregate writes deadlock-free. Absent keys are
  omitted, as for a batched read. This is the answer to the synchronous
  cross-slice write that adoption flagged — the Runners retirement-blocker case
  that took a global advisory lock at six sites.
- **Reason codes are `SCREAMING_SNAKE_CASE`.** `Reason::new` validates its argument
  against `^[A-Z][A-Z0-9_]+$` (a leading capital, then capitals, digits or
  underscores); a mistyped literal is a compile error at the `const` site, and a
  code decoded from the wire that fails the shape is rejected at deserialization
  rather than trusted. This aligns the engine with the frozen `br-test-harness`
  `verdict::expect_code_shaped` and with a consumer that assumes the casing. All
  in-tree codes were migrated (e.g. `already_closed` → `ALREADY_CLOSED`). New
  `Reason::parse(&'static str) -> Result<Reason, ReasonFormat>` is the fallible
  sibling of `Reason::new` for a `'static` code whose shape is only known at
  runtime; `service_engine::{ReasonFormat, is_reason_code}` are exposed. The wire
  encoding is unchanged; only the accepted alphabet narrowed.

### F. Multi-pod behaviour, now verified

- **Multi-pod black-box conformance (`bb08`–`bb11`).** Four scenarios boot two
  instances of the `example-service` binary against one Postgres and one NATS and
  prove the fleet behaviour issue §F flagged as untested: cross-pod reconcile relay
  (a mutation on pod A produces the delta on a session attached to pod B), the
  rolling roll (a client mid-session reconnects to another pod for a fresh `Reset`
  from committed state), outbox exactly-once across pods (a row staged on one pod
  is published once though both relay, guaranteed by the outbox relay's `FOR UPDATE
  SKIP LOCKED` row claim), and mirror leader/standby failover (the leader projects,
  the standby converges to readiness from the KV bucket with no RPC, and on lease
  loss takes over and resumes its watch without a reload). Test-only; no library
  API change. The reference binary now reads `ENGINE_LEASE_MS` / `ENGINE_BEAT_MS`
  to run leader elections under a short lease, so a failover is observable inside a
  bounded wait; unset, both keep the engine defaults (30 s lease, 1 s beat).
- `Reaction::message_id()` exposes the stable inbound message identity to handlers,
  enabling domain deduplication that outlives the engine's delivery-claim
  retention — the durable dedup key a multi-pod fleet needs when a claim has been
  swept.

### Adopter migration

Every public API change in 0.2.0, with its one-line fix. `EngineError` is
`#[non_exhaustive]`, so its new variants (`PolicyRefused`, `UnhonouredSeam`,
`Projection`, `SchemaParse`, `UndeclaredSchemaType`, `RawJsonConsumption`,
`EmptyAccessReason`) are not adopter breaks on their own.

- **`Reason::new` casing (D).** Rename every reason-code literal to
  `SCREAMING_SNAKE_CASE` — a lower-case literal that compiled under 0.1 is now a
  compile-time panic in `Reason::new`. Any client, test or fixture that matched a
  reason code as a string (e.g. `"already_closed"`) must match the upper-case code.
- **`Projector::project` is fallible (H2).** Wrap every `project` return in
  `Ok(...)` — `Ok(view)` for `view::Projector`, `Ok(Some(view))` / `Ok(None)` for
  the raw `projector::Projector`; a total projection that cannot fail never returns
  `Err`.
- **`DeadLetterSource` gains `Render` (H2).** An exhaustive match on
  `DeadLetterSource` gains a `Render` arm.
- **`Unrestricted` third type parameter (H6).** Declare a marker with
  `open_access!(pub MyReason = "…")` and change `Unrestricted<Row, Principal>` to
  `Unrestricted<Row, Principal, MyReason>`; the marker's visibility must be at
  least the view's, and an empty reason is refused at `register_view`.
- **`SliceFragment` is derived and no longer `Copy` (A2, H3).** Build fragments
  with `SliceFragment::derive::<Q, M, S>(slice)` (use `EmptyMutation` /
  `EmptySubscription` for an absent slot), or `SliceFragment::from_claims(slice,
  root_fields, owned_types)` for a synthetic one; `SliceFragment::new(..)` is gone.
  `SchemaSlices::add` now takes `&SliceFragment`.
- **`SchemaSlices::verify_root_fields` → `verify` (A2).** Rename the call;
  services on the standard boot path (`Engine::run` / `run_with`) call nothing here
  and need no change.
- **Typed consumption (H4).** A mirror that registered a `Consumed for
  serde_json::Value` must type the consumed value or set
  `Consumed::RAW_JSON_ESCAPE_HATCH = true`; every typed consumer is unaffected.
  `Consumed::VERSION` defaults to 1, so existing `Consumed` impls compile
  unchanged.
- **Declared post-save subjection (B).** A slice that declares
  `require_post_save_policy::<A>()` must have some slice register one with
  `register_post_save_policy::<A>(..)`, or boot fails with
  `EngineError::UnhonouredSeam`. Purely additive otherwise.
- **Inferred window shape default (H5).** The window shape now derives from the
  declared `Visibility::LIVE` (default `true` → a live `Population::Query`). A view
  that intends a closed `Keys` snapshot must set `LIVE = false`, or return a
  `Population` directly from `populate` to bypass inference.
- **Mirror stream identity (H0).** Migration `9113000023_mirror_stream_identity.sql`
  adds a nullable column; existing watermark rows adopt an identity on their next
  read — no manual step. Any use of the removed `ALLOW_EMPTY` flag or the required
  marker is deleted. `Shadows::put`/`remove` keep their 0.1.0 signatures;
  `put_at`/`remove_at` and `KvBucket::entries_with_revisions` are additive.
- **Boot kit (H1).** A service `main` that hand-wired `connect_pool` +
  `Engine::run_with`, re-added `br-util-observability`, a `/sdl` route, a `schema`
  subcommand and the owner→migrate→grant sequence collapses to a single
  `run_service(BootPlan { .. })` call (see `crates/example-service/src/bin/service.rs`).

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
