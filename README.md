# br-service-engine

> [!IMPORTANT]
> **This repository is maintained for BotResources and its authorized clients.**
> It is published under Apache-2.0 and made available read-only for visibility
> and dependency consumption. The Apache-2.0 license governs your rights to
> use, modify, and fork the code; the rest of this notice describes our
> operational stance, not a legal restriction.
>
> **We do not accept external pull requests, issues, or support requests.**
> Issues and Discussions are disabled. PRs from accounts that are not on the
> internal contributor allowlist will be closed without review. Forks are
> permitted by Apache-2.0 and we neither monitor nor support them.
>
> - Clients with a commercial relationship: contact your BR account manager.
> - Security reports: see [SECURITY.md](SECURITY.md) (private email channel).
> - This is not a community-supported project. No support is provided through
>   GitHub.

The **reactive personalized delivery and process skeleton** every
[BotResources](https://botresources.ai) service runs on: sessions, cohorts,
impacts, projection, diff, multi-pod fan-out, streaming sources, boot, relays,
cron, and mirror supervision.

> [!NOTE]
> **This tag is the scaffold only.** Version `0.0.0` contains the repository
> skeleton — two empty crates, CI/CD, governance — and **no engine code**. The
> engine ships with **0.1.0**.

## Catalog

| Crate | Role | Status | Docs | Changelog |
|---|---|---|---|---|
| `service-engine` | The engine itself — the reactive personalized delivery and process skeleton a service builds on | Scaffold; ships with 0.1.0 | [README](crates/service-engine/README.md) | [CHANGELOG](CHANGELOG.md) |
| `conformance-service-engine` | Black-box conformance battery for the engine, run against real PostgreSQL 16 + NATS JetStream (no infra mocks) | Scaffold; ships with 0.1.0 | [README](crates/conformance-service-engine/README.md) | [CHANGELOG](CHANGELOG.md) |

## Distribution

Not published on crates.io, shipped as no image and no CLI: **the git tag is the
release**. Both crates share one workspace version and a single tag `v{version}`
ships the set.

`service-engine` is a **normal dependency** of a service:

```toml
[dependencies]
service-engine = { git = "https://github.com/BotResources/br-service-engine", package = "service-engine", tag = "v0.0.0", version = "0.0.0" }
```

`conformance-service-engine` is a **dev-dependency**, never a runtime one:

```toml
[dev-dependencies]
conformance-service-engine = { git = "https://github.com/BotResources/br-service-engine", package = "conformance-service-engine", tag = "v0.0.0", version = "0.0.0" }
```

The `version` beside the `tag` is required, not decoration: a tag-only git
dependency carries a `*` version requirement, which `cargo-deny`'s
`wildcards = "deny"` rejects.

### Pins

The engine has its **own version line**. Each engine minor pins **one exact**
`br-rust-common` tag and one exact `br-e2e-harness` tag; a consumer that mixes
sources will not resolve a single `br-core-*`.

| Engine version | `br-rust-common` | `br-e2e-harness` |
|---|---|---|
| 0.0.0 (scaffold) | `v1.2.0` | `v1.1.3` |

0.1.0 will move both pins forward — to `br-rust-common` `v1.3.0` and
`br-e2e-harness` `v1.2.0`.

## Release process

1. In your PR, bump the workspace `version` in the root `Cargo.toml` and add a
   matching `## X.Y.Z - YYYY-MM-DD` section to `CHANGELOG.md` (plain heading,
   hyphen, no brackets — CI greps that exact form).
2. CI gates the PR: fmt, clippy, tests, MSRV, docs, deny, machete,
   semver-checks, changelog + README pins, shellcheck, secret scan, and the
   conformance battery against real infra.
3. On merge to `main`, the `release-tags` workflow creates the annotated
   `v{version}` tag and the matching GitHub Release (notes lifted from
   `CHANGELOG.md`). That tag *is* the published version.

## Development

```bash
cargo build  --workspace
cargo test   --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt    --all
```

The conformance battery needs real infra (PostgreSQL 16 + NATS JetStream); CI
runs it in a dedicated job.

MSRV: **1.88** (edition 2024). License: Apache-2.0.
