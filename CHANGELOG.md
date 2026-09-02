# Changelog

All notable changes to `br-service-engine` are documented here. The whole
workspace ships **one version**: every crate inherits `version.workspace = true`,
and a single git tag `v{version}` releases the set. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver.

## Unreleased

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
