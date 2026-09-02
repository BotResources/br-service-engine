# conformance-service-engine

The black-box conformance battery for [`service-engine`](../service-engine/README.md),
run against real PostgreSQL 16 and NATS JetStream — no infra mocks.

**Status: scaffold; implementation ships with 0.1.0.** This crate is empty at
`v0.0.0` — it carries no code and no dependency.

A **dev-dependency**, never a runtime one:

```toml
[dev-dependencies]
conformance-service-engine = { git = "https://github.com/BotResources/br-service-engine", package = "conformance-service-engine", tag = "v0.0.0", version = "0.0.0" }
```

---

Part of [`br-service-engine`](../../README.md) · [Changelog](../../CHANGELOG.md) · [botresources.ai](https://botresources.ai)
