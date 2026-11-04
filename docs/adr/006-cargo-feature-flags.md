# ADR-006: Cargo Feature Flags for Optional Infrastructure

**Status:** Accepted  
**Date:** 2026-05-12

## Context

Each crate in this system has a pure domain/application core and one or more concrete infrastructure adapters (PostgreSQL via Diesel, AMQP via lapin, etc.). Applications that consume a crate may not need all adapters — for example, a test harness might only need the port traits and a test double, with no database dependency at all.

Unconditionally compiling all infrastructure into every crate would:
- Force every consumer to depend on Diesel, lapin, and their transitive dependencies.
- Slow down CI builds for crates that only test domain logic.
- Pull in `unsafe` and native-library dependencies (OpenSSL, libpq) even when unused.

## Decision

Infrastructure adapters are gated behind Cargo feature flags. Feature names follow the convention `infra_<adapter>` or simply the adapter name for well-known ones:

| Feature  | What it enables                                                                         |
| -------- | --------------------------------------------------------------------------------------- |
| `diesel` | queue-specific `DieselPg…Repository`, `DbPool`, r2d2 connection pool, Diesel PostgreSQL |
| `amqp`   | `AmqpWorker`, `AmqpTransport`, `WorkerLoop`, lapin, tokio-amqp                          |

Feature flags apply to both the `Cargo.toml` dependency declarations and the conditional compilation blocks in `lib.rs`:

```toml
[features]
diesel = ["dep:diesel", "dep:r2d2", ...]
amqp   = ["dep:lapin", "dep:tokio-amqp", ...]
```

```rust
// lib.rs
#[cfg(feature = "diesel")]
mod infra_diesel { ... }

#[cfg(feature = "amqp")]
mod amqp_consumption { ... }
```

The application and domain layers are always compiled and carry no feature flags.

## Consequences

**Benefits:**
- Consumers that only need ports and test doubles add no infrastructure weight.
- CI can run unit/domain tests with `cargo test` (no `diesel`/`amqp` features) and integration tests with features enabled separately.
- Binary size is smaller for deployments that omit an adapter.

**Trade-offs:**
- Feature combinations must be tested explicitly; a bug that only manifests with `--features diesel,amqp` can go undetected if CI only tests each feature in isolation.
- The `#[cfg(feature = …)]` guards add visual noise to `lib.rs` and Cargo.toml.
- Consumers must know which feature to enable; this must be documented in the crate's README or SPEC.

## References

- `inbox/Cargo.toml` — `diesel` and `amqp` feature declarations
- `inbox/src/lib.rs` — conditional re-exports
- [inbox-spec.md](../inbox-spec.md) — feature flag table in the spec
