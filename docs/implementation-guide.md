# Crate Implementation Guide

This guide describes how to implement new crates in this repository, using the `inbox` crate as the reference implementation. Follow these patterns when building `write_side` (Command/Event Dispatchers) and `outbox`.

For the architectural rationale behind each pattern, see the [ADR index](adr/).

---

## File structure

Queue crates have private `domain`, `application`, `infra_diesel`, and HTTP
boundaries. Commanding groups its HTTP adapters under `http`; other private file
decompositions are allowed. `pub mod prelude` is the only public module interface.
Application owns both message models and use cases; Diesel entity types remain
in infrastructure. Optional adapters are feature-gated.

---

## Layer rules

### domain.rs

- Contains only models: structs, enums, and their `impl` blocks.
- No external crate dependencies — not even `serde` unless the domain type is directly serialised.
- Status enums use sparse integer codes. See [ADR-002](adr/002-status-code-gaps.md).
- Implement `From<i32>` and `Into<i32>` (or `TryFrom<i32>`) for status enums.

```rust
// Sparse codes: 0, 2, 4, 5, 7, 8. Gaps reserved for future insertion.
pub enum CommandStatus {
    Received   = 0,
    Reserved   = 2,
    Failed     = 4,
    Completed  = 5,
    Dead       = 7,
    Archive    = 8,
}
```

### application.rs

- Contains port traits, application-level envelopes, and the crate's `Error` enum.
- All port traits are async (`async fn` or return `impl Future`).
- Error enum uses `thiserror::Error`. Each variant corresponds to a failure category (storage, transport, conversion, etc.), not to individual functions.
- No concrete implementations — this file has no `use diesel::…` or `use lapin::…`.

Port naming convention:

| Responsibility                  | Trait name                 |
| ------------------------------- | -------------------------- |
| Write a new entry               | `<Component>StorePort`     |
| Reserve entries                 | `<Component>ReservePort`   |
| Mark completed/failed           | `<Component>ProcessPort`   |
| Release stale reservations      | `<Component>SweepPort`     |
| Receive from external transport | `<Component>TransportPort` |
| Acknowledge transport delivery  | `AcknowledgeHandle`        |

### Use-case modules

Recorders delegate directly to `Arc<dyn StorePort>`. Consumers hold one `Arc<R>`
implementing reserve/process ports, plus their independent dispatcher, gateway,
or publisher. Sweepers hold `Arc<dyn SweepPort>`. No synchronous code requires
`R: 'static`; HTTP async bridges erase repository types at the blocking boundary.
Use one queue-specific Diesel repository, constructed from an existing pool.
See [ADR-012](adr/012-unified-queue-repositories.md).

### infra_<name>.rs

- Contains concrete implementations of the port traits defined in `application.rs`.
- Named by adapter, not by use case: `infra_diesel.rs`, not `recording_diesel.rs`.
- One concrete repository per queue implements its four storage ports:

  ```
  DieselPgCommandRepository → implements CommandStorePort + CommandReservePort + CommandProcessPort + CommandSweepPort
  ```

- Reservation uses `SELECT … FOR UPDATE SKIP LOCKED` in a single transaction. See [ADR-004](adr/004-skip-locked-reservation.md).
- Failed transitions apply retry backoff: `attempts × 30 seconds`, capped at 2 minutes. See [contracts.md](contracts.md).
- All generated IDs use UUID v7. See [ADR-003](adr/003-uuid-v7-identifiers.md).

### lib.rs

Re-export everything that external callers need through a single `prelude` module. Nothing else.

```rust
mod application;
mod domain;
#[cfg(feature = "diesel")]
mod infra_diesel;
pub mod prelude {
    pub use crate::application::prelude::*;
    pub use crate::domain::prelude::{CommandStatus, ExtraInfo};
    #[cfg(feature = "diesel")]
    pub use crate::infra_diesel::prelude::DieselPgCommandRepository;
}
```

---

## Database schema

Each component owns one entry table. The schema follows the inbox pattern exactly:

```sql
CREATE TABLE <component>_entries (
    id              BIGSERIAL    PRIMARY KEY,
    status          INTEGER      NOT NULL,
    payload         TEXT         NOT NULL,
    meta            JSONB,
    scheduled_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    attempts        INTEGER      NOT NULL DEFAULT 0,
    reservation_id  UUID,
    reserved_at     TIMESTAMPTZ,
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ
);

-- required indexes
CREATE INDEX ON <component>_entries (status, scheduled_at);
CREATE INDEX ON <component>_entries (status, reserved_at);
```

Metadata is stored as JSONB. See [ADR-005](adr/005-jsonb-metadata.md).

---

## Retry policy

Apply this policy uniformly in `failed()` transitions across all components:

- Delay = `attempts × 30 seconds`, where `attempts` is the value after incrementing on reservation.
- Maximum single-attempt delay: 2 minutes.
- At `max_attempts` (default 6): transition to `Dead` instead of `Failed`.
- Stale sweeps transition to `Failed` or `Dead` according to the timeout policy without incrementing `attempts`.

```rust
let delay_secs = (entry.attempts as i64 * 30).min(120);
let scheduled_at = Utc::now() + Duration::seconds(delay_secs);
```

---

## Cargo.toml checklist

```toml
[package]
edition = "2024"

[features]
diesel = ["dep:diesel", "dep:r2d2", "dep:diesel-derive-newtype"]
amqp   = ["dep:lapin", "dep:tokio-amqp"]

[dependencies]
thiserror = "..."
uuid = { version = "...", features = ["v7"] }
chrono = { version = "...", features = ["serde"] }
serde = { version = "...", features = ["derive"] }
serde_json = "..."
tokio = { version = "...", features = ["full"] }
# infrastructure dependencies are optional:
diesel = { version = "...", features = ["postgres", "chrono", "uuid", "serde_json"], optional = true }
```

---

## Checklist for implementing a new crate

- [ ] Fill in `SPEC.md` before writing code
- [ ] Write `domain/mod.rs`: status enum with sparse codes, entry model
- [ ] Write `application/models.rs`: port traits, envelope types, error enum
- [ ] Write application use cases with specs and direct port dependencies; use generic consumers and one concrete adapter per queue
- [ ] Write `infra_<name>/mod.rs` behind feature flag
- [ ] Wire up `lib.rs` with `prelude` re-exports
- [ ] Add reservation tests: concurrent workers must not claim the same entry
- [ ] Add retry tests: failed entries schedule at correct future time
- [ ] Add stale sweep tests: sweep does not increment attempt counter
- [ ] Update `Makefile` if the crate is new

---

## What not to do

- Do not put SQL in use-case modules. SQL belongs in `infra_*` modules only.
- Import through feature preludes, not private implementation submodules.
- Share a queue repository across its use cases; do not share one generic implementation across different queues.
- Keep concrete storage adapters non-generic; consumers and synchronous HTTP bridges are generic over their reserve/process repository. See ADR-012.
- Do not add status codes sequentially. Leave gaps. See [ADR-002](adr/002-status-code-gaps.md).
- Do not use UUID v4 for generated IDs. Use `Uuid::now_v7()`. See [ADR-003](adr/003-uuid-v7-identifiers.md).
- Do not acknowledge transport delivery before durable storage is confirmed.

---

## References

- [developer-guidelines.md](developer-guidelines.md) — import style, Rust idioms
- [contracts.md](contracts.md) — what each component guarantees to its callers
- [components.md](components.md) — component responsibilities and lifecycle states
- [architecture-spec.md](architecture-spec.md) — end-to-end flows
- [inbox-spec.md](inbox-spec.md) — reference specification
- [ADR index](adr/) — rationale behind every architectural decision
