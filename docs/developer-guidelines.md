# Developer Guidelines

These guidelines describe the preferred coding and organization style for this repository.

## Import rules

### Group imports from the same top module

Combine bindings from the same top-level module into a single `use` statement:

```rust
use crate::application::prelude::{InboxError, InboxMessageEnvelope, InboxProcessPort};
```

Prefer this over separate lines:

```rust
use crate::application::prelude::InboxError;
use crate::application::prelude::InboxMessageEnvelope;
use crate::application::prelude::InboxProcessPort;
```

### Force multiline formatting for large import groups

When a `use` group has more than 3 bindings, add a trailing `//` after the last element to prevent rustfmt from collapsing it to a single line:

```rust
pub use crate::application::prelude::{
    InboxError,
    InboxMessageEnvelope,
    InboxMessageMetadata,
    InboxProcessPort, //
};
```

Groups with 3 or fewer bindings may stay on one line.

## Rust style

### Prefer `?` over manual error matches

Use `map_err` with `?` when converting error types:

```rust
let pool = build_pool(&database_url).map_err(|e| e.to_string())?;
```

Prefer this over:

```rust
let pool = match build_pool(&database_url) {
    Ok(pool) => pool,
    Err(e) => return Err(e.to_string()),
};
```

If the surrounding function returns `anyhow::Result<_>`, add context at the boundary:

```rust
let repository = repository(config.database_url)
    .map_err(|e| anyhow::anyhow!("failed to initialize repository: {e}"))?;
```

### Prefer `let else` for required `Option` values

Use `let else` when a missing value should skip or return early:

```rust
let Some(reservation_id) = message.reservation_id() else {
    errors.push(InboxError::MissingReservation { id });
    continue;
};
```

Keep `match` when the error value is needed and the match remains clearer:

```rust
let cmd: Command = match message.try_into() {
    Ok(cmd) => cmd,
    Err(e) => {
        errors.push(e);
        continue;
    }
};
```

### Avoid unnecessary clones

Only clone when a value is reused or shared ownership is required:

```rust
let recorder = OutboxRecorder::new(store);
```

Prefer this over:

```rust
let recorder = OutboxRecorder::new(store.clone());
```

when `store` is not used afterward.

### Choose dependency representation at the boundary

Gateways and recorders take `Arc<dyn StorePort>` and sweepers take
`Arc<dyn SweepPort>`. Consumers use one `Arc<R>` where `R` implements reserve and
process ports. Do not impose `'static` on synchronous code. Async HTTP bridges erase
repository types at the blocking-task boundary, where `'static` is required; there
are no synchronous HTTP services. Dispatcher and publisher dependencies remain
separate. See [ADR-012](adr/012-unified-queue-repositories.md) and
[ADR-014](adr/014-merge-sibling-execution-bridges.md).

## Module organization

### Keep dependency direction clear

Domain rules have no storage or runtime dependencies. Application owns envelopes
and ports. Execution owns the synchronous consumers and dispatchers. Diesel
implements the storage ports. HTTP adaptation validates requests and formats
responses outside application and execution.

### One concrete repository per queue

A queue's `DieselPg…Repository::new(pool)` implements store, reserve, process,
and sweep ports. Share it across the store-port owner and consumer using `Arc`; do not
insert forwarding repository wrappers or create a generic table-driven queue.
Pure timeout policies belong in domain; SQL persists their computed outcomes.

### Keep infrastructure in infra modules

Concrete adapters belong in `infra_*` modules:

```text
infra_diesel/
infra_amqp/
```

Feature modules should express use cases and ports, not database or broker implementation details.

### Keep public API behind `prelude.rs`

External callers should import from the root facade:

```rust
use inbox::prelude::{DieselPgInboxRepository, InboxConsumer};
```

Inside the crate, import through each feature’s prelude rather than its implementation submodules.

## Inbox-specific rules

### Reservation and processing

- Reserving a message owns it by assigning a `reservation_id`.
- Processing must use the reservation ID to prevent double completion/failure.
- A missing `reservation_id` is an error, not a panic.
- Failed messages should be released and scheduled according to retry policy.
- Completed messages should clear reservation ownership unless historical reservation data is intentionally retained.

### Attempt counting

Attempts are incremented when a message is reserved for processing. Failure handling should use the already-incremented attempt count.

### Avoid leaking implementation details

Internal query criteria, database row models, and transport-specific message types should not be part of the public API unless callers need them directly.
