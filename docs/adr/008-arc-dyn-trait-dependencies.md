# ADR-008: Arc<dyn Trait> for Shared Port Dependencies

**Status:** Amended by ADR-012 for queue consumers and synchronous HTTP bridges.
**Date:** 2026-05-12

## Context

Repositories and workers need to hold references to port implementations (storage adapters, transport adapters). Two patterns are possible in Rust:

**Generic parameters:**
```rust
pub struct InboxRecorderRepository<S: InboxStorePort> {
    store: S,
}
```
- Monomorphised at compile time — zero runtime cost.
- Makes the containing type generic, which propagates through all callers: `InboxConsumer<S>`, `AmqpWorker<S>`, etc.
- Difficult to store in collections or pass across `async` boundaries without boxing.

**`Arc<dyn Trait>`:**
```rust
pub struct InboxRecorderRepository {
    store: Arc<dyn InboxStorePort>,
}
```
- Dynamic dispatch — small, constant overhead per call.
- The containing type is not generic; it can be stored in `Vec`, passed as `Arc<InboxRecorderRepository>`, cloned cheaply, and sent across `tokio::spawn` boundaries without additional bounds.
- Multiple workers can share one storage adapter without cloning the underlying connection pool.

The system spawns multiple concurrent workers (see `test_app/src/lib.rs`). Each worker needs a `InboxRecorderRepository` that shares the same underlying connection pool. With generics, each worker would require the full generic type in the `spawn` call; with `Arc<dyn Trait>`, a single `Arc::clone` is sufficient.

## Decision

Port dependencies stored inside repositories and workers use `Arc<dyn Trait + Send + Sync>`.

```rust
pub struct InboxRecorderRepository {
    store: Arc<dyn InboxStorePort>,
}

impl InboxRecorderRepository {
    pub fn new(store: Arc<dyn InboxStorePort>) -> Self {
        Self { store }
    }
}
```

Generics are used only when the intent is to make the containing type explicitly generic (e.g., `WorkerLoop<T: InboxTransportPort>` where transport type affects the struct's public API).

## Consequences

**Benefits:**
- Workers and repositories are cheaply cloneable — `Arc::clone` is a reference count increment.
- Spawning `N` workers from a single storage adapter requires one `Arc::clone` per worker, not `N` connection pools.
- No generic parameter pollution through the call stack.
- `dyn Trait` objects are easy to replace with test doubles in unit tests.

**Trade-offs:**
- Dynamic dispatch has a small indirect-call overhead. For this system (I/O-bound, database calls dominate) the overhead is negligible.
- Trait objects require `Send + Sync` bounds for async use, which must be declared on the trait definition.
- Compiler errors from missing trait implementations surface at the `Arc::new(...)` call site rather than as monomorphisation errors, which can be less readable.

## References

- [ADR-012](012-unified-queue-repositories.md) — the amendment for queue consumers and synchronous HTTP bridges
- `libs/inbox/src/http/execution.rs` — `AsyncInboxService::new` takes `Arc<dyn InboxStorePort>`
- `libs/inbox/src/application/stale_reservation_sweep.rs` — `ReservationSweeper` with `Arc<dyn InboxSweepPort>`
- `libs/inbox/src/execution/inbox_consumer.rs` — `InboxConsumer<R>`, generic over its repository per ADR-012
- `kernel/src/lib.rs` — one repository shared through `Arc` clones during persistent startup
- [developer-guidelines.md](../developer-guidelines.md) — "Use Arc<dyn Trait> for cloneable shared dependencies"
