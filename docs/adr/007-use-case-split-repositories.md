# ADR-007: Use-Case-Split Repositories and Storage Adapters

**Status:** Superseded by ADR-012; migration is staged.
**Date:** 2026-05-12

## Context

Early designs often reach for a single repository type (e.g. `InboxRepository`) that implements every database operation for a component. This is convenient initially but causes problems at scale:

- A test that exercises the recording use case is forced to stub every method of the omnibus repository, including reservation and processing methods it doesn't use.
- The single repository accumulates unrelated port implementations, making it harder to see which use case uses which ports.
- When a new use case is added, the existing repository grows rather than a new, focused one being created.

## Decision

Each use case owns its own repository and storage adapter.

**Repository** (feature layer): named after its use case, depends only on the ports that use case needs.

```
record_messages/InboxRecorderRepository  →  InboxStorePort
inbox_consumer/InboxConsumerRepository   →  InboxReservePort + InboxProcessPort
stale_reservation_sweep/(inline)         →  InboxSweepPort
```

**Storage adapter** (infra layer): named after the responsibility it implements, not the use case.

```
InboxStoreStorage    →  implements InboxStorePort
InboxConsumerStorage →  implements InboxReservePort + InboxProcessPort + InboxSweepPort
```

A single storage struct may implement multiple ports if those ports are tightly coupled at the SQL level (e.g. reservation and processing both touch `inbox_entries`). However, it should never implement ports from *different* use cases that have no SQL coupling.

The constructor pattern for each use case:

```rust
// In the feature module:
pub fn repository(database_url: String) -> Result<InboxRecorderRepository, String> {
    let pool = build_pool(&database_url)?;
    let store = Arc::new(InboxStoreStorage::new(pool));
    Ok(InboxRecorderRepository::new(store))
}
```

## Consequences

**Benefits:**
- Each repository has a small, clear interface — easy to stub in tests.
- Adding a new use case means creating a new repository file; existing repositories are untouched.
- Use-case boundaries are visible in the file structure.

**Trade-offs:**
- Multiple storage adapters may implement overlapping SQL queries (e.g. both touch `inbox_entries`). This is intentional — they serve different use cases and should not share implementation through inheritance.
- The number of types grows linearly with use cases; this is acceptable because each type is small.

## References

- [ADR-012](012-unified-queue-repositories.md) — the decision that superseded this one
- `libs/inbox/src/infra_diesel/repository.rs` — `DieselPgInboxRepository`, which replaced `InboxRecorderRepository`, `InboxConsumerRepository`, `InboxStoreStorage`, and `InboxConsumerStorage`
- [developer-guidelines.md](../developer-guidelines.md) — "Separate repositories by use case"
