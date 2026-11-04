# ADR-005: JSONB Column for Flexible Message Metadata

**Status:** Accepted  
**Date:** 2026-05-12

## Context

Each entry type carries routing metadata alongside its payload: fields like `message_id`, `correlation_id`, `source`, and `routing_key`. These fields are:

- Optional — not all messages carry all fields.
- Likely to grow — future transport adapters may add new fields.
- Not used in filtering queries — reservation and sweep queries only filter on `status`, `scheduled_at`, `attempts`, and `reserved_at`.

Options considered:

1. **Separate nullable columns** for each metadata field: strongly typed, easy to query, but schema migrations required for each new field.
2. **JSONB column**: schemaless, no migration needed for new fields, queryable with PostgreSQL JSON operators if needed, serialised and deserialised at the application boundary.
3. **Separate metadata table**: normalised but adds a join to every read and a second write to every insert.

## Decision

Metadata is stored as a single JSONB column (`meta`) on each entry table. The application layer deserialises it into a typed struct (`InboxMessageMetadata`) on read and serialises it on write using `serde_json`.

The JSONB wrapper struct carries the deserialised form:

```rust
#[derive(Queryable, Insertable)]
struct MetadataJsonb(serde_json::Value);
```

The typed application model:

```rust
pub struct InboxMessageMetadata {
    pub message_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
    pub source: Option<String>,
    pub routing_key: Option<String>,
}
```

A missing `meta` column value maps to `None` in the envelope; a present value is deserialised into the typed struct.

## Consequences

**Benefits:**
- New metadata fields can be added to the application model without a database migration.
- The column stores whatever the transport provides; no data is lost if the schema evolves faster than the typed struct.
- Queries on `status`, `scheduled_at`, and similar indexed columns are unaffected — metadata is never a filter criterion.

**Trade-offs:**
- Metadata fields are not individually indexed; ad-hoc queries on e.g. `source` require a GIN index or a full table scan.
- Type safety at the database level is lost — a serialisation bug can write valid JSON that doesn't match the expected struct shape.
- Deserialisation errors surface at read time, not write time; defensive handling is required in the infra adapter.

## References

- `libs/inbox/src/infra_diesel/models.rs` — `MetadataJsonb`
- `libs/inbox/src/infra_diesel/entity.rs` — `InboxEntry`, `NewInboxEntry`
- `libs/inbox/src/application/models.rs` — `InboxMessageMetadata`
