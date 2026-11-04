# ADR-004: Atomic Reservation via SELECT FOR UPDATE SKIP LOCKED

**Status:** Accepted  
**Date:** 2026-05-12

## Context

Every component in this system exposes a `reserve(count)` operation that multiple concurrent workers call simultaneously. Two workers must never claim the same entry. Options considered:

1. **Application-level locking** (Redis or similar): introduces an external dependency and a second failure domain; lock expiry and crash recovery add complexity.
2. **Optimistic concurrency** (version column + CAS update): works but produces many failed retries under high contention; requires retry loops in application code.
3. **PostgreSQL advisory locks**: per-row advisory locks are supported but require explicit lock management and cleanup.
4. **`SELECT FOR UPDATE SKIP LOCKED`**: a PostgreSQL-native mechanism that atomically claims rows and skips rows already locked by another transaction, with no retry loops needed in application code.

## Decision

Reservation queries use `SELECT … FOR UPDATE SKIP LOCKED` inside a single transaction:

```sql
-- Phase 1: select candidates
SELECT id
FROM inbox_entries
WHERE status IN (0, 4)
  AND scheduled_at <= NOW()
  AND attempts < $max_attempts
ORDER BY scheduled_at ASC
LIMIT $n
FOR UPDATE SKIP LOCKED;

-- Phase 2: update claimed rows in the same transaction
UPDATE inbox_entries
SET status = 2,
    reservation_id = $uuid,
    reserved_at = NOW(),
    attempts = attempts + 1,
    updated_at = NOW()
WHERE id = ANY($ids);
```

The transaction commits atomically: either all selected rows are claimed or none are. Workers that reach a locked row skip it immediately rather than blocking — contention resolves itself in O(1) without spin loops.

## Consequences

**Benefits:**
- No external lock store required; the database is the single source of truth.
- Workers do not block each other — `SKIP LOCKED` means a busy row is simply skipped.
- Combining SELECT and UPDATE in one transaction eliminates TOCTOU races.
- Simple application code: no retry loops, no lock TTL management.

**Trade-offs:**
- Ties the storage implementation to PostgreSQL; other databases (MySQL, SQLite) do not support `SKIP LOCKED` in the same way.
- The two-phase query (SELECT then UPDATE) must remain inside a single transaction; splitting them across calls would reintroduce race conditions.
- Under very high contention all workers may find zero rows on a given poll cycle, requiring the caller to implement its own polling interval.

## References

- `libs/inbox/src/infra_diesel/repository.rs` — reference implementation of `InboxReservePort`
- PostgreSQL docs: `SELECT … FOR UPDATE SKIP LOCKED`
- [contracts.md](../contracts.md) — reservation guarantees for each component
