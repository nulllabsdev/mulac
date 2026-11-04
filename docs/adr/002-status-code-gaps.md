# ADR-002: Integer Status Codes with Intentional Numeric Gaps

**Status:** Accepted  
**Date:** 2026-05-12

## Context

Every durable entry type in this system (`InboxEntry`, `CommandEntry`, `EventEntry`, `OutboxEntry`) follows the same lifecycle state machine: `received → reserved → completed/failed → dead`. These states are stored as integer codes in the database.

A naive 0-indexed sequential assignment (0, 1, 2, 3, 4, …) means that inserting a new state between two existing ones forces a renumbering of all downstream codes. Renumbering stored values in a live database without downtime is operationally complex.

## Decision

All status codes use a sparse integer scheme with reserved gaps:

| Code           | State       |
| -------------- | ----------- |
| 0              | `received`  |
| _(1 reserved)_ |             |
| 2              | `reserved`  |
| _(3 reserved)_ |             |
| 4              | `failed`    |
| 5              | `completed` |
| _(6 reserved)_ |             |
| 7              | `dead`      |
| 8              | `archive`   |

Gaps at positions 1, 3, and 6 allow new states to be inserted between existing ones without renumbering any stored value.

The mapping is implemented via an explicit `From<i32>` / `Into<i32>` on the status enum. Unrecognised codes are treated as a conversion error at read time, not panics.

## Consequences

**Benefits:**
- New intermediate states can be added in the future without a data migration.
- The integer representation stored in PostgreSQL never changes for existing states.
- All components use the same sparse scheme, so the pattern is learnable once.

**Trade-offs:**
- The codes appear non-sequential in the database, which surprises readers who don't know this convention — document the gaps prominently in each status enum.
- The reserved gaps are a naming convention only; nothing enforces the reservation at compile time.

## References

- `libs/inbox/src/domain/mod.rs` — `InboxStatus` reference implementation
- `libs/commanding/src/domain/mod.rs` — `CommandStatus` following the same scheme
- `libs/outbox/src/domain/mod.rs` — `OutboxStatus` following the same scheme
