# Architecture Decision Records

| #                                         | Title                                                | Status                |
|-------------------------------------------|------------------------------------------------------|-----------------------|
| [001](001-hexagonal-architecture.md)      | Hexagonal Architecture (Ports and Adapters)          | Accepted              |
| [002](002-status-code-gaps.md)            | Integer Status Codes with Intentional Numeric Gaps   | Accepted              |
| [003](003-uuid-v7-identifiers.md)         | UUID v7 for All Generated Identifiers                | Accepted              |
| [004](004-skip-locked-reservation.md)     | Atomic Reservation via SELECT FOR UPDATE SKIP LOCKED | Accepted              |
| [005](005-jsonb-metadata.md)              | JSONB Column for Flexible Message Metadata           | Accepted              |
| [006](006-cargo-feature-flags.md)         | Cargo Feature Flags for Optional Infrastructure      | Accepted              |
| [007](007-use-case-split-repositories.md) | Use-Case-Split Repositories and Storage Adapters     | Superseded by ADR-012 |
| [008](008-arc-dyn-trait-dependencies.md)  | Arc\<dyn Trait\> for Shared Port Dependencies        | Accepted              |
| [009](009-inbox-http-adapters.md)         | Shared Inbox HTTP API with Poem and Axum Adapters    | Accepted; see ADR-011 |
| [010](010-queue-http-apis.md)             | Commanding, Eventing and Outbox HTTP APIs            | Accepted; see ADR-011 |

## Adding a new ADR

1. Copy the next available number.
2. Create `docs/adr/<number>-<kebab-title>.md` with sections: **Context**, **Decision**, **Consequences**, **References**.
3. Add a row to this table.
4. Link from the relevant guideline or spec document.

- [ADR-011: API and HTTP features on core crates](011-core-http-features.md) — Accepted; supersedes the crate layout in ADR-009/010.

- [ADR-012: Unified queue repositories](012-unified-queue-repositories.md) — Accepted; supersedes ADR-007 and amends ADR-008. Eventing, inbox, and outbox migrated.

- [ADR-013: Merge commanding's execution bridges](013-merge-commanding-execution-bridges.md) — Accepted; amends ADR-011 and ADR-012 for commanding.

- [ADR-014: Merge the other queues' execution bridges](014-merge-sibling-execution-bridges.md) — Accepted; extends ADR-013 to eventing, inbox and outbox, and amends ADR-011 and ADR-012.

- [ADR-015: Report HTTP errors by logged ID](015-http-errors-by-id.md) — Accepted; changes the error text in ADR-009, ADR-010 and ADR-014.
