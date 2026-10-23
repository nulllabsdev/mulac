# Architecture Decision Records

| #                                         | Title                                                | Status   |
|-------------------------------------------|------------------------------------------------------|----------|
| [001](001-hexagonal-architecture.md)      | Hexagonal Architecture (Ports and Adapters)          | Accepted |
| [002](002-status-code-gaps.md)            | Integer Status Codes with Intentional Numeric Gaps   | Accepted |
| [003](003-uuid-v7-identifiers.md)         | UUID v7 for All Generated Identifiers                | Accepted |
| [004](004-skip-locked-reservation.md)     | Atomic Reservation via SELECT FOR UPDATE SKIP LOCKED | Accepted |
| [005](005-jsonb-metadata.md)              | JSONB Column for Flexible Message Metadata           | Accepted |
| [006](006-cargo-feature-flags.md)         | Cargo Feature Flags for Optional Infrastructure      | Accepted |
| [007](007-use-case-split-repositories.md) | Use-Case-Split Repositories and Storage Adapters     | Accepted |
| [008](008-arc-dyn-trait-dependencies.md)  | Arc\<dyn Trait\> for Shared Port Dependencies        | Accepted |
| [009](009-inbox-http-adapters.md)         | Shared Inbox HTTP API with Poem and Axum Adapters    | Accepted |
| [010](010-queue-http-apis.md)             | Commanding, Eventing and Outbox HTTP APIs            | Accepted |

## Adding a new ADR

1. Copy the next available number.
2. Create `docs/adr/<number>-<kebab-title>.md` with sections: **Context**, **Decision**, **Consequences**, **References**.
3. Add a row to this table.
4. Link from the relevant guideline or spec document.
