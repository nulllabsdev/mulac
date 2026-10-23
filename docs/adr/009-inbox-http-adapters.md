# ADR-009: Shared Inbox HTTP API with Poem and Axum Adapters

**Status:** Accepted  
**Date:** 2026-10-22

## Context

Applications expose two inbox operations over HTTP: storing one message
(`POST /inbox`) and turning one batch of stored messages into queued commands
(`POST /inbox/run`). Each application reimplemented the same request types,
validation, envelope conversion, `spawn_blocking` calls, inbox assembly and
error mapping. The offers service did this with Poem; other services use Axum.

Duplicated contracts drift. Framework-specific parsing also differs: Poem
coerces JSON numbers into string fields unless told otherwise, and Axum's JSON
extractor answers some invalid bodies with `422` instead of `400`.

## Decision

Split inbox HTTP support into three crates in the `libs` workspace:

| Crate             | Responsibility                                                                                  |
|-------------------|-------------------------------------------------------------------------------------------------|
| `inbox_api`       | Framework-free contract, validation, `InboxService`, optional Tokio wrapper, PostgreSQL assembly |
| `mulac_poem`      | Poem/OpenAPI request and response types, `InboxAdapter`, error conversion                       |
| `mulac_axum`      | Axum extractors, `InboxAdapter`, `IntoResponse` conversions, optional Utoipa schemas            |

- The shared crate depends on `mulac-inbox` and `mulac-commanding` only. It does
  not depend on the kernel or on any HTTP framework.
- Adapters parse request bodies into JSON values with their framework, then
  delegate field parsing and validation to the shared crate. Business rules are
  never expressed in framework attributes.
- `InboxService::from_postgres` (feature `diesel`) builds the recorder and an
  inbox consumer whose command gateway only records commands. Running the inbox
  never executes commands.
- `AsyncInboxService` (feature `tokio`) owns the `spawn_blocking` calls. It uses
  the caller's runtime and creates no runtime, pool or worker.
- Applications keep ownership of paths, authentication, middleware, database
  configuration, migrations and server lifecycle.
- Storage, deduplication, reservation and retries stay in `mulac-inbox`.

### HTTP contract

| Case                               | Response                                    |
|------------------------------------|---------------------------------------------|
| Valid submission                   | `202` `{"message_id":"<uuid>"}` after storage |
| Missing `message_id`               | UUID v7 generated                           |
| Duplicate `message_id`             | `202`; original row unchanged               |
| `payload`                          | Required; any JSON value including `null`   |
| `routing_key`                      | Required string with non-whitespace text    |
| Invalid fields or malformed JSON   | `400`                                       |
| Missing or non-JSON content type   | `415`                                       |
| Storage failure                    | `500` `inbox message could not be stored`   |
| Blocking task failure              | `500` with a fixed message                  |
| `limit`                            | Required integer from `1` to `1000`         |
| Batch result                       | `200` `{"processed":N\|null,"errors":[...]}` |

Unknown fields are ignored. Submission errors never include storage or panic
details; batch responses keep reporting consumer error messages.

## Consequences

**Benefits:**
- One implementation of the contract, tested once and reused by both
  frameworks through shared fixtures.
- Applications declare two endpoints and delegate.
- Database-free consumers can use the contract and service with test doubles.

**Trade-offs:**
- Two more crates to version and publish, in dependency order after
  `mulac-inbox`.
- Framework-generated wording for malformed bodies differs between Poem and
  Axum; statuses and Mulac's own messages do not.
- Dropping an HTTP request does not cancel blocking database work that has
  already started.

## References

- [ADR-006](006-cargo-feature-flags.md) — feature flags for infrastructure.
- [ADR-008](008-arc-dyn-trait-dependencies.md) — shared port dependencies.
- [Inbox spec](../inbox-spec.md).
