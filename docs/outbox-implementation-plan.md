# Outbox Implementation Plan

## Purpose

This document breaks the outbox implementation into small, independently reviewable steps. Each step is intended to be atomic: it should compile or be explicitly documentation-only, have a narrow write set, and have a clear verification command.

The plan implements [outbox-spec.md](outbox-spec.md) while following the crate conventions in [implementation-guide.md](implementation-guide.md), [command-handling-spec.md](command-handling-spec.md), and [inbox-spec.md](inbox-spec.md).

## Target End State

The `outbox` crate exposes a single `prelude` facade with:

- Domain models: `OutboxEntry`, `NewOutboxEntry`, `OutboxStatus`
- Application models: `NewOutboxEnvelope`, `NewOutboxMetadata`, `OutboxEntryEnvelope`, `OutboxEntryMetadata`, `OutboundMessageEnvelope`
- Ports: `OutboxStorePort`, `OutboxReservePort`, `OutboxProcessPort`, `OutboxSweepPort`, `OutboxPublisherPort`
- Use cases: `OutboxRecorder`, `OutboxConsumer`, `ReservationSweeper`
- Optional adapters:
  - `diesel`: PostgreSQL storage with idempotent recording, reservation, completion, failure, dead-lettering, and stale sweep
  - `amqp`: AMQP publisher adapter that waits for broker acceptance

## Cross-Cutting Rules

Apply these rules to every step:

1. Keep SQL and concrete I/O in `assembly/infra_diesel.rs` or transport adapter modules only.
2. Keep use-case modules dependent on ports, not concrete adapters.
3. Do not import through `crate::prelude` inside the crate; use direct module paths internally.
4. Use UUID v7 for generated reservation IDs.
5. Do not generate `event_id` inside the outbox; `NewOutboxMetadata.event_id` is caller-supplied and becomes `OutboxEntry.id`.
6. Treat duplicate `event_id` storage as idempotent success; do not update existing rows on conflict.
7. A reserved entry may be completed, failed, or dead only with the matching `reservation_id`.
8. Retry delay is `attempts × 30 seconds`, capped at 2 minutes.
9. Stale sweep must not increment `attempts`.
10. Public API additions must be re-exported through `lib.rs::prelude` only.

## Suggested Atomic Commit Sequence

### 1. Prepare crate dependencies and feature flags

**Goal:** Make the crate ready for domain/application code and optional adapters.

**Files:**

- `outbox/Cargo.toml`

**Changes:**

- Replace wildcard `thiserror = "*"` with a concrete major version matching repository style.
- Add common dependencies:
  - `uuid` with `v7` and `serde`
  - `chrono`
  - `serde` with `derive`
  - `serde_json`
- Add feature flags:
  - `default = ["diesel"]` if outbox should match inbox default behavior
  - `diesel = ["dep:diesel"]`
  - `amqp = ["dep:lapin", "dep:tokio"]` or `amqp = ["diesel", "dep:lapin", "dep:tokio"]` if AMQP worker/publisher construction should assume storage support
- Add optional adapter dependencies:
  - `diesel = { version = "2", features = ["postgres", "r2d2", "uuid", "chrono", "serde_json"], optional = true }`
  - `lapin = { version = "4.5.0", optional = true }`
  - `tokio = { version = "1", features = ["macros", "time"], optional = true }`

**Verification:**

```sh
make check
```

**Commit message:**

```text
chore(outbox): add implementation dependencies
```

### 2. Create the assembly module skeleton

**Goal:** Establish the target file layout without implementing behavior.

**Files:**

- `outbox/src/assembly/mod.rs`
- `outbox/src/assembly/domain.rs`
- `outbox/src/assembly/application.rs`
- `outbox/src/assembly/infra_diesel.rs`
- `outbox/src/lib.rs`

**Changes:**

- Create `assembly/mod.rs` with `domain`, `application`, and feature-gated `infra_diesel` modules.
- Add empty or minimal placeholder modules that compile.
- Update `lib.rs` to declare `mod assembly;` and remove sample `add()` code and generated test.
- Keep existing top-level TODO modules only if they will be replaced in later steps; otherwise remove them when their replacements are introduced.

**Verification:**

```sh
make check
```

**Commit message:**

```text
refactor(outbox): add assembly module layout
```

### 3. Implement domain models and status mapping

**Goal:** Implement pure domain types with no infrastructure dependencies.

**Files:**

- `outbox/src/assembly/domain.rs`

**Changes:**

- Add `OutboxEntry` with fields from `outbox-spec.md`:
  - `id: Uuid`
  - `status: OutboxStatus`
  - `payload: String`
  - `meta: OutboxEntryMetadata`
  - timestamps and reservation fields
  - `last_error: Option<String>`
- Add `NewOutboxEntry` with insert-only fields:
  - `id: Uuid`
  - `payload: String`
  - `meta: OutboxEntryMetadata`
  - `scheduled_at: DateTime<Utc>`
  - `received_at: DateTime<Utc>`
- Add `OutboxStatus` sparse enum:
  - `Received = 0`
  - `Reserved = 2`
  - `Failed = 4`
  - `Completed = 5`
  - `Dead = 7`
  - `Archive = 8`
- Add `UnknownOutboxStatus` error.
- Implement `TryFrom<i32> for OutboxStatus`, `From<OutboxStatus> for i32`, and `as_str()`.
- Add unit tests for all status conversions and unknown values.

**Design note:** `OutboxEntryMetadata` lives in the application layer by spec. If importing it into `domain.rs` creates an undesirable dependency direction, choose one of these and document the choice in code comments:

1. Move metadata value objects into `domain.rs` and re-export them from `application.rs`; or
2. Keep `OutboxEntry.meta` as a domain-owned value object and implement conversions to/from the application metadata type.

Prefer option 1 if it keeps the model simple and mirrors how JSONB metadata is used across crates.

**Verification:**

```sh
make test
```

**Commit message:**

```text
feat(outbox): add domain models
```

### 4. Implement application envelopes, ports, and errors

**Goal:** Define the stable application API without concrete behavior.

**Files:**

- `outbox/src/assembly/application.rs`
- `outbox/src/assembly/domain.rs` if metadata ownership is adjusted

**Changes:**

- Add `NewOutboxEnvelope` and `NewOutboxMetadata`.
- Add `OutboxEntryMetadata` with required `event_id` and `message_id` after recording.
- Add `OutboxEntryEnvelope`.
- Add `OutboundMessageEnvelope`.
- Add `OutboxError` variants:
  - `Storage`
  - `Routing`
  - `Serialization`
  - `Transport`
  - `Reservation`
  - `Publish`
  - `MissingReservation`
  - `Conversion`
- Add port traits:
  - `OutboxStorePort`
  - `OutboxReservePort`
  - `OutboxProcessPort`
  - `OutboxSweepPort`
  - `OutboxPublisherPort`
- Use async trait methods if project policy allows `async fn` in traits on the selected compiler; otherwise use boxed futures consistently with the inbox style.
- Add unit tests for metadata normalization:
  - supplied `message_id` is preserved
  - missing `message_id` defaults to `event_id`

**Verification:**

```sh
make test
```

**Commit message:**

```text
feat(outbox): define application ports and envelopes
```

### 5. Implement the record-events use case

**Goal:** Accept event-dispatcher input and create idempotently storable outbox entries.

**Files:**

- `outbox/src/record_events.rs`
- `outbox/src/lib.rs`

**Changes:**

- Add `OutboxRecorderRepository` with `Arc<dyn OutboxStorePort>`.
- Add `OutboxRecorder` as the public component wrapper.
- Add conversion from `NewOutboxEnvelope` to `NewOutboxEntry`:
  - validate `routing_key` is not blank
  - set `id = metadata.event_id`
  - set `message_id = metadata.message_id.unwrap_or(event_id)`
  - set `scheduled_at` and `received_at` to `Utc::now()` unless supplied by a test seam
- Add `record()` method that delegates to `OutboxStorePort::record`.
- Add tests with a fake store:
  - valid envelope produces expected `NewOutboxEntry`
  - blank routing key returns `OutboxError::Routing`
  - repository delegates exactly once
  - event ID is not generated or replaced

**Verification:**

```sh
make test
```

**Commit message:**

```text
feat(outbox): record outbound events
```

### 6. Implement reservable spec and consumer orchestration

**Goal:** Publish reserved entries through an abstract publisher and update state through ports.

**Files:**

- `outbox/src/outbox_consumer.rs`
- `outbox/src/lib.rs`

**Changes:**

- Add `ReservableOutboxSpec { limit, max_attempts }` with default `max_attempts = 6` helper if useful.
- Add `OutboxConsumerRepository` with:
  - `Arc<dyn OutboxReservePort>`
  - `Arc<dyn OutboxProcessPort>`
- Add `OutboxConsumer` with repository and `Arc<dyn OutboxPublisherPort>`.
- Implement `publish_batch(&ReservableOutboxSpec)`:
  1. reserve entries
  2. for each entry, require `reservation_id`
  3. convert to `OutboundMessageEnvelope`
  4. publish via `OutboxPublisherPort`
  5. on success, call `completed(id, reservation_id)`
  6. on transport/publish failure, call `failed(id, reservation_id, max_attempts, reason)`
  7. on serialization/conversion failure, call `dead(id, reservation_id, reason)`
  8. collect errors and continue processing the batch
- Add unit tests with fake ports:
  - successful publish completes entry
  - publish failure fails entry
  - conversion failure dead-letters entry
  - missing reservation returns `MissingReservation`
  - one failing entry does not stop later entries

**Verification:**

```sh
make test
```

**Commit message:**

```text
feat(outbox): publish reserved entries
```

### 7. Implement stale reservation sweep use case

**Goal:** Release stale reservations through a storage port.

**Files:**

- `outbox/src/stale_reservation_sweep.rs`
- `outbox/src/lib.rs`

**Changes:**

- Add `StaleReservationSpec { timeout, max_attempts }`.
- Add `ReservationSweeper` with `Arc<dyn OutboxSweepPort>`.
- Implement `sweep(&StaleReservationSpec) -> Result<u64, OutboxError>`.
- Add tests with fake sweep port:
  - spec is passed through
  - returned count is surfaced
  - errors are surfaced

**Verification:**

```sh
make test
```

**Commit message:**

```text
feat(outbox): add stale reservation sweep
```

### 8. Wire the public `prelude` facade

**Goal:** Expose the crate API consistently and hide internal modules.

**Files:**

- `outbox/src/lib.rs`

**Changes:**

- Re-export all public models, ports, and use-case types from `pub mod prelude`.
- Feature-gate adapter re-exports:
  - `OutboxStoreStorage`, `OutboxConsumerStorage`, `DbPool`, `build_pool` behind `diesel`
  - `AmqpPublisher`, `AmqpPublishConfig` behind `amqp`
- Keep implementation modules private unless integration tests require access.

**Verification:**

```sh
make check
```

**Commit message:**

```text
feat(outbox): expose prelude facade
```

### 9. Implement Diesel schema and storage entities

**Goal:** Map `outbox_entries` to Diesel structs and JSONB metadata.

**Files:**

- `outbox/src/assembly/infra_diesel.rs`

**Changes:**

- Add Diesel `table!` schema for `outbox_entries`.
- Add storage `DbPool` and `build_pool` helpers consistent with inbox.
- Add `MetadataJsonb` newtype if needed for JSONB serialization/deserialization.
- Add `NewOutboxEntryRecord` insertable entity.
- Add `OutboxEntryRecord` queryable/selectable entity.
- Implement conversions:
  - `NewOutboxEntry -> NewOutboxEntryRecord`
  - `OutboxEntryRecord -> OutboxEntryEnvelope`
- Ensure unknown status codes convert to `OutboxError::Storage` or `OutboxError::Conversion`.
- Add unit tests for record/entity conversion where possible without a database.

**Verification:**

```sh
make check
```

**Commit message:**

```text
feat(outbox): add diesel storage mapping
```

### 10. Implement idempotent Diesel recording

**Goal:** Store accepted events durably and idempotently.

**Files:**

- `outbox/src/assembly/infra_diesel.rs`
- optional Diesel migration files if migrations are stored in this repository

**Changes:**

- Add `OutboxStoreStorage` with a pool.
- Implement `OutboxStorePort::record`.
- Insert rows with status `Received`.
- Use `ON CONFLICT (id) DO NOTHING` or Diesel equivalent.
- Treat both inserted rows and conflict-no-op as success.
- Do not mutate existing rows on conflict.
- Add integration tests if a database test harness exists; otherwise add SQL-focused comments and rely on later end-to-end storage tests.

**Verification:**

```sh
make check
```

Run repository-level tests when database integration is available:

```sh
make test
```

**Commit message:**

```text
feat(outbox): persist events idempotently
```

### 11. Implement Diesel reservation

**Goal:** Atomically reserve eligible entries without double-claiming.

**Files:**

- `outbox/src/assembly/infra_diesel.rs`

**Changes:**

- Add `OutboxConsumerStorage` with a pool.
- Implement `OutboxReservePort::reserve` using one transaction:
  - select eligible IDs where `status IN (Received, Failed)`
  - `scheduled_at <= now()`
  - `attempts < max_attempts`
  - order by `scheduled_at ASC`
  - limit by spec
  - `FOR UPDATE SKIP LOCKED`
  - update selected rows to `Reserved`
  - set `reservation_id = Uuid::now_v7()`
  - set `reserved_at = now()`
  - increment `attempts`
  - set `updated_at = now()`
  - return full envelopes
- Add concurrency/integration test when database is available:
  - two concurrent reservations cannot claim the same entry
  - attempts increments exactly once per reservation

**Verification:**

```sh
make check
```

Run repository-level tests when database integration is available:

```sh
make test
```

**Commit message:**

```text
feat(outbox): reserve publishable entries
```

### 12. Implement Diesel completion, failure, and dead transitions

**Goal:** Enforce reservation ownership and lifecycle updates.

**Files:**

- `outbox/src/assembly/infra_diesel.rs`

**Changes:**

- Implement `OutboxProcessPort` for `OutboxConsumerStorage`.
- `completed(id, reservation_id)`:
  - update only where `id`, matching `reservation_id`, and status `Reserved`
  - set status `Completed`
  - clear reservation fields
  - set `processed_at` and `updated_at`
- `failed(id, reservation_id, max_attempts, reason)`:
  - update only the matching reservation
  - if current attempts >= max_attempts, set `Dead`
  - otherwise set `Failed` and `scheduled_at = now() + min(attempts * 30s, 120s)`
  - clear reservation fields
  - set `last_error`, `updated_at`
- `dead(id, reservation_id, reason)`:
  - update only the matching reservation
  - set `Dead`
  - clear reservation fields
  - set `last_error`, `updated_at`
  - do not schedule retry
- Return an error when no row matches the reservation ownership condition.
- Add tests for:
  - successful completion
  - stale reservation token rejected
  - failed schedules retry
  - failed at max attempts becomes dead
  - dead bypasses retry

**Verification:**

```sh
make check
```

Run repository-level tests when database integration is available:

```sh
make test
```

**Commit message:**

```text
feat(outbox): update publish lifecycle states
```

### 13. Implement Diesel stale reservation sweep

**Goal:** Release stuck reservations without incrementing attempts.

**Files:**

- `outbox/src/assembly/infra_diesel.rs`

**Changes:**

- Implement `OutboxSweepPort` for `OutboxConsumerStorage`.
- Find entries with:
  - status `Reserved`
  - `reserved_at < now() - timeout`
- Reset them to `Failed` unless attempts already reached/exceeded max attempts; then set `Dead`.
- Clear `reservation_id` and `reserved_at`.
- Set `scheduled_at` using the same retry delay formula.
- Do not increment `attempts`.
- Set `last_error` to a clear stale-reservation message.
- Add tests for:
  - stale rows are released
  - fresh reservations are ignored
  - attempts are not incremented
  - exhausted rows become dead if that policy is chosen

**Verification:**

```sh
make check
```

Run repository-level tests when database integration is available:

```sh
make test
```

**Commit message:**

```text
feat(outbox): sweep stale reservations
```

### 14. Add database migration or schema documentation

**Goal:** Ensure operators can create the required table and indexes.

**Files:**

- migration location used by the repository, if any
- or `docs/outbox-spec.md` / a new migration note if no migration framework exists

**Changes:**

- Add `outbox_entries` table exactly as specified:
  - UUID primary key
  - non-null JSONB `meta`
  - reservation and lifecycle columns
  - `last_error`
- Add required indexes:
  - `(status, scheduled_at)`
  - `(status, reserved_at)`
- Add optional operational indexes if desired:
  - `(meta ->> 'routing_key')`
  - `(meta ->> 'message_id')`

**Verification:**

- If migrations are executable, run the migration test command.
- Otherwise, verify SQL manually and ensure docs match `outbox-spec.md`.

**Commit message:**

```text
feat(outbox): add storage schema
```

### 15. Implement AMQP outbound publisher adapter

**Goal:** Publish outbox messages to AMQP and wait for broker acceptance.

**Files:**

- `outbox/src/amqp_publisher.rs`
- `outbox/src/lib.rs`

**Changes:**

- Add `AmqpPublishConfig` with at least:
  - connection URI or channel construction input
  - exchange
  - mandatory flag if needed
  - default content type if metadata does not provide one
- Add `AmqpPublisher`.
- Implement `OutboxPublisherPort`:
  - use `metadata.routing_key`
  - publish `payload`
  - set AMQP properties for `message_id`, `correlation_id`, `content_type`, and headers for `event_id`, `causation_id`, `event_type`, `source`
  - wait for publisher confirmation
  - map negative confirmation/channel/connection errors to `OutboxError::Transport`
- Keep AMQP-specific types out of domain and application modules.
- Add unit tests for metadata-to-AMQP-property conversion if separable from live broker calls.

**Verification:**

```sh
make check
```

**Commit message:**

```text
feat(outbox): add amqp publisher
```

### 16. Add constructor helpers for storage-backed repositories

**Goal:** Match the ergonomic constructor pattern used by inbox.

**Files:**

- `outbox/src/record_events.rs`
- `outbox/src/outbox_consumer.rs`
- `outbox/src/stale_reservation_sweep.rs`
- `outbox/src/lib.rs`

**Changes:**

- Add `repository(database_url: String) -> Result<Arc<...>, String>` helper for recording if consistent with existing crates.
- Add storage-backed constructor helper for consumer/sweeper repository wiring.
- Avoid duplicating connection pools unnecessarily if the caller needs to share one; provide `new(pool)` constructors on storage structs.

**Verification:**

```sh
make check
```

**Commit message:**

```text
feat(outbox): add repository constructors
```

### 17. Add public API smoke tests

**Goal:** Ensure external callers can use only the `prelude` facade.

**Files:**

- `outbox/tests/public_api.rs` or crate-level tests

**Changes:**

- Write tests that import from `outbox::prelude::*` only.
- Construct metadata/envelopes.
- Use fake ports to instantiate `OutboxRecorder`, `OutboxConsumer`, and `ReservationSweeper` if constructors permit.
- Confirm no internal module import is required for normal use.

**Verification:**

```sh
make test
```

**Commit message:**

```text
test(outbox): cover public prelude facade
```

### 18. Add lifecycle integration tests

**Goal:** Validate the full outbox lifecycle with storage and fake publisher.

**Files:**

- `outbox/tests/outbox_lifecycle.rs`
- test database setup helpers, if present or introduced

**Changes:**

- Test `record -> reserve -> publish -> completed`.
- Test `record duplicate event_id` creates only one row and succeeds.
- Test `record -> reserve -> transport failure -> failed -> reserve after scheduled_at`.
- Test `record -> reserve -> conversion failure -> dead`.
- Test `record -> reserve -> stale sweep -> reserve again`.
- Keep live broker tests separate and ignored unless a local broker is required.

**Verification:**

```sh
make test
```

**Commit message:**

```text
test(outbox): verify durable lifecycle
```

### 19. Update repository-level checks

**Goal:** Ensure outbox participates in normal developer workflows.

**Files:**

- `Makefile` only if current discovery does not already include outbox
- `README.md` or docs index if the project maintains links

**Changes:**

- Confirm `make check` and `make test` include `outbox` through `find`-based crate discovery.
- Add outbox docs links if there is a documentation index.
- Do not duplicate Makefile entries if automatic discovery already works.

**Verification:**

```sh
make check
make test
```

**Commit message:**

```text
chore(outbox): include implementation in project checks
```

### 20. Final consistency pass

**Goal:** Ensure implementation, docs, and public API agree.

**Files:**

- `docs/outbox-spec.md`
- `docs/outbox-implementation-plan.md`
- source files touched by implementation

**Changes:**

- Update spec only if implementation exposed an intentional deviation.
- Remove stale TODO modules or comments that point to old `outbox/SPEC.md` if they no longer apply.
- Ensure `outbox/SPEC.md` either links to `docs/outbox-spec.md` or is updated/removed to avoid divergent specs.
- Run formatting across all crates.

**Verification:**

```sh
make check
make test
```

**Commit message:**

```text
docs(outbox): align implementation notes
```

## Minimum Viable Implementation Slice

If implementation must be staged for an early usable release, stop after these steps:

1. dependencies and layout
2. domain/application models
3. record-events use case
4. consumer orchestration with fake publisher tests
5. stale sweep use case
6. public `prelude` facade
7. Diesel recording/reservation/process/sweep

This slice supports durable outbox semantics with a caller-provided publisher port, even before the AMQP adapter exists.

## Risk Register

| Risk                                                                        | Impact                                     | Mitigation                                                                                          |
| --------------------------------------------------------------------------- | ------------------------------------------ | --------------------------------------------------------------------------------------------------- |
| Outbox ID and event ID semantics drift from event dispatcher implementation | Duplicate rows or broken idempotency       | Keep `event_id` as required input and primary key; add duplicate-record tests                       |
| Async trait style differs from inbox implementation                         | Inconsistent public API or compiler issues | Match the style used by the current crate baseline before implementing ports                        |
| Diesel JSONB newtype is hard to wire                                        | Storage adapter delays                     | Copy the inbox JSONB newtype pattern and keep metadata serde-friendly                               |
| AMQP confirms are not awaited correctly                                     | Message loss or false completion           | Treat publish success as only after broker confirmation; test negative confirmations where possible |
| Stale sweep increments attempts accidentally                                | Premature dead-lettering                   | Add explicit test asserting attempts are unchanged                                                  |
| Completion after broker acceptance fails                                    | Duplicate outbound publish                 | Document as expected at-least-once behavior and keep `message_id` stable                            |
| Existing `outbox/SPEC.md` diverges from `docs/outbox-spec.md`               | Confusing implementation target            | Replace `outbox/SPEC.md` with a pointer or synchronize it in the final consistency pass             |

## Definition of Done

- `docs/outbox-spec.md` and implementation agree on names, IDs, ports, and schema.
- All public types needed by callers are available via `outbox::prelude`.
- `make check` passes.
- `make test` passes.
- Duplicate `event_id` recording is idempotent.
- Concurrent reservations cannot claim the same entry.
- `failed()` schedules retry with capped linear backoff.
- `dead()` bypasses retry scheduling.
- Stale sweep clears reservations without incrementing attempts.
- AMQP publisher returns success only after broker acceptance.
