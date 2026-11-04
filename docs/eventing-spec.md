# Eventing Specification

See [item failure isolation](item-failure-isolation.md) for the implemented
transactional reservation, item-level panic containment, and recovery limits.

## Overview

The eventing module is the write-side component responsible for accepting domain events and delivering them to subscribers. It is the counterpart to command handling: command handlers produce `NewEventEnvelope` instances, and the eventing module either delivers them immediately or stores them durably for later delivery.

`EventGateway` is the single entry point for event dispatch. Callers, including `CommandDispatcher`, pass a `NewEventEnvelope` to the gateway; the gateway decides how execution proceeds based on the configured variant:

- **Direct**: the gateway immediately delegates to `EventDispatcher`, which invokes the configured subscriber port synchronously. No durable state; errors propagate immediately to the caller.
- **TwoPhased**: the gateway persists a durable `NewEventEntry` and returns. A separate consumer loop later reserves each entry and passes it to `EventDispatcher`, with bounded retry and stale-reservation recovery.

`EventDispatcher` is the shared delivery engine for both variants. It receives an event envelope and invokes the registered subscriber port. The subscriber port may represent a single subscriber, a composite subscriber registry, or an adapter such as the Outbox.

The TwoPhased variant guarantees at-least-once delivery. Event subscribers must therefore tolerate duplicate delivery. Direct delivery is synchronous and has no automatic retry record.

## Architecture

Private layers separate domain status/timeout rules, application envelopes and
ports, the execution core (consumer, dispatcher and reservation spec), Diesel persistence, and HTTP adaptation. Only
`prelude` is public. `DieselPgEventRepository::new(pool)` implements all four storage ports.
`EventGateway` and `EventSweeper` take their trait-object ports; `EventConsumer<R>` holds one `Arc<R>`
implementing reserve/process. `AsyncEventingService` wraps the consumer and erases `R` at the
blocking-task boundary. There is no synchronous HTTP service, so callers without
Tokio use the consumer directly. Synchronous code supports borrowed repositories.
Transport adapters retain their independent acknowledgment/publication behavior.
See [ADR-012](adr/012-unified-queue-repositories.md) and
[ADR-014](adr/014-merge-sibling-execution-bridges.md).

## Variants

### Direct

Execution is synchronous and in-flight only. No database table, reservation mechanism, or retry record is involved.

1. The caller passes a `NewEventEnvelope` to `EventGateway`.
2. The gateway delegates immediately to `EventDispatcher`.
3. The dispatcher invokes `EventSubscriberPort::handle`.
4. On success, the gateway returns `Ok(())` to the caller.
5. On failure, the error propagates to the caller. Any subscriber side effects already applied remain in place; no automatic retry is available.

Direct eventing is suitable for tests, local-only flows, or request-response paths where the caller controls retry and all subscribers are idempotent.

### TwoPhased

Execution is decoupled and durable. The gateway persists a `NewEventEntry` and returns; a consumer loop does the actual delivery via `EventDispatcher`.

1. The caller passes a `NewEventEnvelope` to `EventGateway`.
2. The gateway stores a `NewEventEntry` with status `Received` via `EventStorePort` and returns.
3. The event is now durably queued; the caller's work is done.
4. A worker drives a consumer loop that reserves eligible entries atomically via `EventReservePort`.
5. For each reserved `EventEnvelope`, the consumer reconstructs a `NewEventEnvelope` and passes it to `EventDispatcher`.
6. The dispatcher invokes the subscriber port.
7. On success: the consumer marks the entry `Completed` via `EventProcessPort`.
8. On failure: the consumer records `Failed` and clears its reservation. Retry is eligible only below the attempt cap; exhausted entries remain `Failed` for inspection.
9. A separate sweep job releases entries stuck in `Reserved` longer than the configured timeout.

## Components

### EventGateway

The single entry point for all event dispatch. Callers go through the gateway rather than calling the store port directly.

- Accepts a `NewEventEnvelope` from command handling or application code.
- Requires `metadata.event_id`; the caller is responsible for supplying it. UUID v7 is recommended.
- **Direct**: delegates immediately to `EventDispatcher`.
- **TwoPhased**: persists the event through `EventStorePort` and returns. Does not invoke `EventDispatcher`. The envelope is passed through unchanged, with no ID generation outside infra conversion, and the Diesel adapter ignores duplicate inserts for the same `event_id` (`ON CONFLICT DO NOTHING`).
- Implements the command-side `EventDispatchPort` so `CommandDispatcher` can hand off produced events without depending on eventing internals.

### EventDispatcher

Shared delivery engine used by both variants. Not called directly by application code.

- Receives a `NewEventEnvelope` from the gateway in Direct mode or from the event consumer in TwoPhased mode.
- Invokes `EventSubscriberPort::handle`.
- Returns success only after the subscriber port has accepted the event.
- Treats zero subscribers as success when the subscriber port is implemented as a registry/composite; absence of subscribers is not a dispatch error.

### EventConsumer<R>

Use case: reserve persisted events and deliver them via `EventDispatcher` (TwoPhased only).

- Reserves up to `limit` eligible entries atomically.
- Converts each reserved `EventEnvelope` back into a `NewEventEnvelope`.
- Delivers each event through `EventDispatcher`.
- Marks each entry `Completed` or `Failed` based on the dispatcher outcome.
- Returns all errors collected across the batch; does not abort the batch on first error.

### EventSweeper

Use case: release stale reservations back to the eligible queue (TwoPhased only).

- Finds all `Reserved` entries whose `reserved_at` is older than the configured timeout.
- Sets all timed-out rows to `Failed`, including exhausted rows, and clears reservation fields. Only entries below the attempt cap can be reserved again.
- Does **not** increment the attempt counter; reservation increments attempts, not failure or sweeping.

### DieselPgEventRepository: recording

Implements `EventStorePort`. Inserts a new `event_entries` row. Does not touch existing rows on conflict, making event recording idempotent by `event_id`.

### DieselPgEventRepository: processing and recovery

Implements `EventReservePort`, `EventProcessPort`, and `EventSweepPort`.

- **reserve**: uses `SELECT … FOR UPDATE SKIP LOCKED`, then updates status to `Reserved`, assigns `reservation_id` (UUID v7), increments `attempts`, and returns full event envelopes.
  Reservation, raw JSON loading, per-row decoding, and malformed-row failure writes share one transaction. Only healthy envelopes are returned after commit; the candidate limit is not refilled.
- **completed**: updates status to `Completed`, clears `reservation_id` and `reserved_at`, and sets `processed_at`.
- **failed**: records `Failed`, even at exhaustion, and clears reservation fields. The reservation attempt cap controls retry eligibility. Failure scheduling uses database time; see [item failure isolation](item-failure-isolation.md) for delay formulas.
- **sweep**: releases stale `Reserved` entries by clearing reservation fields and scheduling another attempt without incrementing `attempts`.

## Timeout recovery contract

Sweeps run in one transaction and take one PostgreSQL `transaction_timestamp()`.
Only `Reserved` rows with non-null `reserved_at < database_time - timeout` are
selected, with blocking row locks held through the update. A pure internal policy
calculates each outcome; one set-based update clears reservation fields, preserves
existing errors and appends the timeout reason. Errors roll back the entire sweep.
Attempts do not change. The default retry limit is 6; configure the same limit on
consumer and sweeper. No recovery worker is started automatically.

Retry delay is `attempts × 30 seconds`, uncapped. Both retryable and exhausted
rows get `scheduled_at = database_time + delay`.
Explicit failure paths also use database time; they always retain `Failed` status.

## Models

### EventStatus (domain)

| Variant     | Code | Description                                                                |
| ----------- | ---- | -------------------------------------------------------------------------- |
| `Received`  | `0`  | Stored, eligible for reservation                                           |
| `Reserved`  | `2`  | Claimed by a consumer                                                      |
| `Failed`    | `4`  | Failure retained for inspection; retry eligible only below the attempt cap |
| `Completed` | `5`  | Successfully delivered to the subscriber port                              |
| `Dead`      | `7`  | Legacy/manual terminal status; never an automatic failure outcome          |
| `Archive`   | `8`  | Archived (no further processing)                                           |

Numeric codes use intentional gaps (1, 3, 6 are reserved for future insertion without renumbering). See [ADR-002](adr/002-status-code-gaps.md).

### NewEventEnvelope (application)

The gateway input envelope. `event_id` is always supplied inside metadata; no generation occurs inside the system boundary.

| Field        | Type                       | Description              |
| ------------ | -------------------------- | ------------------------ |
| `event_type` | `String`                   | Subscriber discriminator |
| `payload`    | `String`                   | Raw event body           |
| `metadata`   | `Option<NewEventMetadata>` | Routing/correlation data |

### NewEventMetadata (application)

| Field            | Type             | Description                                      |
| ---------------- | ---------------- | ------------------------------------------------ |
| `event_id`       | `Uuid`           | Stable deduplication ID (required)               |
| `correlation_id` | `Option<Uuid>`   | Cross-service correlation chain                  |
| `causation_id`   | `Option<Uuid>`   | ID of the command/message that caused this event |
| `source`         | `Option<String>` | Originating service identifier                   |

When events are produced from a command, `causation_id` should be the command's `command_id`. `correlation_id` should be propagated from the incoming command or inbox message when available.

### EventEnvelope (application)

Read-side envelope returned to the consumer after reservation. Constructed by the infra layer from the stored `EventEntry`; contains all fields the consumer needs without exposing infra types.

| Field            | Type                    | Description                                    |
| ---------------- | ----------------------- | ---------------------------------------------- |
| `id`             | `Uuid`                  | Entry identifier, used to mark complete/failed |
| `reservation_id` | `Uuid`                  | Ownership token, required for process calls    |
| `event_type`     | `String`                | Subscriber discriminator                       |
| `payload`        | `String`                | Raw event body                                 |
| `attempts`       | `i32`                   | Number of times reserved so far                |
| `metadata`       | `Option<EventMetadata>` | Decoded routing and correlation metadata       |

### EventMetadata (application)

| Field            | Type             | Description                                      |
| ---------------- | ---------------- | ------------------------------------------------ |
| `event_id`       | `Uuid`           | Stable deduplication ID                          |
| `correlation_id` | `Option<Uuid>`   | Cross-service correlation chain                  |
| `causation_id`   | `Option<Uuid>`   | ID of the command/message that caused this event |
| `source`         | `Option<String>` | Originating service identifier                   |

### ReservableEventSpec (event_consumer)

| Field          | Default | Description                                        |
| -------------- | ------- | -------------------------------------------------- |
| `limit`        | —       | Maximum number of entries to reserve per call      |
| `max_attempts` | `6`     | Entries at or above this attempt count are skipped |

Builds query criteria: `StatusIn([Received, Failed])`, `ScheduledBeforeNow`, `MaxAttempts(max_attempts)`, `OrderByScheduledAtAsc`.

### StaleEventSpec (stale_event_sweep)

| Field          | Default | Description                                                      |
| -------------- | ------- | ---------------------------------------------------------------- |
| `timeout`      | —       | Staleness threshold; entries reserved longer than this are swept |
| `max_attempts` | `6`     | Max attempts forwarded to the retry policy                       |

The repository calculates the strict cutoff from the database transaction timestamp and the timeout; the spec reads no clock.

### EventError (application)

| Variant               | Description                                 |
| --------------------- | ------------------------------------------- |
| `Storage`             | Storage adapter error                       |
| `Reservation`         | Reservation query or update failure         |
| `SubscriberNotFound`  | No subscriber registry/handler is available |
| `SubscriberExecution` | Subscriber returned an error                |
| `MissingReservation`  | Entry returned without a reservation_id     |
| `Conversion`          | Envelope conversion failure                 |

## Ports

### EventDispatchPort

Command handling depends on this port when handing off produced events.

```rust
fn dispatch(&self, event: NewEventEnvelope) -> Result<(), CommandError>;
```

The eventing gateway implements this port. It maps eventing errors into `CommandError::EventDispatch` for the command side.

### EventStorePort

```rust
fn record(&self, envelope: &NewEventEnvelope) -> Result<(), EventError>;
```

### EventReservePort

```rust
fn reserve(&self, spec: &ReservableEventSpec) -> Result<Vec<EventEnvelope>, EventError>;
```

### EventProcessPort

```rust
fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), EventError>;
fn failed(&self, id: Uuid, reservation_id: Uuid, max_attempts: i32) -> Result<(), EventError>;
```

### EventSweepPort

```rust
fn sweep(&self, spec: &StaleEventSpec) -> Result<u64, EventError>;
```

### EventSubscriberPort

```rust
fn handle(&self, envelope: &NewEventEnvelope) -> Result<(), EventError>;
```

Implementations may be concrete subscribers, composite subscriber registries, or adapters to other components such as the Outbox.

## Infra Models

These types are defined in `eventing/infra_diesel/` and are only available with the `diesel` feature. They are not referenced by application or feature layers.

### EventEntry (infra, Diesel)

Queryable struct mapping the full `event_entries` row. Constructed by `DieselPgEventRepository` and converted into `EventEnvelope` before being returned across the port boundary.

| Field            | Type                    | Description                             |
| ---------------- | ----------------------- | --------------------------------------- |
| `id`             | `Uuid`                  | Primary key; equals `metadata.event_id` |
| `event_type`     | `String`                | Subscriber discriminator                |
| `status`         | `i32`                   | Encoded `EventStatus`                   |
| `payload`        | `String`                | Raw event body                          |
| `meta`           | `Option<MetadataJsonb>` | JSONB-serialized event metadata         |
| `scheduled_at`   | `DateTime<Utc>`         | Earliest eligible-for-reservation time  |
| `attempts`       | `i32`                   | Number of reservations                  |
| `reservation_id` | `Option<Uuid>`          | Current reservation token               |
| `reserved_at`    | `Option<DateTime<Utc>>` | Current reservation timestamp           |
| `received_at`    | `DateTime<Utc>`         | Initial storage timestamp               |
| `updated_at`     | `DateTime<Utc>`         | Last state-change timestamp             |
| `processed_at`   | `Option<DateTime<Utc>>` | Completion timestamp                    |

### NewEventEntry (infra, Diesel)

Insertable struct used only by `DieselPgEventRepository`.

| Field          | Type                    | Description                          |
| -------------- | ----------------------- | ------------------------------------ |
| `id`           | `Uuid`                  | Primary key from `metadata.event_id` |
| `event_type`   | `String`                | Subscriber discriminator             |
| `payload`      | `String`                | Raw event body                       |
| `meta`         | `Option<MetadataJsonb>` | JSONB-serialized metadata            |
| `scheduled_at` | `DateTime<Utc>`         | Initial reservation eligibility      |
| `received_at`  | `DateTime<Utc>`         | Initial storage timestamp            |

## Database Schema

Table: `event_entries`

```sql
CREATE TABLE event_entries (
    id              UUID         PRIMARY KEY,
    event_type      TEXT         NOT NULL,
    status          INTEGER      NOT NULL,
    payload         TEXT         NOT NULL,
    meta            JSONB,
    scheduled_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    attempts        INTEGER      NOT NULL DEFAULT 0,
    reservation_id  UUID,
    reserved_at     TIMESTAMPTZ,
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ
);
```

Recommended indexes:

- `(status, scheduled_at)` — reservation queries
- `(status, reserved_at)` — stale sweep queries
- `(event_type, received_at)` — operational inspection by event type

## Retry Policy

Applies to `failed()` transitions:

- Failure retry delay = `max(attempts, 1) × 30 seconds`, measured from database time
- Cap: 2 minutes per attempt
- At `max_attempts` (default 6), failed entries remain `Failed` but are ineligible for automatic reservation
- Stale sweep does **not** increment the attempt counter

## Reliability Semantics

### Direct

- No durable acceptance point exists.
- Subscriber delivery is attempted inline.
- If the subscriber fails, the error propagates to the caller.
- If a composite subscriber invokes some subscribers before one fails, already-completed subscriber side effects are not rolled back.
- Retrying Direct dispatch can re-invoke subscribers that already succeeded.

### TwoPhased

- Acceptance is signalled only after durable storage of the `EventEntry`.
- Delivery to the subscriber port is at-least-once.
- A reserved entry cannot be concurrently claimed by another consumer.
- If delivery succeeds but marking `Completed` fails, the entry can be retried and delivered again.
- If a composite subscriber partially succeeds before returning an error, retry can re-invoke already-successful subscribers.
- Event ordering is not guaranteed across entries or after retries.

## Feature Flags

| Feature  | Adds                                                                |
| -------- | ------------------------------------------------------------------- |
| `diesel` | PostgreSQL/Diesel storage adapters, schema module, and infra models |

## Examples

### Direct eventing

```rust
use std::sync::Arc;
use uuid::Uuid;
use write_side::prelude::{EventDispatcher, EventGateway, NewEventEnvelope, NewEventMetadata};

let dispatcher = Arc::new(EventDispatcher::new(subscriber));
let gateway = EventGateway::direct(dispatcher);

gateway.dispatch(NewEventEnvelope {
    event_type: "todo.created".to_string(),
    payload: r#"{"title":"Write spec"}"#.to_string(),
    metadata: Some(NewEventMetadata {
        event_id: Uuid::now_v7(),
        correlation_id: None,
        causation_id: None,
        source: Some("todo-service".to_string()),
    }),
})?;
```

### Two-phased eventing

```rust
use std::sync::Arc;
use eventing::prelude::{DieselPgEventRepository, EventConsumer,
    EventDispatcher, EventGateway, ReservableEventSpec};
let repository = Arc::new(DieselPgEventRepository::new(pool));
let gateway = EventGateway::two_phased(repository.clone());
let dispatcher = Arc::new(EventDispatcher::new(subscriber));
let consumer = EventConsumer::new(repository, dispatcher);
consumer.consume(&ReservableEventSpec::new(100))?;
```

## Implementation Notes

- `EventStatus` should live in `eventing/domain/mod.rs`; the obsolete `model.rs` placeholder is removed.
- `EventDispatchPort` is the boundary between command handling and eventing. The command side should not depend on eventing concrete types beyond this port and `NewEventEnvelope`.
- `EventSubscriberPort` should remain deliberately small. Subscriber resolution, fan-out, filtering, and outbox transformation belong in implementations behind the port, not in the core dispatcher.
- `EventGateway` should reject envelopes with missing metadata before either direct dispatch or durable recording, because `event_id` is required for deduplication.
- The Diesel adapter should use UUID v7 for `reservation_id` and should use `ON CONFLICT DO NOTHING` when inserting events.

## Document Metadata

| Version | Author | Reviewers            | Summary                                  | Date       |
| ------- | ------ | -------------------- | ---------------------------------------- | ---------- |
| 0.1.0   | Codex  | Miro, Claude & Codex | Initial eventing technical specification | 2026-05-13 |

## Reusable HTTP endpoints

Framework preludes expose `event_routes(service)` and `EventingApi::new(service)`.
Both accept `impl Into<AsyncEventingService>`, so `kernel.event_consumer()` can be
passed directly.
Relative `POST /run` processes a batch.
The host owns mount prefixes, authentication, authorization and middleware.
Existing custom adapters remain available; HTTP status/body contracts and Cargo
feature names are unchanged. The shared Poem run schemas remain in `mulac_http`.
