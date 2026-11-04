# Command Handling Specification

See [item failure isolation](item-failure-isolation.md) for the implemented
transactional reservation, item-level panic containment, and recovery limits.

## Overview

`CommandGateway` is the single entry point for all command dispatch. Callers (application code or the inbox consumer) pass a `NewCommandEnvelope` to the gateway; the gateway decides how execution proceeds based on the configured variant:

- **Direct**: the gateway immediately delegates to `CommandDispatcher`, which resolves and invokes the handler synchronously. No durable state; errors propagate immediately to the caller.
- **TwoPhased**: the gateway persists a durable `NewCommandEntry` and returns. A separate consumer loop later picks up the entry and passes it to `CommandDispatcher` for execution, with full retry and stale-reservation recovery.

`CommandDispatcher` is the shared execution engine for both variants. It resolves the command handler from a registry, invokes it, and hands produced events to the event dispatcher.

The TwoPhased variant guarantees at-least-once execution. Command handlers must therefore tolerate duplicate invocations or implement their own deduplication.

## Architecture

The crate follows a ports-and-adapters organization. The shared execution core
is a private module beside HTTP, not an additional runtime service:

- **domain**: lifecycle status, metadata helpers, and reservation-timeout policy.
- **application**: envelopes, errors, storage ports, criteria, gateway, and sweeper.
- **execution** (`execution/mod.rs`): consumer, reservation specification,
  dispatcher, handler ports, and typed-handler wrapper; depends on application
  contracts and event-dispatch ports, not HTTP or a runtime.
- **HTTP**: request/response contracts, async execution bridge, and framework adapters.
- **infra adapters**: concrete storage implementing the application ports.

External callers import exclusively through the crate's `prelude`. Execution
implementation modules remain private and expose their types through preludes.
The kernel's registry and application's worker remain outside this crate.

```text
lib.rs (prelude facade)
  ├─ execution/command_consumer     CommandConsumer
  ├─ execution/reservable           ReservableCommandSpec
  ├─ execution/command_dispatcher   CommandDispatcher, ApplicationEvent,
  │                                CommandHandlerPort, ErasedCommandHandler,
  │                                wrap_handler
  ├─ application/command_gateway   CommandGateway
  ├─ application/stale_command_sweep  CommandSweeper, StaleCommandSpec
  ├─ application                   envelopes, errors, storage ports
  ├─ domain                        status and lifecycle policy
  ├─ http [feature = "api"]        HTTP contracts and adapters
  └─ infra_diesel [feature = "diesel"]  database repository and entry types
```

## Variants

### Direct

Execution is synchronous and in-flight only. No database table, no reservation mechanism, no retry record.

1. The caller passes a `NewCommandEnvelope` to `CommandGateway`.
2. The gateway delegates immediately to `CommandDispatcher`.
3. The dispatcher resolves the handler via `CommandHandlerPort` using `command_type` and invokes it.
4. The handler returns zero or more `EventEnvelope` instances; the dispatcher hands each to the event dispatcher.
5. On any failure the error propagates to the caller. Any handler side-effects already applied remain in place; no automatic retry is available.

The Direct variant is suitable for synchronous request-response flows where the caller controls retry and the command handler is idempotent.

### TwoPhased

Execution is decoupled and durable. The gateway persists a `NewCommandEntry` and returns; a consumer loop does the actual work via `CommandDispatcher`.

1. The caller passes a `NewCommandEnvelope` to `CommandGateway`.
2. The gateway stores a `NewCommandEntry` (status `Received`) via `CommandStorePort` and returns.
3. The `NewCommandEntry` is now durably queued; the caller's work is done.
4. A worker drives a consumer loop that reserves eligible entries atomically via `CommandReservePort`.
5. For each reserved entry, the consumer passes the envelope to `CommandDispatcher`.
6. The dispatcher resolves the handler via `CommandHandlerPort` and invokes it.
7. The handler returns zero or more `EventEnvelope` instances; the dispatcher hands each to the event dispatcher.
8. On success: the consumer marks the entry `Completed` via `CommandProcessPort`.
9. On failure: the consumer records `Failed` and clears its reservation. Retry is eligible only below the attempt cap; exhausted entries remain `Failed` for inspection.
10. A separate sweep job releases entries stuck in `Reserved` longer than the configured timeout.

## Components

### CommandGateway

The single entry point for all command dispatch. Callers never interact with `CommandDispatcher` directly.

- Accepts a `NewCommandEnvelope` from any caller. The caller is responsible for supplying a `command_id`; no generation occurs inside the gateway.
- **Direct**: delegates immediately to `CommandDispatcher`.
- **TwoPhased**: stores the command directly via `Arc<dyn CommandStorePort>` and returns. Does not invoke `CommandDispatcher`.

### CommandDispatcher

Shared execution engine used by both variants. Not called directly by application code.

- Receives a `NewCommandEnvelope` (from the gateway in Direct, from the consumer in TwoPhased).
- Resolves the command handler via `CommandHandlerPort` using `command_type`.
- Invokes the handler and collects produced `EventEnvelope` instances.
- Hands each event to the event dispatcher.
- Returns the combined result to its caller (gateway or consumer).

### CommandConsumer

Use case: reserve persisted commands and execute them via `CommandDispatcher` (TwoPhased only).

- Reserves up to `limit` eligible entries atomically.
- Passes each reserved `CommandEnvelope` to `CommandDispatcher`.
- Marks each entry `Completed` or `Failed` based on the outcome returned by the dispatcher.
- Returns all errors collected across the batch; does not abort the batch on first error.

### CommandSweeper

Use case: release stale reservations back to the eligible queue (TwoPhased only).

- Finds all `Reserved` entries whose `reserved_at` is older than the configured timeout.
- Resets them to `Failed` with a retry backoff, clearing reservation fields.
- Does **not** increment the attempt counter (reservation increments attempts, not failure or sweeping).

### DieselPgCommandRepository (infra, Diesel)

Implements `CommandStorePort`, `CommandReservePort`, `CommandProcessPort`, and `CommandSweepPort` using one database pool.

- **record**: Inserts a new `command_entries` row. Does not touch existing rows on conflict (idempotent recording).

- **reserve**: `SELECT … FOR UPDATE SKIP LOCKED`, then updates status to `Reserved`, assigns `reservation_id` (UUID v7), increments `attempts`. Returns full envelopes.
  Reservation, raw JSON loading, per-row decoding, and malformed-row failure writes share one transaction. Only healthy envelopes are returned after commit; the candidate limit is not refilled.
- **completed**: Updates status to `Completed`, clears `reservation_id` and `reserved_at`, sets `processed_at`.
- **failed**: records `Failed`, even at exhaustion, and clears reservation fields. The reservation attempt cap controls retry eligibility. Failure scheduling uses database time; see [item failure isolation](item-failure-isolation.md) for delay formulas.

## Models

### CommandStatus (domain)

| Variant     | Code | Description                                                                |
| ----------- | ---- | -------------------------------------------------------------------------- |
| `Received`  | `0`  | Stored, eligible for reservation                                           |
| `Reserved`  | `2`  | Claimed by a consumer                                                      |
| `Failed`    | `4`  | Failure retained for inspection; retry eligible only below the attempt cap |
| `Completed` | `5`  | Successfully executed and events handed off                                |
| `Dead`      | `7`  | Legacy/manual terminal status; never an automatic failure outcome          |
| `Archive`   | `8`  | Archived (no further processing)                                           |

Numeric codes use intentional gaps (1, 3, 6 are reserved for future insertion without renumbering). See [ADR-002](adr/002-status-code-gaps.md).

### NewCommandEnvelope (application)

The gateway input envelope. `command_id` is always `Uuid` — the caller must supply it (UUID v7 recommended). No generation occurs inside the system boundary.

| Field          | Type                         | Description                               |
| -------------- | ---------------------------- | ----------------------------------------- |
| `command_type` | `String`                     | Handler discriminator                     |
| `payload`      | `String`                     | Raw command body                          |
| `metadata`     | `Option<NewCommandMetadata>` | Optional routing and correlation metadata |

### NewCommandMetadata (application)

| Field            | Type             | Description                                |
| ---------------- | ---------------- | ------------------------------------------ |
| `command_id`     | `Uuid`           | Stable deduplication ID (required)         |
| `correlation_id` | `Option<Uuid>`   | Cross-service correlation chain            |
| `causation_id`   | `Option<Uuid>`   | ID of the message that caused this command |
| `source`         | `Option<String>` | Originating service identifier             |

`causation_id` should be propagated from the triggering inbox message's `message_id` when a command originates from inbox processing. Produced `EventEnvelope` instances should carry the command's `command_id` as their own `causation_id`.

### CommandEnvelope (application)

Read-side envelope returned to the consumer after reservation. Constructed by the infra layer from the stored `CommandEntry`; contains all fields the consumer needs without exposing infra types.

| Field            | Type                      | Description                                    |
| ---------------- | ------------------------- | ---------------------------------------------- |
| `id`             | `Uuid`                    | Entry identifier, used to mark complete/failed |
| `reservation_id` | `Uuid`                    | Ownership token, required for process calls    |
| `command_type`   | `String`                  | Handler discriminator                          |
| `payload`        | `String`                  | Raw command body                               |
| `attempts`       | `i32`                     | Number of times reserved so far                |
| `metadata`       | `Option<CommandMetadata>` | Decoded routing and correlation metadata       |

### CommandMetadata (application)

Decoded metadata attached to a reserved command.

| Field            | Type             | Description                                |
| ---------------- | ---------------- | ------------------------------------------ |
| `command_id`     | `Uuid`           | Stable deduplication ID                    |
| `correlation_id` | `Option<Uuid>`   | Cross-service correlation chain            |
| `causation_id`   | `Option<Uuid>`   | ID of the message that caused this command |
| `source`         | `Option<String>` | Originating service identifier             |

### ReservableCommandSpec (command_consumer)

| Field          | Default | Description                                        |
| -------------- | ------- | -------------------------------------------------- |
| `limit`        | —       | Maximum number of entries to reserve per call      |
| `max_attempts` | `6`     | Entries at or above this attempt count are skipped |

Builds query criteria: `StatusIn([Received, Failed])`, `ScheduledBeforeNow`, `MaxAttempts(max_attempts)`, `OrderByScheduledAtAsc`.

### StaleCommandSpec (stale_command_sweep)

| Field          | Default | Description                                                      |
| -------------- | ------- | ---------------------------------------------------------------- |
| `timeout`      | —       | Staleness threshold; entries reserved longer than this are swept |
| `max_attempts` | `6`     | Max attempts forwarded to the retry policy                       |

### CommandError (application)

| Variant              | Description                                      |
| -------------------- | ------------------------------------------------ |
| `Storage`            | Storage adapter error                            |
| `Reservation`        | Reservation query or update failure              |
| `HandlerNotFound`    | No handler registered for the given command_type |
| `HandlerExecution`   | Command handler returned an error                |
| `EventDispatch`      | Event dispatcher rejected a produced event       |
| `MissingReservation` | Entry returned without a reservation_id          |
| `Conversion`         | Envelope conversion failure                      |

## Ports

### CommandStorePort

```rust
async fn record(&self, envelope: &NewCommandEnvelope) -> Result<(), CommandError>;
```

### CommandReservePort

```rust
async fn reserve(&self, spec: &ReservableCommandSpec) -> Result<Vec<CommandEnvelope>, CommandError>;
```

### CommandProcessPort

```rust
async fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), CommandError>;
async fn failed(&self, id: Uuid, reservation_id: Uuid, max_attempts: i32) -> Result<(), CommandError>;
```

### CommandSweepPort

```rust
async fn sweep(&self, spec: &StaleCommandSpec) -> Result<u64, CommandError>;
```

### CommandHandlerPort

```rust
async fn execute(&self, envelope: &NewCommandEnvelope) -> Result<Vec<EventEnvelope>, CommandError>;
```

## Infra Models

These types are defined in `infra_diesel/repository.rs` and are only available with the `diesel` feature. They are not referenced by the application or feature layers.

### CommandEntry (infra, Diesel)

Queryable struct mapping the full `command_entries` row. Constructed by `DieselPgCommandRepository` and converted into `CommandEnvelope` before being returned across the port boundary.

| Field            | Type                        | Description                               |
| ---------------- | --------------------------- | ----------------------------------------- |
| `id`             | `Uuid`                      | UUID v7 primary key                       |
| `command_type`   | `String`                    | Discriminator used to resolve the handler |
| `status`         | `CommandStatus`             | Lifecycle state                           |
| `payload`        | `String`                    | Raw UTF-8 command body                    |
| `meta`           | `Option<serde_json::Value>` | JSONB-serialised routing metadata         |
| `scheduled_at`   | `DateTime<Utc>`             | Earliest eligible-for-reservation time    |
| `attempts`       | `i32`                       | Number of times reserved for execution    |
| `reservation_id` | `Option<Uuid>`              | Ownership token assigned on reservation   |
| `reserved_at`    | `Option<DateTime<Utc>>`     | Timestamp of last reservation             |
| `received_at`    | `DateTime<Utc>`             | Timestamp of initial storage              |
| `updated_at`     | `DateTime<Utc>`             | Timestamp of last state change            |
| `processed_at`   | `Option<DateTime<Utc>>`     | Timestamp of completion                   |

### NewCommandEntry (infra, Diesel)

Insertable struct built from a `NewCommandEnvelope` inside `DieselPgCommandRepository::record()`. Contains only the fields the application provides; database-managed fields are absent.

| Field          | Type                        | Description                                           |
| -------------- | --------------------------- | ----------------------------------------------------- |
| `id`           | `Uuid`                      | UUID v7, sourced from `NewCommandMetadata.command_id` |
| `command_type` | `String`                    | Discriminator used to resolve the handler             |
| `payload`      | `String`                    | Raw UTF-8 command body                                |
| `meta`         | `Option<serde_json::Value>` | JSONB-serialised routing metadata                     |
| `scheduled_at` | `DateTime<Utc>`             | Earliest eligible-for-reservation time                |
| `received_at`  | `DateTime<Utc>`             | Timestamp of initial storage                          |

## Database Schema

Applies to the TwoPhased variant only.

Table: `command_entries`

```sql
CREATE TABLE command_entries (
    id              UUID         PRIMARY KEY,
    command_type    TEXT         NOT NULL,
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
- `(command_type, status)` — per-type operational queries

The primary key is UUID (not BIGSERIAL) because command IDs are generated by the application layer before storage and may originate from external callers. See [ADR-003](adr/003-uuid-v7-identifiers.md).

## Retry Policy

Applies to `failed()` transitions (TwoPhased variant only):

- Failure retry delay = `max(attempts, 1) × 30 seconds`, measured from database time
- Cap: 2 minutes per attempt
- At `max_attempts` (default 6), failed entries remain `Failed` but are ineligible for automatic reservation
- Stale sweep does **not** increment the attempt counter

## Feature Flags

| Feature  | Adds                                                                                         |
| -------- | -------------------------------------------------------------------------------------------- |
| `diesel` | `CommandEntry`, `NewCommandEntry`, `DieselPgCommandRepository`, `DbPool`, Diesel + r2d2 deps |

## Examples

### Direct dispatch via CommandGateway

```rust
let envelope = NewCommandEnvelope {
    command_type: "CreateOrder".into(),
    payload: serde_json::to_string(&cmd)?,
    metadata: Some(NewCommandMetadata {
        command_id: Uuid::now_v7(),
        correlation_id: Some(correlation_id),
        causation_id: Some(inbox_message_id),
        source: Some("order-service".into()),
    }),
};

// Gateway delegates to CommandDispatcher internally
gateway.dispatch(envelope).await?;
```

### TwoPhased: dispatching and consuming

```rust
// Caller side: gateway stores the command and returns
let envelope = NewCommandEnvelope {
    command_type: "PlaceOrder".into(),
    payload: serde_json::to_string(&cmd)?,
    metadata: Some(NewCommandMetadata {
        command_id: Uuid::now_v7(),
        correlation_id: Some(correlation_id),
        causation_id: Some(inbox_message_id),
        source: Some("order-service".into()),
    }),
};
gateway.dispatch(envelope).await?;

// Background consumer loop: reserves entries and passes each to CommandDispatcher
let spec = ReservableCommandSpec { limit: 10, max_attempts: 6 };
let entries = repository.reserve(&spec).await?;

for entry in entries {
    // consumer calls CommandDispatcher internally
    match consumer.execute(&entry).await {
        Ok(_) => repository.completed(entry.id, reservation_id).await?,
        Err(_) => repository.failed(entry.id, reservation_id, spec.max_attempts).await?,
    }
}
```

### Stale reservation sweep

```rust
let spec = StaleCommandSpec {
    timeout: Duration::from_secs(15 * 60),
    max_attempts: 6,
};
let released = sweeper.sweep(&spec).await?;
```

## Document Metadata

| Version | Author                          | Summary                                                                                                                   | Date       |
| ------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ---------- |
| 0.1.0   | Claude::claude-sonnet-4-6::high | Initial command handling technical specification                                                                          | 2026-05-13 |
| 0.1.1   | Claude::claude-sonnet-4-6::high | Move CommandEntry and NewCommandEntry to infra_diesel; flatten CommandEnvelope; CommandStorePort takes NewCommandEnvelope | 2026-05-13 |

## HTTP boundary

Commanding’s private `http` module owns contracts and execution adaptation, not
command lifecycle behavior. `CommandConsumer<R>` remains the use case. Existing
public synchronous/async service types and `api*` feature names are retained.
Other queues follow the dependency boundaries described in ADR-012.
