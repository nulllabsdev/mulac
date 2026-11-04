# Inbox Specification

See [item failure isolation](item-failure-isolation.md) for the implemented
transactional reservation, item-level panic containment, and recovery limits.

## Overview

The inbox crate provides a durable inbound message buffer that decouples external transport delivery from internal command processing. It accepts raw messages from an external transport (AMQP or other), stores them atomically, and exposes them to consumers that transform each entry into a command for the write side.

The inbox guarantees at-least-once delivery to consumers. Deduplication, stale-reservation recovery, and retry scheduling are built in.

## Architecture

Private layers separate domain status/timeout rules, application envelopes and
ports, the execution core (consumer and reservation spec), Diesel persistence, and HTTP adaptation. Only
`prelude` is public. `DieselPgInboxRepository::new(pool)` implements all four storage ports.
`AmqpWorker`, `AsyncInboxService` and `ReservationSweeper` take their trait-object ports; `InboxConsumer<R>` holds one `Arc<R>`
implementing reserve/process. `AsyncInboxService` wraps the store port and the consumer and erases `R` at the
blocking-task boundary. There is no synchronous HTTP service, so callers without
Tokio use those directly. Synchronous code supports borrowed repositories.
Transport adapters retain their independent acknowledgment/publication behavior.
See [ADR-012](adr/012-unified-queue-repositories.md) and
[ADR-014](adr/014-merge-sibling-execution-bridges.md).

## Components

### AmqpWorker / WorkerLoop

Bridges an external AMQP consumer to inbox storage. One worker per queue binding.

- Polls the transport for the next `InboundMessageEnvelope`.
- Converts the envelope to a `NewInboxMessageEnvelope`, generating a UUID v7 message ID when the transport does not supply one.
- Spawns a blocking task to record the message via `InboxStorePort::store`.
- On success: acknowledges the delivery via `AcknowledgeHandle`.
- On recording failure: negative-acknowledges (nacks) the delivery and backs off.
- Respects a `CancellationToken`; shuts down cleanly on cancellation or stream end.

### AmqpTransport / AmqpClient

Wraps a `lapin::Consumer`. Implements `InboxTransportPort` by converting AMQP `Delivery` objects into `InboundMessageEnvelope`, extracting UUID properties from AMQP message headers.

### InboxConsumer<R>

Use case: reserve messages and process them into commands.

- Reserves up to `limit` eligible entries atomically.
- Converts each `InboxMessageEnvelope` to a `Command::Publish(payload)`.
- Publishes each command via the write-side gateway.
- Marks each entry completed or failed based on the publish outcome.
- Returns all errors collected across the batch; does not abort early.

### ReservationSweeper

Use case: release stale reservations back to the eligible queue.

- Finds all `Reserved` entries whose `reserved_at` is older than the configured timeout.
- Sets all timed-out rows to `Failed`, including exhausted rows, and clears reservation fields. Only entries below the attempt cap can be reserved again.
- Does not increment the attempt counter (reservation increments attempts, not failure or sweeping).

### DieselPgInboxRepository: recording

Implements `InboxStorePort`. Inserts a new `inbox_entries` row. Does not touch existing rows on conflict.

### DieselPgInboxRepository: processing and recovery

Implements `InboxReservePort` and `InboxProcessPort`.

- **reserve**: `SELECT … FOR UPDATE SKIP LOCKED`, then updates status to `Reserved`, assigns `reservation_id` (UUID v7), increments `attempts`. Returns full envelopes.
  Reservation, raw JSON loading, per-row decoding, and malformed-row failure writes share one transaction. Only healthy envelopes are returned after commit; the candidate limit is not refilled.
- **completed**: Updates status to `Completed`, clears `reservation_id` and `reserved_at`, sets `processed_at`.
- **failed**: records `Failed`, even at exhaustion, and clears reservation fields. The reservation attempt cap controls retry eligibility. Failure scheduling uses database time; see [item failure isolation](item-failure-isolation.md) for delay formulas.

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

### InboxMessage (application)

The application model for a stored message.

| Field            | Type                    | Description                             |
| ---------------- | ----------------------- | --------------------------------------- |
| `id`             | `i64`                   | Auto-increment primary key              |
| `status`         | `InboxStatus`           | Lifecycle state                         |
| `payload`        | `String`                | Raw UTF-8 message body                  |
| `meta`           | `Option<MetadataJsonb>` | JSONB-serialised routing metadata       |
| `scheduled_at`   | `DateTime<Utc>`         | Earliest eligible-for-reservation time  |
| `attempts`       | `i32`                   | Number of times reserved for processing |
| `reservation_id` | `Option<Uuid>`          | Ownership token assigned on reservation |
| `reserved_at`    | `Option<DateTime<Utc>>` | Timestamp of last reservation           |
| `received_at`    | `DateTime<Utc>`         | Timestamp of initial storage            |
| `updated_at`     | `DateTime<Utc>`         | Timestamp of last state change          |
| `processed_at`   | `Option<DateTime<Utc>>` | Timestamp of completion                 |

### InboxStatus (domain)

| Variant     | Code | Description                                                                |
| ----------- | ---- | -------------------------------------------------------------------------- |
| `Received`  | `0`  | Stored, eligible for reservation                                           |
| `Reserved`  | `2`  | Claimed by a consumer                                                      |
| `Failed`    | `4`  | Failure retained for inspection; retry eligible only below the attempt cap |
| `Completed` | `5`  | Successfully handed off                                                    |
| `Dead`      | `7`  | Legacy/manual terminal status; never an automatic failure outcome          |
| `Archive`   | `8`  | Archived (no further processing)                                           |

Numeric codes use intentional gaps (1, 3, 6 are reserved for future insertion without renumbering). See [ADR-002](adr/002-status-code-gaps.md).

### InboundMessageEnvelope (application)

Transport-facing inbound message before storage.

| Field      | Type                           | Description               |
| ---------- | ------------------------------ | ------------------------- |
| `payload`  | `String`                       | Raw message body          |
| `metadata` | `Option<InboxMessageMetadata>` | Optional routing metadata |

### InboxMessageMetadata (application)

| Field            | Type             | Description                            |
| ---------------- | ---------------- | -------------------------------------- |
| `message_id`     | `Option<Uuid>`   | Stable deduplication ID from transport |
| `correlation_id` | `Option<Uuid>`   | Cross-service correlation chain        |
| `source`         | `Option<String>` | Originating service identifier         |
| `routing_key`    | `Option<String>` | Routing hint from transport headers    |

### InboxMessageEnvelope (application)

Read-side envelope returned to consumers after reservation.

| Field      | Type                           | Description             |
| ---------- | ------------------------------ | ----------------------- |
| `message`  | `InboxMessage`                 | The stored domain model |
| `metadata` | `Option<InboxMessageMetadata>` | Decoded metadata        |

### NewInboxMessageEnvelope / NewInboxMessageMetadata

Write-side envelope used for recording. `message_id` is always `Uuid` (never `Option`) — generated as UUID v7 when absent from the transport.

### ReservableInboxSpec

| Field          | Default | Description                                        |
| -------------- | ------- | -------------------------------------------------- |
| `limit`        | —       | Maximum number of messages to reserve per call     |
| `max_attempts` | `6`     | Entries at or above this attempt count are skipped |

Builds query criteria: `StatusIn([Received, Failed])`, `ScheduledBeforeNow`, `MaxAttempts(max_attempts)`, `OrderByScheduledAtAsc`.

### StaleReservationSpec (stale_reservation_sweep)

| Field          | Default | Description                                                      |
| -------------- | ------- | ---------------------------------------------------------------- |
| `timeout`      | —       | Staleness threshold; entries reserved longer than this are swept |
| `max_attempts` | `6`     | Max attempts forwarded to the retry policy                       |

### InboxError (application)

| Variant              | Description                             |
| -------------------- | --------------------------------------- |
| `Transport`          | Transport adapter error                 |
| `Storage`            | Storage adapter error                   |
| `Acknowledgement`    | AMQP ack/nack failure                   |
| `Recording`          | Message recording failure               |
| `Reservation`        | Reservation query or update failure     |
| `Publish`            | Command gateway publish failure         |
| `MissingReservation` | Entry returned without a reservation_id |
| `Conversion`         | Envelope-to-command conversion failure  |

## Ports

### InboxStorePort

```rust
fn record(&self, envelope: NewInboxMessageEnvelope) -> Result<(), InboxError>;
```

### InboxReservePort

```rust
fn reserve(&self, spec: &ReservableInboxSpec) -> Result<Vec<InboxMessageEnvelope>, InboxError>;
```

### InboxProcessPort

```rust
fn completed(&self, id: i64, reservation_id: Uuid) -> Result<(), InboxError>;
fn failed(&self, id: i64, reservation_id: Uuid, max_attempts: i32) -> Result<(), InboxError>;
```

### InboxSweepPort

```rust
fn sweep(&self, spec: &StaleReservationSpec) -> Result<u64, InboxError>;
```

### InboxTransportPort

```rust
fn next(&mut self) -> Option<(InboundMessageEnvelope, Box<dyn AcknowledgeHandle>)>;
```

### AcknowledgeHandle

```rust
fn ack(&self) -> Result<(), InboxError>;
fn nack(&self) -> Result<(), InboxError>;
```

## Database Schema

Table: `inbox_entries`

```sql
CREATE TABLE inbox_entries (
    id              BIGSERIAL    PRIMARY KEY,
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

## Retry Policy

Applies to `failed()` transitions:

- Failure retry delay = `max(attempts, 1) × 30 seconds`, measured from database time
- Failure backoff is uncapped
- At `max_attempts` (default 6), failed entries remain `Failed` but are ineligible for automatic reservation
- Stale sweep does **not** increment the attempt counter

## Feature Flags

| Feature  | Adds                                                                                       |
| -------- | ------------------------------------------------------------------------------------------ |
| `diesel` | `DieselPgInboxRepository`, `DieselPgInboxRepository`, `DbPool`, Diesel + r2d2 dependencies |
| `amqp`   | `AmqpWorker`, `AmqpTransport`, `WorkerLoop`, lapin + tokio-amqp dependencies               |

## Examples

### Recording via AMQP worker

```rust
let mut worker = AmqpWorker::new(&channel, "inbox", store).await?;
worker.run(cancellation_token).await?;
```

### Manual reservation and processing

```rust
let spec = ReservableInboxSpec { limit: 10, max_attempts: 6 };
let entries = repository.reserve(&spec).await?;

for entry in entries {
    match process(&entry).await {
        Ok(_) => repository.completed(entry.message.id, reservation_id).await?,
        Err(_) => repository.failed(entry.message.id, reservation_id, spec.max_attempts).await?,
    }
}
```

### Stale reservation sweep

```rust
let spec = StaleReservationSpec {
    timeout: Duration::from_secs(15 * 60),
    max_attempts: 6,
};
let released = sweeper.sweep(&spec)?;
```

## Document Metadata

| Version | Author                          | Summary                               | Date       |
| ------- | ------------------------------- | ------------------------------------- | ---------- |
| 0.1.0   | Claude::claude-sonnet-4-6::high | Initial inbox technical specification | 2026-05-12 |

## Reusable HTTP endpoints

Framework preludes expose `inbox_routes(service)` and `InboxApi::new(service)`.
Both accept `impl Into<AsyncInboxService>`. Build one with
`AsyncInboxService::from_postgres(pool)` or `AsyncInboxService::new(store, consumer)`.
Relative `POST /run` processes a batch. Relative `POST /submit` accepts inbox messages with `202 Accepted`; a mount at `/inbox` therefore submits at `/inbox/submit`. Apps retaining `/inbox` can keep custom routes.
The host owns mount prefixes, authentication, authorization and middleware.
Existing custom adapters remain available; HTTP status/body contracts and Cargo
feature names are unchanged. The shared Poem run schemas remain in `mulac_http`.
