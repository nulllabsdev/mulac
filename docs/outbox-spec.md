# Outbox Specification

See [item failure isolation](item-failure-isolation.md) for the implemented
transactional reservation, item-level panic containment, and recovery limits.

## Overview

The outbox crate provides a durable outbound message buffer that decouples internal event processing from external transport publication. It accepts event envelopes from the event dispatcher, stores outbound work atomically, and exposes stored entries to consumers that publish each entry to a broker or other external transport. The event dispatcher is the only caller expected to record into the outbox.

The outbox guarantees at-least-once delivery to the external transport boundary. Reservation, retry scheduling, failure recording, and stale-reservation recovery are built in. Duplicate publication is possible and must be tolerated by downstream systems.

## Architecture

Private layers separate domain status/timeout rules, application envelopes and
ports, the execution core (consumer and reservation spec), Diesel persistence, and HTTP adaptation. Only
`prelude` is public. `DieselPgOutboxRepository::new(pool)` implements all four storage ports.
`OutboxRecorder` and `ReservationSweeper` take their trait-object ports; `OutboxConsumer<R>` holds one `Arc<R>`
implementing reserve/process. `AsyncOutboxService` wraps the consumer and erases `R` at the
blocking-task boundary. There is no synchronous HTTP service, so callers without
Tokio use the consumer directly. Synchronous code supports borrowed repositories.
Transport adapters retain their independent acknowledgment/publication behavior.
See [ADR-012](adr/012-unified-queue-repositories.md) and
[ADR-014](adr/014-merge-sibling-execution-bridges.md).

## Components

### OutboxRecorder

Use case: accept an event from the event dispatcher and record outbound work durably.

- Receives a `NewOutboxEnvelope` from the event dispatcher; no event ID generation is performed inside the outbox.
- Requires `event_id` and `routing_key` in metadata. `event_id` becomes the `OutboxEntry.id` and the default outbound `message_id`.
- Validates that the event metadata contains a routable `routing_key`.
- Converts the input envelope into a `NewOutboxEntry`.
- Stores exactly one `outbox_entries` row for each accepted event ID.
- Does not touch existing rows on conflict, making recording idempotent when the event dispatcher retries subscriber delivery.
- Returns success only after durable storage succeeds.
- Rejects routing errors before storage, so invalid routing does not create an outbox row.

### OutboxConsumer<R>

Use case: reserve stored outbound work and publish it to an external transport.

- Reserves up to `limit` eligible entries atomically.
- Converts each `OutboxEntryEnvelope` into an `OutboundMessageEnvelope`.
- Publishes each outbound message via `OutboxPublisherPort` and waits for broker acceptance.
- Marks each entry `Completed` after broker acceptance.
- Marks each entry `Failed` on processing or completion error, including recoverable unwinding panics.
- Conversion failures also become `Failed`; the automatic consumer never calls `dead()`.
- Returns all errors collected across the batch; does not abort early.

### ReservationSweeper

Use case: release stale reservations back to the eligible queue.

- Finds all `Reserved` entries whose `reserved_at` is older than the configured timeout.
- Sets all timed-out rows to `Failed`, including exhausted rows, and clears reservation fields. Only entries below the attempt cap can be reserved again.
- Does not increment the attempt counter; reservation increments attempts, not failure or sweeping.

### DieselPgOutboxRepository: recording

Implements `OutboxStorePort`. Inserts a new `outbox_entries` row. Does not touch existing rows on conflict. The insert, or an idempotent conflict on an already-recorded event ID, is the outbox acceptance boundary.

### DieselPgOutboxRepository: processing and recovery

Implements `OutboxReservePort`, `OutboxProcessPort`, and `OutboxSweepPort`.

- **reserve**: `SELECT … FOR UPDATE SKIP LOCKED`, then updates status to `Reserved`, assigns `reservation_id` (UUID v7), increments `attempts`, and returns full envelopes.
  Reservation, raw JSON loading, per-row decoding, and malformed-row failure writes share one transaction. Only healthy envelopes are returned after commit; the candidate limit is not refilled.
- **completed**: Updates status to `Completed`, clears `reservation_id` and `reserved_at`, sets `processed_at`.
- **failed**: records `Failed`, even at exhaustion, and clears reservation fields. The reservation attempt cap controls retry eligibility. Failure scheduling uses database time; see [item failure isolation](item-failure-isolation.md) for delay formulas.
- **dead**: Transitions directly to `Dead`, records the failure reason when available, and clears reservation fields without scheduling retry.

### AmqpPublisher

Implements `OutboxPublisherPort` for AMQP.

- Resolves the AMQP exchange and routing key from `OutboundMessageEnvelope` metadata.
- Publishes the payload and required message properties.
- Waits for publisher confirmation / broker acceptance before returning success.
- Maps broker unavailability, negative confirmations, and channel failures to retriable `OutboxError::Transport` errors.

### ConfirmedAmqpPublisher

A strict `OutboxPublisherPort` for AMQP, also behind the `amqp` feature.
`AmqpPublisher` keeps its behavior; choose this publisher when a message must
not count as published unless the broker routed and confirmed it.

- `ConfirmedAmqpPublisher::new(url, exchange)` does not connect and does not
  validate the URL; lapin parses it on the first connection. Environment
  parsing and URL policy stay with the application.
- The connection and channel are opened lazily, with `confirm_select` once per
  connection, reused after successful publishes and dropped after any failure,
  so the next publish reconnects.
- Every message is published with `mandatory: true` and persistent delivery
  (`delivery_mode` 2). Properties: `message_id`, optional `correlation_id`,
  the content type (default `application/json`) and the headers `event_id`,
  `event_type` and optional `causation_id` and `source`.
- Connecting and publishing are bounded by a timeout, 10 seconds by default
  (`ConfirmedAmqpPublisher::DEFAULT_TIMEOUT`, `with_timeout`).
- `publish` never creates a runtime. It needs a current Tokio runtime handle
  and must run on that runtime's blocking pool, as `outbox_api`'s
  `AsyncOutboxService` does.
- Errors are fixed strings without broker diagnostics:

| Outcome                                                       | Error                                                                    |
| ------------------------------------------------------------- | ------------------------------------------------------------------------ |
| Connection, channel or publish failure, or an unparseable URL | `transport error: RabbitMQ connection or publication failed`             |
| Timeout                                                       | `transport error: RabbitMQ publication timed out`                        |
| No current runtime                                            | `transport error: RabbitMQ publication requires the application runtime` |
| `Ack` with a returned (unroutable) message                    | `routing error: RabbitMQ returned an unroutable message`                 |
| `Nack`                                                        | `publish error: RabbitMQ negatively acknowledged publication`            |
| Confirms not active                                           | `publish error: RabbitMQ did not confirm publication`                    |

The broker test is ignored by default; run it with
`AMQP_URL_TEST=amqp://… cargo test -p mulac-outbox --features amqp -- --ignored`
from `libs/`.

## Timeout recovery contract

Sweeps run in one transaction and take one PostgreSQL `transaction_timestamp()`.
Only `Reserved` rows with non-null `reserved_at < database_time - timeout` are
selected, with blocking row locks held through the update. A pure internal policy
calculates each outcome; one set-based update clears reservation fields, preserves
existing errors and appends the timeout reason. Errors roll back the entire sweep.
Attempts do not change. The default retry limit is 6; configure the same limit on
consumer and sweeper. No recovery worker is started automatically.

Retry delay is `min(max(attempts, 1) × 30 seconds, 120 seconds)`. Exhausted
rows keep their previous `scheduled_at`; retryable rows use database time + delay.
Explicit failure paths now use database time; manual outbox `dead()` remains separate.

## Models

### OutboxEntry (domain)

The core domain model for stored outbound work.

| Field            | Type                    | Description                                      |
| ---------------- | ----------------------- | ------------------------------------------------ |
| `id`             | `Uuid`                  | UUID v7 primary key, sourced from event metadata |
| `status`         | `OutboxStatus`          | Lifecycle state                                  |
| `payload`        | `String`                | Serialised event payload                         |
| `meta`           | `OutboxEntryMetadata`   | JSONB-serialised routing metadata                |
| `scheduled_at`   | `DateTime<Utc>`         | Earliest eligible-for-reservation time           |
| `attempts`       | `i32`                   | Number of times reserved for publishing          |
| `reservation_id` | `Option<Uuid>`          | Ownership token assigned on reservation          |
| `reserved_at`    | `Option<DateTime<Utc>>` | Timestamp of last reservation                    |
| `received_at`    | `DateTime<Utc>`         | Timestamp of initial storage                     |
| `updated_at`     | `DateTime<Utc>`         | Timestamp of last state change                   |
| `processed_at`   | `Option<DateTime<Utc>>` | Timestamp of broker acceptance                   |
| `last_error`     | `Option<String>`        | Last failure reason for operations               |

### OutboxStatus (domain)

| Variant     | Code | Description                                                                |
| ----------- | ---- | -------------------------------------------------------------------------- |
| `Received`  | `0`  | Stored, eligible for reservation                                           |
| `Reserved`  | `2`  | Claimed by an outbox consumer                                              |
| `Failed`    | `4`  | Failure retained for inspection; retry eligible only below the attempt cap |
| `Completed` | `5`  | Broker accepted the outbound message                                       |
| `Dead`      | `7`  | Legacy/manual terminal status; never an automatic failure outcome          |
| `Archive`   | `8`  | Archived (no further processing)                                           |

Numeric codes use intentional gaps (1, 3, 6 are reserved for future insertion without renumbering). See [ADR-002](adr/002-status-code-gaps.md).

### NewOutboxEnvelope (application)

Event-dispatcher-facing input before storage. The event dispatcher must provide the stable event identity; the outbox does not generate it.

| Field      | Type                | Description                               |
| ---------- | ------------------- | ----------------------------------------- |
| `payload`  | `String`            | Event payload before outbound publication |
| `metadata` | `NewOutboxMetadata` | Required event and routing metadata       |

### NewOutboxMetadata (application)

Metadata accepted from the event dispatcher.

| Field            | Type             | Description                                          |
| ---------------- | ---------------- | ---------------------------------------------------- |
| `event_id`       | `Uuid`           | Stable event ID; becomes `OutboxEntry.id`            |
| `message_id`     | `Option<Uuid>`   | Optional outbound message ID; defaults to `event_id` |
| `correlation_id` | `Option<Uuid>`   | Cross-service correlation chain                      |
| `causation_id`   | `Option<Uuid>`   | Command/message that caused this event               |
| `event_type`     | `String`         | Event type name                                      |
| `routing_key`    | `String`         | Destination routing key; required at acceptance      |
| `source`         | `Option<String>` | Originating service identifier                       |
| `content_type`   | `Option<String>` | Payload content type, for example `application/json` |

### OutboxEntryMetadata (application)

Decoded metadata attached to a stored or reserved `OutboxEntry`. `message_id` is always present after recording.

| Field            | Type             | Description                                          |
| ---------------- | ---------------- | ---------------------------------------------------- |
| `event_id`       | `Uuid`           | Original event identifier                            |
| `message_id`     | `Uuid`           | Stable outbound message ID / idempotency key         |
| `correlation_id` | `Option<Uuid>`   | Cross-service correlation chain                      |
| `causation_id`   | `Option<Uuid>`   | Command/message that caused this event               |
| `event_type`     | `String`         | Event type name                                      |
| `routing_key`    | `String`         | Destination routing key                              |
| `source`         | `Option<String>` | Originating service identifier                       |
| `content_type`   | `Option<String>` | Payload content type, for example `application/json` |

### OutboxEntryEnvelope (application)

Read-side envelope returned to consumers after reservation.

| Field      | Type                  | Description             |
| ---------- | --------------------- | ----------------------- |
| `message`  | `OutboxEntry`         | The stored domain model |
| `metadata` | `OutboxEntryMetadata` | Decoded metadata        |

### NewOutboxEntry (domain)

Write-side struct passed to `OutboxStorePort` for insertion. Contains only the fields supplied or derived during acceptance; database-managed fields (`attempts`, `reservation_id`, `reserved_at`, `updated_at`, `processed_at`, `last_error`) are absent.

| Field          | Type                  | Description                                        |
| -------------- | --------------------- | -------------------------------------------------- |
| `id`           | `Uuid`                | UUID v7, sourced from `NewOutboxMetadata.event_id` |
| `payload`      | `String`              | Serialised event payload                           |
| `meta`         | `OutboxEntryMetadata` | JSONB-serialised routing metadata                  |
| `scheduled_at` | `DateTime<Utc>`       | Earliest eligible-for-reservation time             |
| `received_at`  | `DateTime<Utc>`       | Timestamp of initial storage                       |

### OutboundMessageEnvelope (application)

Transport-facing outbound message after reservation and transformation.

| Field      | Type                  | Description                          |
| ---------- | --------------------- | ------------------------------------ |
| `payload`  | `Vec<u8>`             | Bytes sent to the external transport |
| `metadata` | `OutboxEntryMetadata` | Routing and message properties       |

### ReservableOutboxSpec (outbox_consumer)

| Field          | Default | Description                                        |
| -------------- | ------- | -------------------------------------------------- |
| `limit`        | —       | Maximum number of entries to reserve per call      |
| `max_attempts` | `6`     | Entries at or above this attempt count are skipped |

Builds query criteria: `StatusIn([Received, Failed])`, `ScheduledBeforeNow`, `MaxAttempts(max_attempts)`, `OrderByScheduledAtAsc`.

### StaleReservationSpec (stale_reservation_sweep)

| Field          | Default | Description                                                      |
| -------------- | ------- | ---------------------------------------------------------------- |
| `timeout`      | —       | Staleness threshold; entries reserved longer than this are swept |
| `max_attempts` | `6`     | Max attempts forwarded to the retry policy                       |

### OutboxError (application)

| Variant              | Description                                     |
| -------------------- | ----------------------------------------------- |
| `Storage`            | Storage adapter error                           |
| `Routing`            | Missing or unresolvable routing key             |
| `Serialization`      | Payload cannot be converted for the transport   |
| `Transport`          | Transport adapter or broker acceptance error    |
| `Reservation`        | Reservation query or update failure             |
| `Publish`            | Outbound publish orchestration failure          |
| `MissingReservation` | Entry returned without a `reservation_id`       |
| `Conversion`         | Envelope-to-outbound-message conversion failure |

## Ports

### OutboxStorePort

```rust
fn record(&self, entry: &NewOutboxEntry) -> Result<(), OutboxError>;
```

### OutboxReservePort

```rust
fn reserve(&self, spec: &ReservableOutboxSpec) -> Result<Vec<OutboxEntryEnvelope>, OutboxError>;
```

### OutboxProcessPort

```rust
fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), OutboxError>;
fn failed(&self, id: Uuid, reservation_id: Uuid, max_attempts: i32, reason: Option<String>) -> Result<(), OutboxError>;
fn dead(&self, id: Uuid, reservation_id: Uuid, reason: Option<String>) -> Result<(), OutboxError>;
```

### OutboxSweepPort

```rust
fn sweep(&self, spec: &StaleReservationSpec) -> Result<u64, OutboxError>;
```

### OutboxPublisherPort

```rust
fn publish(&self, envelope: OutboundMessageEnvelope) -> Result<(), OutboxError>;
```

## Database Schema

Table: `outbox_entries`

```sql
CREATE TABLE outbox_entries (
    id              UUID         PRIMARY KEY,
    status          INTEGER      NOT NULL,
    payload         TEXT         NOT NULL,
    meta            JSONB        NOT NULL,
    scheduled_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    attempts        INTEGER      NOT NULL DEFAULT 0,
    reservation_id  UUID,
    reserved_at     TIMESTAMPTZ,
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ,
    last_error      TEXT
);
```

Recommended indexes:

- `(status, scheduled_at)` — reservation queries
- `(status, reserved_at)` — stale sweep queries
- `(meta ->> 'routing_key')` — optional operational lookup by destination
- `(meta ->> 'message_id')` — optional downstream idempotency lookup

The primary key is UUID (not BIGSERIAL) because the event ID is generated before outbox storage and is reused as the idempotency key for subscriber retries. See [ADR-003](adr/003-uuid-v7-identifiers.md).

## Retry Policy

Applies to all automatic `failed()` transitions, including conversion and publication failures:

- Failure retry delay = `max(attempts, 1) × 30 seconds`, measured from database time
- Cap: 2 minutes per attempt
- At `max_attempts` (default 6), failed entries remain `Failed` but are ineligible for automatic reservation
- Stale sweep does **not** increment the attempt counter
- Explicit administrative `dead()` remains available; automatic consumers never invoke it

## Delivery and Idempotency

- The outbox acceptance boundary is durable storage of the `OutboxEntry`, or an idempotent conflict on an already-recorded `event_id`.
- The publication boundary is broker acceptance / publisher confirmation.
- If broker acceptance succeeds but `completed()` fails, the entry may be retried and the same outbound message may be published again.
- Consumers downstream of the external transport must treat `message_id` as an idempotency key when possible. By default, `message_id` equals `event_id`, unless the event dispatcher supplied a distinct outbound message ID.
- The outbox does not guarantee ordering across entries, destinations, retries, or concurrent consumers.

## HTTP Batch Endpoint

`POST /run` is provided by this crate's `axum` and `poem` features and mounted under
an application-owned prefix ([ADR-011](adr/011-core-http-features.md)). It publishes
one batch through
`OutboxConsumer::publish_batch` with `ReservableOutboxSpec::new(limit)`:

- `limit` must be from `1` to `1000`; other values return `400` with that message.
  A body that can't be parsed returns `400` with only a logged error ID.
- A processed batch returns `200` with `{"processed":N,"errors":[]}`. Any
  failure sets `processed` to `null` and lists one logged error ID per failure in
  `errors`; published entries stay completed.
- Without a configured publisher, valid requests return `503`
  `outbox publisher is not configured` and storage is not touched.
- A failed blocking task returns `500` with only a logged error ID.

Error IDs are explained in [ADR-015](adr/015-http-errors-by-id.md).

`AsyncOutboxService::from_postgres(pool, publisher)` assembles the consumer from
`Arc::new(DieselPgOutboxRepository::new(pool))`. The publisher is any `OutboxPublisherPort`; the
HTTP features never enable `amqp`.

## Feature Flags

| Feature  | Adds                                                                   |
| -------- | ---------------------------------------------------------------------- |
| `diesel` | PostgreSQL storage adapters, schema mapping, JSONB metadata support    |
| `amqp`   | `AmqpPublisher` and the strict `ConfirmedAmqpPublisher` (lapin, Tokio) |

## Examples

### Recording an event

```rust
let recorder = OutboxRecorder::new(repository);
recorder.record(NewOutboxEnvelope {
    payload: r#"{"user_id":"123"}"#.to_string(),
    metadata: NewOutboxMetadata {
        event_id,
        message_id: None,
        correlation_id: Some(correlation_id),
        causation_id: Some(command_id),
        event_type: "UserRegistered".to_string(),
        routing_key: "users.registered".to_string(),
        source: Some("identity-service".to_string()),
        content_type: Some("application/json".to_string()),
    },
}).await?;
```

### Publishing reserved entries

```rust
let spec = ReservableOutboxSpec { limit: 100, max_attempts: 6 };
let mut consumer = OutboxConsumer::new(repository, publisher);
let result = consumer.publish_batch(&spec).await;
```

## Reusable HTTP endpoints

Framework preludes expose `outbox_routes(service)` and `OutboxApi::new(service)`.
Both accept `impl Into<AsyncOutboxService>`, so an `Arc<OutboxConsumer<R>>` can be
passed directly.
Relative `POST /run` processes a batch.
The host owns mount prefixes, authentication, authorization and middleware.
Existing custom adapters remain available; HTTP status/body contracts and Cargo
feature names are unchanged. The shared Poem run schemas remain in `mulac_http`.
