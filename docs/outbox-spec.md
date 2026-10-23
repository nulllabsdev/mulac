# Outbox Specification

## Overview

The outbox crate provides a durable outbound message buffer that decouples internal event processing from external transport publication. It accepts event envelopes from the event dispatcher, stores outbound work atomically, and exposes stored entries to consumers that publish each entry to a broker or other external transport. The event dispatcher is the only caller expected to record into the outbox.

The outbox guarantees at-least-once delivery to the external transport boundary. Reservation, retry scheduling, non-retriable dead-lettering, and stale-reservation recovery are built in. Duplicate publication is possible and must be tolerated by downstream systems.

## Architecture

The crate follows the same hexagonal (ports and adapters) architecture as the inbox crate:

```
domain  ←  application  ←  features  ←  infra adapters
```

- **domain** (`assembly/domain.rs`): core models and status machine — no I/O, no external transport types
- **application** (`assembly/application.rs`): port traits, event/outbound envelopes, and errors — no implementation
- **features** (`record_events.rs`, `outbox_consumer.rs`, `stale_reservation_sweep.rs`): use-case orchestration — depends on ports only
- **infra adapters** (`assembly/infra_diesel.rs`, `amqp_publisher.rs`, `confirmed_amqp_publisher.rs`): concrete I/O — depends on feature and application layers

External callers import exclusively from `lib.rs` via the `io` re-export facade.

```
lib.rs (io facade)
  ├─ record_events          OutboxRecorder, OutboxRecorderRepository
  ├─ outbox_consumer        OutboxConsumer, OutboxConsumerRepository, ReservableOutboxSpec
  ├─ stale_reservation_sweep  ReservationSweeper, StaleReservationSpec
  ├─ assembly/application   NewOutboxEnvelope, NewOutboxMetadata,
  │                         OutboxEntryEnvelope, OutboxEntryMetadata,
  │                         OutboundMessageEnvelope, NewOutboxEntry, OutboxError,
  │                         OutboxStorePort, OutboxReservePort, OutboxProcessPort,
  │                         OutboxPublisherPort
  ├─ assembly/infra_diesel  [feature = "diesel"]
  │    OutboxStoreStorage, OutboxConsumerStorage, DbPool
  ├─ amqp_publisher         [feature = "amqp"]
  │    AmqpPublisher, AmqpPublishConfig
  └─ confirmed_amqp_publisher  [feature = "amqp"]
       ConfirmedAmqpPublisher
```

## Components

### OutboxRecorder / OutboxRecorderRepository

Use case: accept an event from the event dispatcher and record outbound work durably.

- Receives a `NewOutboxEnvelope` from the event dispatcher; no event ID generation is performed inside the outbox.
- Requires `event_id` and `routing_key` in metadata. `event_id` becomes the `OutboxEntry.id` and the default outbound `message_id`.
- Validates that the event metadata contains a routable `routing_key`.
- Converts the input envelope into a `NewOutboxEntry`.
- Stores exactly one `outbox_entries` row for each accepted event ID.
- Does not touch existing rows on conflict, making recording idempotent when the event dispatcher retries subscriber delivery.
- Returns success only after durable storage succeeds.
- Rejects routing errors before storage, so invalid routing does not create an outbox row.

### OutboxConsumer / OutboxConsumerRepository

Use case: reserve stored outbound work and publish it to an external transport.

- Reserves up to `limit` eligible entries atomically.
- Converts each `OutboxEntryEnvelope` into an `OutboundMessageEnvelope`.
- Publishes each outbound message via `OutboxPublisherPort` and waits for broker acceptance.
- Marks each entry `Completed` after broker acceptance.
- Marks each entry `Failed` when publication fails with a retriable transport error.
- Marks each entry `Dead` when post-acceptance transformation fails permanently, such as payload serialisation.
- Returns all errors collected across the batch; does not abort early.

### ReservationSweeper

Use case: release stale reservations back to the eligible queue.

- Finds all `Reserved` entries whose `reserved_at` is older than the configured timeout.
- Resets them to `Failed` with retry scheduling, clearing reservation fields.
- Does not increment the attempt counter; only explicit `failed()` calls count as attempts.

### OutboxStoreStorage (infra, Diesel)

Implements `OutboxStorePort`. Inserts a new `outbox_entries` row. Does not touch existing rows on conflict. The insert, or an idempotent conflict on an already-recorded event ID, is the outbox acceptance boundary.

### OutboxConsumerStorage (infra, Diesel)

Implements `OutboxReservePort`, `OutboxProcessPort`, and `OutboxSweepPort`.

- **reserve**: `SELECT … FOR UPDATE SKIP LOCKED`, then updates status to `Reserved`, assigns `reservation_id` (UUID v7), increments `attempts`, and returns full envelopes.
- **completed**: Updates status to `Completed`, clears `reservation_id` and `reserved_at`, sets `processed_at`.
- **failed**: Evaluates `attempts` against `max_attempts`; transitions to `Failed` with retry scheduling or `Dead` when exhausted. Clears reservation fields.
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

| Outcome | Error |
|---------|-------|
| Connection, channel or publish failure, or an unparseable URL | `transport error: RabbitMQ connection or publication failed` |
| Timeout | `transport error: RabbitMQ publication timed out` |
| No current runtime | `transport error: RabbitMQ publication requires the application runtime` |
| `Ack` with a returned (unroutable) message | `routing error: RabbitMQ returned an unroutable message` |
| `Nack` | `publish error: RabbitMQ negatively acknowledged publication` |
| Confirms not active | `publish error: RabbitMQ did not confirm publication` |

The broker test is ignored by default; run it with
`AMQP_URL_TEST=amqp://… cargo test -p mulac-outbox --features amqp -- --ignored`
from `libs/`.

## Models

### OutboxEntry (domain)

The core domain model for stored outbound work.

| Field            | Type                    | Description                                      |
|------------------|-------------------------|--------------------------------------------------|
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

| Variant     | Code | Description                                    |
|-------------|------|------------------------------------------------|
| `Received`  | `0`  | Stored, eligible for reservation               |
| `Reserved`  | `2`  | Claimed by an outbox consumer                  |
| `Failed`    | `4`  | Publication failed; scheduled for retry        |
| `Completed` | `5`  | Broker accepted the outbound message           |
| `Dead`      | `7`  | Retry limit exhausted or non-retriable failure |
| `Archive`   | `8`  | Archived (no further processing)               |

Numeric codes use intentional gaps (1, 3, 6 are reserved for future insertion without renumbering). See [ADR-002](adr/002-status-code-gaps.md).

### NewOutboxEnvelope (application)

Event-dispatcher-facing input before storage. The event dispatcher must provide the stable event identity; the outbox does not generate it.

| Field      | Type                | Description                               |
|------------|---------------------|-------------------------------------------|
| `payload`  | `String`            | Event payload before outbound publication |
| `metadata` | `NewOutboxMetadata` | Required event and routing metadata       |

### NewOutboxMetadata (application)

Metadata accepted from the event dispatcher.

| Field            | Type             | Description                                          |
|------------------|------------------|------------------------------------------------------|
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
|------------------|------------------|------------------------------------------------------|
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
|------------|-----------------------|-------------------------|
| `message`  | `OutboxEntry`         | The stored domain model |
| `metadata` | `OutboxEntryMetadata` | Decoded metadata        |

### NewOutboxEntry (domain)

Write-side struct passed to `OutboxStorePort` for insertion. Contains only the fields supplied or derived during acceptance; database-managed fields (`attempts`, `reservation_id`, `reserved_at`, `updated_at`, `processed_at`, `last_error`) are absent.

| Field          | Type                  | Description                                        |
|----------------|-----------------------|----------------------------------------------------|
| `id`           | `Uuid`                | UUID v7, sourced from `NewOutboxMetadata.event_id` |
| `payload`      | `String`              | Serialised event payload                           |
| `meta`         | `OutboxEntryMetadata` | JSONB-serialised routing metadata                  |
| `scheduled_at` | `DateTime<Utc>`       | Earliest eligible-for-reservation time             |
| `received_at`  | `DateTime<Utc>`       | Timestamp of initial storage                       |

### OutboundMessageEnvelope (application)

Transport-facing outbound message after reservation and transformation.

| Field      | Type                  | Description                          |
|------------|-----------------------|--------------------------------------|
| `payload`  | `Vec<u8>`             | Bytes sent to the external transport |
| `metadata` | `OutboxEntryMetadata` | Routing and message properties       |

### ReservableOutboxSpec (outbox_consumer)

| Field          | Default | Description                                        |
|----------------|---------|----------------------------------------------------|
| `limit`        | —       | Maximum number of entries to reserve per call      |
| `max_attempts` | `6`     | Entries at or above this attempt count are skipped |

Builds query criteria: `StatusIn([Received, Failed])`, `ScheduledBeforeNow`, `MaxAttempts(max_attempts)`, `OrderByScheduledAtAsc`.

### StaleReservationSpec (stale_reservation_sweep)

| Field          | Default | Description                                                      |
|----------------|---------|------------------------------------------------------------------|
| `timeout`      | —       | Staleness threshold; entries reserved longer than this are swept |
| `max_attempts` | `6`     | Max attempts forwarded to the retry policy                       |

### OutboxError (application)

| Variant              | Description                                     |
|----------------------|-------------------------------------------------|
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
async fn record(&self, entry: &NewOutboxEntry) -> Result<(), OutboxError>;
```

### OutboxReservePort

```rust
async fn reserve(&self, spec: &ReservableOutboxSpec) -> Result<Vec<OutboxEntryEnvelope>, OutboxError>;
```

### OutboxProcessPort

```rust
async fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), OutboxError>;
async fn failed(&self, id: Uuid, reservation_id: Uuid, max_attempts: i32, reason: Option<String>) -> Result<(), OutboxError>;
async fn dead(&self, id: Uuid, reservation_id: Uuid, reason: Option<String>) -> Result<(), OutboxError>;
```

### OutboxSweepPort

```rust
async fn sweep(&self, spec: &StaleReservationSpec) -> Result<u64, OutboxError>;
```

### OutboxPublisherPort

```rust
async fn publish(&self, envelope: OutboundMessageEnvelope) -> Result<(), OutboxError>;
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

Applies to retriable `failed()` transitions caused by transport publication failures:

- Retry delay = `attempts × 30 seconds`
- Cap: 2 minutes per attempt
- After `max_attempts` (default 6) the entry transitions to `Dead`
- Stale sweep does **not** increment the attempt counter
- `dead()` bypasses retry scheduling and is reserved for non-retriable post-acceptance transformation failures

## Delivery and Idempotency

- The outbox acceptance boundary is durable storage of the `OutboxEntry`, or an idempotent conflict on an already-recorded `event_id`.
- The publication boundary is broker acceptance / publisher confirmation.
- If broker acceptance succeeds but `completed()` fails, the entry may be retried and the same outbound message may be published again.
- Consumers downstream of the external transport must treat `message_id` as an idempotency key when possible. By default, `message_id` equals `event_id`, unless the event dispatcher supplied a distinct outbound message ID.
- The outbox does not guarantee ordering across entries, destinations, retries, or concurrent consumers.

## HTTP Batch Endpoint

`POST /outbox/run` is provided by [`libs/outbox_api`](../libs/outbox_api) and
exposed through the `outbox` feature of `mulac_poem` and `mulac_axum`
([ADR-010](adr/010-queue-http-apis.md)). It publishes one batch through
`OutboxConsumer::publish_batch` with `ReservableOutboxSpec::new(limit)`:

- `limit` is required, from `1` to `1000`; other values return `400`.
- A processed batch returns `200` with `{"processed":N,"errors":[]}`. Any
  failure sets `processed` to `null` and lists each `OutboxError` text in
  `errors`; published entries stay completed.
- Without a configured publisher, valid requests return `503`
  `outbox publisher is not configured` and storage is not touched.
- A failed blocking task returns `500` `queue processing task failed`.

`OutboxService::from_postgres(pool, publisher)` assembles the consumer from
`consumer_repository(pool)`. The publisher is any `OutboxPublisherPort`; the
API crate never enables `amqp`.

## Feature Flags

| Feature  | Adds                                                                |
|----------|---------------------------------------------------------------------|
| `diesel` | PostgreSQL storage adapters, schema mapping, JSONB metadata support |
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
