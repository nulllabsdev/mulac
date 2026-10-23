# outbox_api

Framework-free HTTP contract and operation for publishing one batch of due
outbox entries with the [`outbox`](../outbox) crate, shared by the
[`mulac_poem`](../mulac_poem) and [`mulac_axum`](../mulac_axum) adapters
(feature `outbox`). See
[ADR-010](../../docs/adr/010-queue-http-apis.md).

| Item                 | Purpose                                                          |
|----------------------|------------------------------------------------------------------|
| `RunOutboxRequest`   | Batch request; `limit` from `1` to `1000`; `from_json` parses and validates |
| `RunOutboxResponse`  | `processed` count on full success, otherwise `errors`            |
| `OutboxApiError`     | Validation, missing publisher and sanitized task failures        |
| `OutboxApiErrorKind` | `Invalid`, `Unavailable` or `Internal`, returned by `OutboxApiError::kind` |
| `OutboxService`      | Blocking `run` over an `Arc<OutboxConsumer>`                     |
| `AsyncOutboxService` | Runs `OutboxService` on Tokio's blocking pool, or is `unconfigured()` (feature `tokio`) |

## Features

| Feature   | Default | Enables                                                    |
|-----------|---------|------------------------------------------------------------|
| `diesel`  | yes     | `OutboxService::from_postgres` and Diesel in `mulac-outbox` |
| `tokio`   | no      | `AsyncOutboxService`                                       |
| `testing` | no      | `io::testing`: shared run-request fixtures and port-based doubles |

Without `diesel` the dependency tree contains no database driver. The crate
never depends on a broker client, and does not enable `mulac-outbox/amqp`.

## Construction

```rust
// PostgreSQL, using an existing pool and an application-owned publisher.
let service = OutboxService::from_postgres(pool, Arc::new(publisher));

// Explicit consumer, e.g. custom storage or test doubles.
let service = OutboxService::new(consumer);

let service = AsyncOutboxService::from(service);

// Delivery switched off: valid requests fail with PublisherNotConfigured.
let service = AsyncOutboxService::unconfigured();
```

`from_postgres` exists, unlike in `commanding_api` and `eventing_api`, because
no kernel assembles an outbox consumer. The publisher, its broker
configuration and topology stay with the application.

`AsyncOutboxService` uses the caller's runtime and creates no runtime, pool or
connection. Publishers run on a blocking-pool thread of that runtime, so they
may block on it with `Handle::current().block_on`. Do not call the blocking
`OutboxService::run` from async code. Dropping a request future does not stop
a batch that has already started, so messages may still be published:
delivery stays at least once.

## Contract

- `limit` is a required integer from `1` to `1000`. Unknown fields are ignored.
- `OutboxService::run` validates the request again, then publishes up to
  `limit` due entries with the default six attempts.
- Publication and storage failures are returned in
  `RunOutboxResponse::errors` with their `OutboxError` text, not as errors.
  Published entries stay completed. A failed entry whose failure cannot be
  recorded contributes two errors, the storage error first. Entries without a
  routing key are marked dead.
- An unconfigured service validates the request, then displays
  `outbox publisher is not configured` (`Unavailable`) without touching
  storage.
- A blocking task failure displays `queue processing task failed`
  (`Internal`); the cause stays in `source()`.
- The run types are copies of `inbox_api`'s. `testing::valid_run_requests`,
  `invalid_run_requests` and `run_request_error_messages` are the shared table
  every `*_api` crate and adapter runs, so the copies cannot drift silently.
