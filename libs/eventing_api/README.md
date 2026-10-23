# eventing_api

Framework-free HTTP contract and operation for dispatching one batch of due
events with the [`eventing`](../eventing) crate, shared by the
[`mulac_poem`](../mulac_poem) and [`mulac_axum`](../mulac_axum) adapters
(feature `eventing`). See
[ADR-010](../../docs/adr/010-queue-http-apis.md).

| Item                   | Purpose                                                        |
|------------------------|----------------------------------------------------------------|
| `RunEventsRequest`     | Batch request; `limit` from `1` to `1000`; `from_json` parses and validates |
| `RunEventsResponse`    | `processed` count on full success, otherwise `errors`          |
| `EventingApiError`     | Validation (`is_client_error`) and sanitized task failures     |
| `EventingService`      | Blocking `run` over an `Arc<EventConsumer>`                    |
| `AsyncEventingService` | Runs `EventingService` on Tokio's blocking pool (feature `tokio`) |

## Features

| Feature   | Default | Enables                                                    |
|-----------|---------|------------------------------------------------------------|
| `tokio`   | no      | `AsyncEventingService`                                     |
| `testing` | no      | `io::testing`: shared run-request fixtures and port-based doubles |

The crate has no `diesel` feature and its dependency tree contains no database
driver; storage comes with the consumer.

## Construction

```rust
// The application or kernel owns the dispatcher and subscriber registry.
let service = EventingService::new(kernel.event_consumer());
let service = AsyncEventingService::from(service);
```

There is no `from_postgres` constructor: an event consumer dispatches to the
application's subscribers, which the kernel assembles in `start_persistent`.
The crate never sees subscriber names or event types. `AsyncEventingService`
uses the caller's runtime. Dropping a request future does not stop a batch
that has already started.

## Contract

- `limit` is a required integer from `1` to `1000`. Unknown fields are ignored.
- `EventingService::run` validates the request again, then dispatches up to
  `limit` due events with the default six attempts.
- Event failures are returned in `RunEventsResponse::errors` with their
  `EventError` text, not as errors. Events that were dispatched stay
  completed. A failed event whose failure cannot be recorded contributes two
  errors, the storage error first.
- A blocking task failure displays `queue processing task failed`; the cause
  stays in `source()`.
- The run types are copies of `inbox_api`'s. `testing::valid_run_requests`,
  `invalid_run_requests` and `run_request_error_messages` are the shared table
  every `*_api` crate and adapter runs, so the copies cannot drift silently.
