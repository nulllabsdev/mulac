# commanding_api

Framework-free HTTP contract and operation for running one batch of due
commands with the [`commanding`](../commanding) crate, shared by the
[`mulac_poem`](../mulac_poem) and [`mulac_axum`](../mulac_axum) adapters
(feature `commanding`). See
[ADR-010](../../docs/adr/010-queue-http-apis.md).

| Item                     | Purpose                                                        |
|--------------------------|----------------------------------------------------------------|
| `RunCommandsRequest`     | Batch request; `limit` from `1` to `1000`; `from_json` parses and validates |
| `RunCommandsResponse`    | `processed` count on full success, otherwise `errors`          |
| `CommandingApiError`     | Validation (`is_client_error`) and sanitized task failures     |
| `CommandingService`      | Blocking `run` over an `Arc<CommandConsumer>`                  |
| `AsyncCommandingService` | Runs `CommandingService` on Tokio's blocking pool (feature `tokio`) |

## Features

| Feature   | Default | Enables                                                    |
|-----------|---------|------------------------------------------------------------|
| `tokio`   | no      | `AsyncCommandingService`                                   |
| `testing` | no      | `io::testing`: shared run-request fixtures and port-based doubles |

The crate has no `diesel` feature and its dependency tree contains no database
driver; storage comes with the consumer.

## Construction

```rust
// The application or kernel owns the dispatcher and handler registry.
let service = CommandingService::new(kernel.command_consumer());
let service = AsyncCommandingService::from(service);
```

There is no `from_postgres` constructor: a command consumer needs the
application's handlers, which the kernel assembles in `start_persistent`.
`AsyncCommandingService` uses the caller's runtime. Dropping a request future
does not stop a command that has already started.

## Contract

- `limit` is a required integer from `1` to `1000`. Unknown fields are ignored.
- `CommandingService::run` validates the request again, then consumes up to
  `limit` due commands with the default six attempts.
- Command failures are returned in `RunCommandsResponse::errors` with their
  `CommandError` text, not as errors. Commands that succeeded stay completed.
  A failed command whose failure cannot be recorded contributes two errors,
  the storage error first.
- A blocking task failure displays `queue processing task failed`; the cause
  stays in `source()`.
- The run types are copies of `inbox_api`'s. `testing::valid_run_requests`,
  `invalid_run_requests` and `run_request_error_messages` are the shared table
  every `*_api` crate and adapter runs, so the copies cannot drift silently.
