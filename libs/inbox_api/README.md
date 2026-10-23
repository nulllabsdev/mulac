# inbox_api

Framework-free inbox HTTP contract and operations shared by the
[`mulac_poem`](../mulac_poem) and [`mulac_axum`](../mulac_axum) adapters.
See [ADR-009](../../docs/adr/009-inbox-http-adapters.md).

| Item                  | Purpose                                                        |
|-----------------------|----------------------------------------------------------------|
| `SubmitInboxRequest`  | Submission contract; `from_json` parses and validates          |
| `SubmitInboxResponse` | ID of the accepted message                                     |
| `RunRequest`          | Batch request; `limit` from `1` to `1000`                      |
| `RunResponse`         | `processed` count on full success, otherwise `errors`          |
| `InboxApiError`       | Validation (`is_client_error`) and sanitized server failures   |
| `InboxService`        | Blocking `submit` and `run`                                    |
| `AsyncInboxService`   | Runs `InboxService` on Tokio's blocking pool (feature `tokio`) |

## Features

| Feature   | Default | Enables                                                     |
|-----------|---------|-------------------------------------------------------------|
| `diesel`  | yes     | `InboxService::from_postgres` and Diesel in inbox/commanding |
| `tokio`   | no      | `AsyncInboxService`                                         |
| `testing` | no      | `io::testing`: shared fixtures and port-based test doubles  |

Without `diesel` the dependency tree contains no database driver.

## Construction

```rust
// PostgreSQL, using an existing pool. Running the inbox only queues commands.
let service = InboxService::from_postgres(pool);

// Explicit components, e.g. custom storage or test doubles.
let service = InboxService::new(recorder, consumer);

let service = AsyncInboxService::from(service);
```

`AsyncInboxService` uses the caller's runtime. Dropping a request future does
not stop a blocking database operation that has already started.

## Contract

- `payload` is required. Null, blank strings, empty arrays and empty objects
  are rejected; numbers (including zero), booleans and nonempty containers are
  accepted. Nested values are not checked for emptiness.
- `routing_key` and `source` are required and must contain non-whitespace text;
  both are stored unchanged.
- `message_id` and `correlation_id` are optional UUID strings; a missing
  `message_id` becomes a UUID v7. Duplicate IDs keep the original row.
- String fields reject numbers and booleans. Unknown fields are ignored.
- Storage and task failures display fixed messages; causes stay in `source()`.
- Batch failures are returned in `RunResponse::errors`, not as errors.
