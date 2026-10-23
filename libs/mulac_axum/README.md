# mulac_axum

Axum adapter for [`inbox_api`](../inbox_api) and, behind the `commanding`,
`eventing` and `outbox` features, [`commanding_api`](../commanding_api),
[`eventing_api`](../eventing_api) and [`outbox_api`](../outbox_api). Applications declare the routes; the adapter's extractors and responses handle parsing, validation,
execution and error responses. See
[ADR-009](../../docs/adr/009-inbox-http-adapters.md) and
[ADR-010](../../docs/adr/010-queue-http-apis.md).

| Item               | Purpose                                                       |
|--------------------|---------------------------------------------------------------|
| `InboxAdapter`     | Cloneable router state with `submit` and `run`                |
| `SubmitInbox`      | Submission extractor; checks field types                      |
| `RunInbox`         | Batch extractor; enforces `limit` from `1` to `1000`          |
| `InboxSubmission`  | `202 Accepted` with `{"message_id":"..."}`                    |
| `InboxRunResponse` | `200 OK` with the batch result                                |
| `InboxHttpError`   | Plain-text rejection and failure responses                    |

With the `commanding` feature:

| Item                  | Purpose                                                    |
|-----------------------|------------------------------------------------------------|
| `CommandingAdapter`   | Cloneable router state with `run`                          |
| `RunCommands`         | Command batch extractor; enforces `limit` from `1` to `1000` |
| `CommandsRunResponse` | `200 OK` with the batch result                             |
| `CommandingHttpError` | Plain-text rejection and failure responses                 |

With the `eventing` feature:

| Item                | Purpose                                                      |
|---------------------|--------------------------------------------------------------|
| `EventingAdapter`   | Cloneable router state with `run`                            |
| `RunEvents`         | Event batch extractor; enforces `limit` from `1` to `1000`   |
| `EventsRunResponse` | `200 OK` with the batch result                               |
| `EventingHttpError` | Plain-text rejection and failure responses                   |

With the `outbox` feature:

| Item                | Purpose                                                      |
|---------------------|--------------------------------------------------------------|
| `OutboxAdapter`     | Cloneable router state with `run`, or `unconfigured()`       |
| `RunOutbox`         | Outbox batch extractor; enforces `limit` from `1` to `1000`  |
| `OutboxRunResponse` | `200 OK` with the batch result                               |
| `OutboxHttpError`   | Plain-text rejection and failure responses, including `503`  |

Axum's `Json` extractor answers some invalid bodies with `422`. The extractors
read a JSON value first and return `400` for invalid fields instead, while
keeping Axum's `400` syntax, `415` content-type and `413` body-limit
rejections.

## Features

| Feature      | Default | Enables                                                    |
|--------------|---------|------------------------------------------------------------|
| `diesel`     | yes     | `inbox_api/diesel` (`from_postgres`), and `outbox_api/diesel` with `outbox` |
| `utoipa`     | no      | `ToSchema` for the adapter types, using the shared names   |
| `commanding` | no      | `CommandingAdapter` and its types, with `commanding_api`   |
| `eventing`   | no      | `EventingAdapter` and its types, with `eventing_api`       |
| `outbox`     | no      | `OutboxAdapter` and its types, with `outbox_api`           |

## Usage

```rust
async fn submit_inbox(
    State(inbox): State<InboxAdapter>,
    request: SubmitInbox,
) -> Result<InboxSubmission, InboxHttpError> {
    inbox.submit(request).await
}

async fn run_inbox(
    State(inbox): State<InboxAdapter>,
    request: RunInbox,
) -> Result<InboxRunResponse, InboxHttpError> {
    inbox.run(request).await
}

let inbox_routes = Router::new()
    .route("/inbox", post(submit_inbox))
    .route("/inbox/run", post(run_inbox))
    .with_state(InboxAdapter::new(InboxService::from_postgres(pool)));

let app = Router::new().nest("/api", inbox_routes);
```

With `commanding`:

```rust
async fn run_commands(
    State(commands): State<CommandingAdapter>,
    request: RunCommands,
) -> Result<CommandsRunResponse, CommandingHttpError> {
    commands.run(request).await
}

let command_routes = Router::new()
    .route("/commands/run", post(run_commands))
    .with_state(CommandingAdapter::new(CommandingService::new(
        handle.command_consumer(),
    )));
```

With `eventing`:

```rust
async fn run_events(
    State(events): State<EventingAdapter>,
    request: RunEvents,
) -> Result<EventsRunResponse, EventingHttpError> {
    events.run(request).await
}

let event_routes = Router::new()
    .route("/events/run", post(run_events))
    .with_state(EventingAdapter::new(EventingService::new(
        handle.event_consumer(),
    )));
```

With `outbox`; the application owns the publisher:

```rust
async fn run_outbox(
    State(outbox): State<OutboxAdapter>,
    request: RunOutbox,
) -> Result<OutboxRunResponse, OutboxHttpError> {
    outbox.run(request).await
}

let outbox = match publisher {
    Some(publisher) => OutboxAdapter::new(OutboxService::from_postgres(pool, Arc::new(publisher))),
    None => OutboxAdapter::unconfigured(),
};
let outbox_routes = Router::new()
    .route("/outbox/run", post(run_outbox))
    .with_state(outbox);
```

Examples:

- [`examples/axum_inbox.rs`](examples/axum_inbox.rs) — routes only.
- [`examples/axum_openapi.rs`](examples/axum_openapi.rs) — with
  application-owned `#[utoipa::path]` annotations (`--features utoipa`).
- [`examples/axum_commanding.rs`](examples/axum_commanding.rs) and
  [`examples/axum_commanding_openapi.rs`](examples/axum_commanding_openapi.rs)
  — the command batch route, with a test-double consumer
  (`--features commanding`, plus `utoipa` for the second).
- [`examples/axum_eventing.rs`](examples/axum_eventing.rs) and
  [`examples/axum_eventing_openapi.rs`](examples/axum_eventing_openapi.rs)
  — the event batch route, with a test-double consumer
  (`--features eventing`, plus `utoipa` for the second).
- [`examples/axum_outbox.rs`](examples/axum_outbox.rs) and
  [`examples/axum_outbox_openapi.rs`](examples/axum_outbox_openapi.rs)
  — the outbox batch route, with a test-double consumer
  (`--features outbox`, plus `utoipa` for the second).

With `utoipa`, `request_body = SubmitInbox` publishes `SubmitInboxRequest`,
`body = InboxSubmission` publishes `SubmitInboxResponse`, and the batch types
of every queue (`RunInbox`, `RunCommands`, `RunEvents`, `RunOutbox`,
`InboxRunResponse`, `CommandsRunResponse`, `EventsRunResponse`,
`OutboxRunResponse`) publish `RunRequest` and
`RunResponse`.

## Responses

| Case                               | Status | Body                                  |
|------------------------------------|--------|---------------------------------------|
| Batch processed                    | `200`  | `{"processed":N\|null,"errors":[...]}` |
| Invalid limit or field type        | `400`  | Mulac or Serde message, e.g. `limit must be between 1 and 1000` |
| Malformed JSON                     | `400`  | Axum rejection                        |
| Missing or non-JSON content type   | `415`  | Axum rejection                        |
| Body over the limit                | `413`  | Axum rejection                        |
| Outbox publisher not configured    | `503`  | `outbox publisher is not configured`  |
| Batch task failure                 | `500`  | `queue processing task failed`        |
