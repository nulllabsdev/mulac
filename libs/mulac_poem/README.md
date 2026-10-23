# mulac_poem

Poem and poem-openapi adapter for [`inbox_api`](../inbox_api) and, behind
the `commanding`, `eventing` and `outbox` features,
[`commanding_api`](../commanding_api), [`eventing_api`](../eventing_api) and
[`outbox_api`](../outbox_api).
Applications declare the endpoints; the adapter parses, validates and
executes them. See [ADR-009](../../docs/adr/009-inbox-http-adapters.md) and
[ADR-010](../../docs/adr/010-queue-http-apis.md).

| Item                  | Purpose                                                      |
|-----------------------|--------------------------------------------------------------|
| `InboxAdapter`        | `submit` and `run` for endpoint methods                      |
| `InboxSubmission`     | `202 Accepted` response                                      |
| `SubmitInboxRequest`  | Request body; parsing checks field types                     |
| `RunRequest`          | Batch request; parsing enforces `limit` from `1` to `1000`   |
| `SubmitInboxResponse` | Accepted message ID                                          |
| `RunResponse`         | Batch result; `from_result` converts any queue consumer      |
| `CommandingAdapter`   | `run` for a `/commands/run` endpoint (feature `commanding`)  |
| `EventingAdapter`     | `run` for an `/events/run` endpoint (feature `eventing`)     |
| `OutboxAdapter`       | `run` for an `/outbox/run` endpoint, or `unconfigured()` (feature `outbox`) |

The wrappers publish the OpenAPI schemas `SubmitInboxRequest`,
`SubmitInboxResponse`, `RunRequest` and `RunResponse`. Every run endpoint,
including the commanding, eventing and outbox ones, uses `RunRequest` and
`RunResponse`, so the schema names do not conflict. `CommandingAdapter`,
`EventingAdapter` and `OutboxAdapter` convert them to and from their `*_api`
crate's run types and add no schema.

## Features

| Feature      | Default | Enables                                       |
|--------------|---------|-----------------------------------------------|
| `diesel`     | yes     | `inbox_api/diesel` (`from_postgres`), and `outbox_api/diesel` with `outbox` |
| `commanding` | no      | `CommandingAdapter` and `commanding_api`      |
| `eventing`   | no      | `EventingAdapter` and `eventing_api`          |
| `outbox`     | no      | `OutboxAdapter` and `outbox_api`              |

## Usage

```rust
struct InboxApi {
    adapter: InboxAdapter,
}

#[OpenApi]
impl InboxApi {
    #[oai(path = "/inbox", method = "post")]
    async fn submit(&self, request: Json<SubmitInboxRequest>) -> Result<InboxSubmission> {
        self.adapter.submit(request).await
    }

    #[oai(path = "/inbox/run", method = "post")]
    async fn run(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        self.adapter.run(request).await
    }
}

let adapter = InboxAdapter::new(InboxService::from_postgres(pool));
```

With `commanding`, a command batch endpoint delegates in the same way:

```rust
#[OpenApi]
impl CommandingApi {
    /// Execute one batch of due commands; resulting events remain queued.
    #[oai(path = "/commands/run", method = "post")]
    async fn run_commands(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        self.adapter.run(request).await
    }
}

let adapter = CommandingAdapter::new(CommandingService::new(kernel.command_consumer()));
```

With `eventing`, an event batch endpoint does the same. The application keeps
registering its subscribers with the kernel:

```rust
#[OpenApi]
impl EventingApi {
    /// Dispatch one batch of due events to the registered projectors.
    #[oai(path = "/events/run", method = "post")]
    async fn run_events(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        self.adapter.run(request).await
    }
}

let adapter = EventingAdapter::new(EventingService::new(kernel.event_consumer()));
```

With `outbox`, the outbox batch endpoint delegates too. The application owns
the publisher, and uses `OutboxAdapter::unconfigured()` while delivery is off:

```rust
#[OpenApi]
impl OutboxApi {
    /// Publish one batch; returns 503 when delivery is not configured.
    #[oai(path = "/outbox/run", method = "post")]
    async fn run_outbox(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        self.adapter.run(request).await
    }
}

let adapter = match publisher {
    Some(publisher) => OutboxAdapter::new(OutboxService::from_postgres(pool, Arc::new(publisher))),
    None => OutboxAdapter::unconfigured(),
};
```

The application owns paths, authentication, middleware, the pool, migrations
and the server. Complete services are in
[`examples/poem_inbox.rs`](examples/poem_inbox.rs) and
[`examples/poem_outbox.rs`](examples/poem_outbox.rs) (`--features outbox`).

## Responses

| Case                                 | Status | Body                                  |
|--------------------------------------|--------|---------------------------------------|
| Stored                               | `202`  | `{"message_id":"..."}`                |
| Invalid value                        | `400`  | Mulac message, e.g. `message_id must be a UUID` |
| Wrong field type or malformed JSON   | `400`  | Poem parse error                      |
| Missing or non-JSON content type     | `415`  | Poem error                            |
| Storage or task failure              | `500`  | Fixed message without diagnostics     |
| Batch processed                      | `200`  | `{"processed":N\|null,"errors":[]}`   |
| Limit outside `1` to `1000`          | `400`  | Poem parse error containing `limit must be between 1 and 1000` |
| Outbox publisher not configured      | `503`  | `outbox publisher is not configured`  |
| Batch task failure                   | `500`  | `queue processing task failed`        |
