# ADR-014: Merge the other queues' execution bridges

**Status:** Accepted; extends ADR-013 to eventing, inbox and outbox; amends ADR-011 and ADR-012; error text changed by ADR-015

## Context

ADR-013 removed commanding's synchronous service and left the other queues as they
were. Commanding has since moved its consumer, dispatcher and reservation spec into
a private `execution` module, grouped its HTTP code under `http`, made its run request
valid by construction, and removed its forwarding recorder. The four crates therefore
have two different shapes, and the differences carry no behaviour.

## Decision

Eventing, outbox and inbox adopt commanding's shape, each in its own change and in
that order. HTTP paths, JSON bodies, status codes, error text, OpenAPI schema names,
database schemas and Cargo feature names do not change. Removed items get no
compatibility aliases.

- **Layout.** `api/`, `http_axum/` and `http_poem/` become `http/`, with the bridge in
  `http/execution.rs`. Consumers, dispatchers and reservation specs move to a private
  `execution` module. Ports move to `application/ports.rs`. Public imports still go
  through each crate's `prelude`.
- **Run requests.** `RunEventsRequest`, `RunOutboxRequest` and inbox's `RunRequest`
  get a private `limit`, `new`, `limit()` and `serde(try_from)`, and lose public
  `validate`. An invalid request can no longer be built, including by deserializing.
- **Services.** Remove `EventingService`, `OutboxService` and `InboxService`.
  `AsyncEventingService` and `AsyncOutboxService` wrap `Arc<…Consumer<R>>` directly and
  convert from it, so adapters keep taking `impl Into<Async…Service>`.
- **Inbox.** `submit` can really fail, so ADR-013's "never `Err`" reasoning only applies
  to `run`. `InboxService` is removed anyway: only tests, fixtures and examples use it,
  and `submit` is four lines. `AsyncInboxService::new` takes the store port and the
  consumer, and also converts from that pair.
- **Outbox.** `AsyncOutboxService::unconfigured()` and its `503` stay. Invalid limits are
  still rejected first, because adapters build the request before calling the bridge.
- **PostgreSQL assembly.** `from_postgres` moves to `AsyncInboxService` and
  `AsyncOutboxService`, so it now also requires `api-tokio`.
- **Recorders.** Remove `EventRecorder` and `InboxRecorder`, which only forwarded to
  their ports. `EventGateway::two_phased` takes `Arc<dyn EventStorePort>`, and
  `AppState` and `AmqpWorker` hold `Arc<dyn InboxStorePort>`. `OutboxRecorder` stays,
  because it validates routing keys and sequences and builds `NewOutboxEntry`.
- **Fixtures.** `testing::*_service` becomes `*_consumer`, returning
  `Arc<…Consumer<Repository>>`, so `api-testing` still does not require Tokio. Inbox
  fixtures become `*_parts`, returning the store and consumer pair.

Some differences stay on purpose. Names such as `process`, `publish_batch`,
`InboxStorePort::store` and inbox's `RunRequest` are unchanged. `Criterion`, `criteria()`
and `ReservationTimeoutPolicy` remain crate-private in these crates, although commanding
exports them. Inbox keeps its two-phase `SubmitInboxRequest`, so validation messages
reach clients without framework wording.

## Migration

Callers with a web framework pass the consumer to the routes or adapter, for example
`event_routes(kernel.event_consumer())`.

Callers without Tokio use the consumer directly. To keep the HTTP service's limit
validation and response shape, use the contract types. `consume(...)?` would instead
propagate batch errors, and an arbitrary spec would skip the limit check.

```rust
// Commanding
let request = RunCommandsRequest::new(limit)?;
let result = consumer.consume(&ReservableCommandSpec::new(request.limit()));
let response = RunCommandsResponse::from_result(result);

// Eventing
let request = RunEventsRequest::new(limit)?;
let result = consumer.consume(&ReservableEventSpec::new(request.limit()));
let response = RunEventsResponse::from_result(result);

// Outbox: publishers may block on the runtime, so don't call this from async code.
let request = RunOutboxRequest::new(limit)?;
let result = consumer.publish_batch(&ReservableOutboxSpec::new(request.limit()));
let response = RunOutboxResponse::from_result(result);

// Inbox
let request = RunRequest::new(limit)?;
let result = consumer.process(&ReservableInboxSpec::new(request.limit()));
let response = RunResponse::from_result(result);
```

Inbox submission without the bridge converts and stores directly:

```rust
let envelope = NewInboxMessageEnvelope::try_from(request)?;
let message_id = envelope.id().to_string();
store.store(envelope)?;
```

## Consequences

Every queue's HTTP path has one bridge between the adapter and the consumer. External
callers must migrate, including `offers` if it uses these types. That code is not in
this repository, so check it before publishing.

This changes what earlier ADRs say. In ADR-011's feature table, `api` now provides only
contracts and API errors for all four crates, and `api-tokio` adds the bridge. ADR-011's
statements and import example about synchronous services no longer apply. In ADR-012,
"synchronous HTTP execution bridges" no longer exist; its guarantee about borrowed
repositories now applies to the consumers alone, and its rule about recorders now
applies to `OutboxRecorder` alone.

## References

- ADR-011: API and HTTP features on core crates
- ADR-012: Unified queue repositories
- ADR-013: Merge commanding's execution bridges
- `review_command_flows_codex.md`
