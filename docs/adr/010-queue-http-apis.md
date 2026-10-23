# ADR-010: Commanding, Eventing and Outbox HTTP APIs

**Status:** Accepted  
**Date:** 2026-10-22

## Context

[ADR-009](009-inbox-http-adapters.md) moved the inbox HTTP operations into
`inbox_api` with Poem and Axum adapters. Applications still implement the
other three batch endpoints themselves:

| Endpoint             | Runs one batch of                                  |
|----------------------|----------------------------------------------------|
| `POST /commands/run` | due commands, through the application's dispatcher |
| `POST /events/run`   | due events, through the registered subscribers     |
| `POST /outbox/run`   | due outbox messages, through a publisher           |

Each application repeats the limit validation, the `spawn_blocking` call,
result conversion and error mapping. The offers service does this in one
Poem `QueueApi`. The skeleton crates `commanding_api`, `eventing_api` and
`outbox_api` exist but contain no code.

The three endpoints share one wire contract with `/inbox/run`: a
`{"limit":N}` request from `1` to `1000` and a
`{"processed":N|null,"errors":[...]}` response. poem-openapi and Utoipa key
component schemas by name, and offers publishes exactly one `RunRequest` and
one `RunResponse` schema for all four routes.

## Decision

Extract the three endpoints with the ADR-009 pattern, one at a time, in this
order: commanding, eventing, outbox. The outbox goes last because it is the
only one with a third (`503`) outcome and a service-owned publisher.

### Crates and adapters

| Crate             | Responsibility                                                                 |
|-------------------|--------------------------------------------------------------------------------|
| `commanding_api`  | `RunCommandsRequest`/`RunCommandsResponse`, `CommandingService`, errors         |
| `eventing_api`    | `RunEventsRequest`/`RunEventsResponse`, `EventingService`, errors               |
| `outbox_api`      | `RunOutboxRequest`/`RunOutboxResponse`, `OutboxService`, errors, unconfigured state |
| `mulac_poem`      | Private `commanding`, `eventing` and `outbox` modules with a `*Adapter` each    |
| `mulac_axum`      | The same modules with an adapter, extractor, response and error type each       |

- Each `*_api` crate is framework-free and depends on its own queue crate with
  `default-features = false`. It does not depend on the kernel, on `inbox_api`
  or on the other `*_api` crates.
- Each `*_api` crate has an optional `tokio` feature with an
  `Async*Service` that owns the `spawn_blocking` call and maps a join failure
  to `RunTaskFailed` (`queue processing task failed`, cause kept in
  `source()`). It uses the caller's runtime and creates no runtime, pool,
  worker, dispatcher or connection.
- Each `*_api` crate has a `testing` feature with shared fixtures and doubles
  built on the queue crate's existing ports.
- Services validate the request again, so direct callers get the same check
  as HTTP callers. Consumer failures are data in a `200` response, not errors.
- Reservation, retries, backoff, the six-attempt limit and dead-lettering stay
  in the queue crates. The HTTP contract exposes none of them.
- `commanding_api` and `eventing_api` wrap a consumer that the application or
  the kernel assembled (`PersistentKernelHandle::command_consumer()` and
  `event_consumer()`), because the dispatchers hold application handlers and
  subscribers. They have no `from_postgres` constructor and no `diesel`
  feature. `outbox_api` keeps a `from_postgres(pool, publisher)` constructor
  behind `diesel`, because nothing else assembles an outbox consumer.

### Per-crate run types and one schema owner per framework

Each `*_api` crate defines its own run types, copied from `inbox_api`'s
`RunRequest`/`RunResponse`, with the same bounds (`MIN_LIMIT = 1`,
`MAX_LIMIT = 1000`), the same `limit must be between 1 and 1000` message and
the same `from_result` mapping. `inbox_api` is unchanged.

The per-crate types are never published as schemas:

- **Poem:** `mulac_poem::io::{RunRequest, RunResponse}` stay the only schema
  owners, outside every feature. Each module converts them into and out of its
  own run types with `From` implementations, which the orphan rule allows
  because the wrappers are local to `mulac_poem`. Applications declare every
  run endpoint with `Json<RunRequest>` and `Json<RunResponse>`.
- **Axum:** the Utoipa twins `RunRequest` and `RunResponse` stay the only
  schema owners. Each module's extractor and response publish them through
  `schema_as!`. The JSON-body helper and the `422`-to-`400` normalization live
  in one private module that every extractor uses.

The published OpenAPI document therefore does not change when an application
moves a run endpoint to an adapter.

Duplicating the bounds, the message and `from_result` across four crates is
the accepted price for independent crates without a breaking change. Each
`*_api` crate's `testing` module exposes the same table,
`valid_run_requests()`, `invalid_run_requests()` and
`run_request_error_messages()`, and each crate and adapter runs it, so drift
in behavior or text fails a test.

### Opt-in adapter features

Both adapters gain non-default `commanding`, `eventing` and `outbox` features.
Feature `<name>` enables the private `<name>` module and the optional
`<name>_api` dependency, and implies nothing else; in particular it does not
imply `diesel`. The default adapter build is unchanged, so inbox-only
applications compile none of the new code.

CI checks for each feature that both adapters build with only that feature
(and `mulac_axum` also with `utoipa`), that the default adapter builds do not
contain the `*_api` crate, and that neither the `*_api` crate nor the
feature-only adapter build contains Diesel.

### HTTP contract

| Case                                   | Response                                          |
|----------------------------------------|---------------------------------------------------|
| `limit`                                | Required integer from `1` to `1000`; unknown fields ignored |
| Invalid limit or malformed JSON        | `400`                                             |
| Missing or non-JSON content type       | `415`                                             |
| Batch result                           | `200` `{"processed":N\|null,"errors":[...]}`      |
| Blocking task failure                  | `500` `queue processing task failed`              |
| Outbox without a configured publisher  | `503` `outbox publisher is not configured`        |

Batch responses keep reporting the queue crates' error messages, as today.
Sanitizing them would be a separate behavior change.

Commanding and eventing errors are either client errors (`400`) or server
errors (`500`), classified by `is_client_error()` as in `inbox_api`.

### Outbox: unconfigured state and publisher ownership

- `outbox_api` models a missing publisher as a Mulac state: an unconfigured
  service validates the request and then returns `PublisherNotConfigured`,
  displayed as `outbox publisher is not configured`, which the adapters map
  to `503`. It never touches storage, and invalid bodies still return `400`
  first. Because the outbox has three outcomes, its error type classifies
  itself as invalid, unavailable or internal instead of using a boolean.
- The publisher stays owned by the service. `outbox_api` and the adapters
  accept any `Arc<dyn OutboxPublisherPort>` and neither enable nor forward
  `mulac-outbox/amqp`. Broker configuration, environment parsing and topology
  remain application concerns.
- In a later stage, after the outbox extraction, `mulac-outbox` gains a strict
  publisher, `ConfirmedAmqpPublisher`, behind the existing `amqp` feature. It
  ports the semantics of offers' `RabbitMqPublisher`: mandatory routing,
  required publisher confirms, persistent delivery, timeouts, a reused
  connection that is dropped after failures, no runtime of its own, and fixed
  error messages without broker diagnostics. The existing `AmqpPublisher` is
  unchanged.

## Consequences

**Benefits:**
- Applications declare each run endpoint and delegate; the contract is
  implemented and tested once per framework.
- Published OpenAPI documents stay unchanged, with one `RunRequest` and one
  `RunResponse` schema per document.
- Inbox-only applications pay nothing for the new modules.

**Trade-offs:**
- Three more crates to version and publish, in dependency order after their
  queue crates and before the adapters.
- Run types are duplicated across four crates; shared fixtures, not the type
  system, keep them aligned.
- Poem validates the limit while parsing, so Poem returns Mulac's limit
  message inside its parse-error text, while Axum returns it verbatim.
  Statuses and Mulac's messages match.
- Dropping an HTTP request does not stop a batch that has already started.

## References

- [ADR-006](006-cargo-feature-flags.md) — feature flags for infrastructure.
- [ADR-008](008-arc-dyn-trait-dependencies.md) — shared port dependencies.
- [ADR-009](009-inbox-http-adapters.md) — the inbox pattern this ADR extends.
