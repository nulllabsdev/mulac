# ADR-013: Merge commanding's execution bridges

**Status:** Accepted; amends ADR-011 and ADR-012 for commanding; extended to the other queues by ADR-014

## Context

Commanding's HTTP adapters reached `CommandConsumer` through two bridges:
`AsyncCommandingService` and a synchronous `CommandingService<R>`. The synchronous
bridge only built a `ReservableCommandSpec`, called the consumer and formatted
the response. Its `Result` was never `Err`, because `RunCommandsRequest` is valid
by construction. Outside the HTTP wrappers, only tests and fixtures used it. The
application worker already calls the consumer directly.

## Decision

Remove `CommandingService<R>` without a compatibility alias.
`AsyncCommandingService` wraps `Arc<CommandConsumer<R>>` directly and erases `R`.
It builds the spec and formats the response inside its blocking task. HTTP adapters
accept the consumer through `impl Into<AsyncCommandingService>`.

Callers without Tokio or a web framework use `CommandConsumer` directly. The
consumer remains generic over its repository, so ADR-012's support for borrowed
repositories still applies to synchronous code.

The `api-testing` fixtures return `Arc<CommandConsumer<Repository>>`, so they
still do not require Tokio. Cargo feature names, HTTP behavior and wire contracts
are unchanged.

## Consequences

The Poem and Axum paths each lose one layer. External callers of
`CommandingService` must migrate, and `commanding::prelude::testing` fixtures are
renamed from `*_service` to `*_consumer`. With `api` alone, the crate exposes the
request and response contracts and errors, but no execution bridge.

This applies to commanding only. Eventing, inbox and outbox keep their synchronous
services for now, so commanding's `http/execution.rs` no longer shows their pattern.

## References

- ADR-011: API and HTTP features on core crates
- ADR-012: Unified queue repositories
- `queue-commanding-parity-implementation-plan.md`
