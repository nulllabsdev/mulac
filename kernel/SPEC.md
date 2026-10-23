# Kernel Specification

## Overview

The kernel assembles command and event gateways, registries, and consumers.
Startup must not accept work through placeholder implementations: an unsupported
configuration returns a `KernelError` instead of installing a successful no-op.

## Startup contracts

### Direct mode: `KernelBuilder::start`

- Requires an explicit `Arc<dyn InboxStorePort>` supplied through `inbox_store`.
  Missing storage returns `KernelError::InboxStoreRequired`, even if
  `KernelConfig::database_url` is set.
- The exposed inbox recorder delegates to that store and propagates its errors.
  Production stores must acknowledge recording only after durable acceptance.
- Commands execute inline and events are delivered to explicitly registered
  `event_subscriber` implementations.
- `event_subscriber_with_command_gateway` returns
  `KernelError::CommandGatewaySubscriberRequiresPersistence` at startup. Direct
  gateway assembly currently depends on the subscriber registry and does not
  support these factories.

### Persistent mode: `KernelBuilder::start_persistent`

- Assembles durable command and event processing from the supplied Diesel pool.
- Invokes `event_subscriber_with_command_gateway` factories with the same
  two-phased command gateway exposed by the returned state.
- Does not assemble inbox ingestion. Supplying the direct-mode `inbox_store`
  option returns `KernelError::InboxStoreUnsupportedInPersistentMode` rather
  than ignoring it. Applications configure their inbox recorder separately.

### Outbox registration

The `outbox_subscriber(event_type)` shortcut has no implemented storage/routing
adapter. Both startup modes reject it with
`KernelError::OutboxSubscriberUnsupported` before invoking subscriber factories.

Use `event_subscriber` with an application-owned subscriber that converts the
event to an outbox envelope with an explicit routing policy and records it
durably. Recording failures must propagate from the subscriber so event
processing cannot report success before outbox acceptance.

### Configuration and runtime ownership

`KernelConfig` alone does not provision database or AMQP infrastructure.
Applications own pools, migrations, transports, worker startup, and shutdown
coordination. These startup checks do not add automatic publication or workers.

## Examples

Direct startup using the existing Diesel inbox adapter (inside an async
application startup function):

```rust
use mulac_kernel::io::{InboxStoreStorage, KernelConfig, boot, build_pool};
use std::sync::Arc;

let pool = build_pool(database_url)?;
let kernel = boot(KernelConfig::default())
    .inbox_store(Arc::new(InboxStoreStorage::new(pool)))
    .start()
    .await?;
```

The application must apply the inbox schema before recording messages. Startup
injects the adapter; it does not perform migrations or validate its schema.

## Regression tests

`cargo test --manifest-path kernel/Cargo.toml --all-features` exercises startup
rejection, inbox recording and error propagation, direct dispatch, and
persistent factory wiring. Startup-only persistent tests use an empty lazy
pool, so they do not require PostgreSQL or validate database operations.
