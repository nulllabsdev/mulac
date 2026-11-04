# mulac

mulac is a set of architecture building blocks for reliable event-based systems (Inbox/Outbox + pluggable command and event dispatchers).

This repository contains both technical specifications and Rust crate implementations for the building blocks.

## Docs

- Fast repo TL;DR: [docs/repo-tldr.md](docs/repo-tldr.md)
- High-level component view: [docs/components.md](docs/components.md)
- Detailed spec (end-to-end flows): [docs/architecture-spec.md](docs/architecture-spec.md)
- Lifecycle states and rules: [docs/components.md](docs/components.md)
- Outbox technical specification: [docs/outbox-spec.md](docs/outbox-spec.md)
- Inbox batch flows, Mermaid diagrams, and SVGs: [libs/inbox/flow.md](libs/inbox/flow.md)
- Event batch flows, Mermaid diagrams, and SVGs: [libs/eventing/flow.md](libs/eventing/flow.md)
- Outbox batch flows, Mermaid diagrams, and SVGs: [libs/outbox/flow.md](libs/outbox/flow.md)

## HTTP API features

Each core crate (`mulac-commanding`, `mulac-eventing`, `mulac-inbox`,
`mulac-outbox`) owns its API and framework adapters. Defaults enable `diesel`
and synchronous `api` services. Opt into `api-tokio`, `api-testing`, `axum`,
`axum-openapi`, or `poem` on the core dependency. For a database-free HTTP
build use `default-features = false` and the desired framework feature.

Services are exported through `prelude`; adapters through `prelude::axum` or
`prelude::poem`. The small `mulac_http` support crate preserves shared HTTP
schemas without depending on queues. The former four API crates and two
framework adapter crates are removed.

See [ADR-011](docs/adr/011-core-http-features.md) for the feature matrix,
import migration and dependency guarantees. Run `make ci` for library/kernel
tests, HTTP behavior/schema tests, and feature isolation checks.

## Optional application event revisions

`ApplicationEvent::sequence()` defaults to `None`. Applications may supply a
positive per-aggregate revision up to PostgreSQL BIGINT's maximum. The typed
command wrapper carries it through event metadata without changing framework
IDs, correlation, causation or payloads. Event reservation and subscriber
conversions preserve it. Outbox metadata has the same optional field; recorders
must copy it from the originating event. Missing legacy metadata stays missing.
This supplies provenance, not ordering or atomic command/event persistence.
