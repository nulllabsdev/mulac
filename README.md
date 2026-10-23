# mulac

mulac is a set of architecture building blocks for reliable event-based systems (Inbox/Outbox + pluggable command and event dispatchers).

This repository contains both technical specifications and Rust crate implementations for the building blocks.

## Docs

- Fast repo TL;DR: [docs/repo-tldr.md](docs/repo-tldr.md)
- High-level component view: [docs/components.md](docs/components.md)
- Detailed spec (end-to-end flows): [docs/architecture-spec.md](docs/architecture-spec.md)
- Lifecycle states and rules: [docs/components.md](docs/components.md)
- Outbox technical specification: [docs/outbox-spec.md](docs/outbox-spec.md)

## Inbox HTTP adapters

Services expose `POST /inbox` and `POST /inbox/run` through shared crates
([ADR-009](docs/adr/009-inbox-http-adapters.md)):

- [`libs/inbox_api`](libs/inbox_api) — framework-free contract, validation and service.
- [`libs/mulac_poem`](libs/mulac_poem) — Poem/poem-openapi adapter.
- [`libs/mulac_axum`](libs/mulac_axum) — Axum adapter with optional Utoipa schemas.

## Queue HTTP adapters

`POST /commands/run`, `POST /events/run` and `POST /outbox/run` follow the
same pattern ([ADR-010](docs/adr/010-queue-http-apis.md)):
[`libs/commanding_api`](libs/commanding_api),
[`libs/eventing_api`](libs/eventing_api) and
[`libs/outbox_api`](libs/outbox_api) hold the contracts and services, and
both adapters expose them behind their opt-in `commanding`, `eventing` and
`outbox` features. The outbox answers `503` while no publisher is configured.

Run `make ci` to format-check, lint and test `libs` and `kernel`, to verify
that each adapter depends only on its own framework, and to check that each
opt-in adapter feature builds alone, stays out of the default build and pulls
in no Diesel. It also checks that `outbox_api` and the adapters never depend
on lapin.
