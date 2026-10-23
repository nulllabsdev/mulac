# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Kernel startup no longer installs successful no-op inbox stores or event
  subscribers. Direct startup now requires an explicit `inbox_store`; its
  recording errors propagate to callers.
- Both startup modes reject the unimplemented `outbox_subscriber` shortcut.
  Register a recorder-backed subscriber through `event_subscriber` instead.
- Direct startup rejects gateway-dependent subscriber factories rather than
  dropping them. These factories remain supported by `start_persistent`.

### Changed

- **Startup compatibility:** applications using `start()` must supply an inbox
  store explicitly. `KernelConfig::database_url` does not provision one.
  The direct-only inbox store option is rejected by `start_persistent`.
  See [kernel startup contracts](kernel/SPEC.md) for supported configurations.

## [0.1.0] - 2026-05-24

Initial release. mulac is a set of architecture building blocks for reliable
event-based systems, providing Inbox/Outbox patterns alongside pluggable
command and event dispatchers.

### Added

- **Workspace layout**: unified Cargo workspace under `libs/` with shared
  dependencies, plus a top-level `kernel` crate and `test_apps/` integration
  examples.
- **`inbox` library**: core domain, assembly, feature modules, and AMQP
  transport for ingesting external messages reliably.
- **`outbox` library**: outbox flows, adapters, status model, and SQL schema
  for transactional message dispatch.
- **`commanding` library**: command gateway, handler registry, and status
  models implementing the commanding module along hexagonal-architecture
  boundaries.
- **`eventing` library**: event dispatcher and subscriber registry following
  the hexagonal-architecture layout.
- **`mulac_diesel` library**: shared Diesel-based persistence helpers.
- **`kernel` crate**: top-level composition crate exposing registries,
  workers, and helpers through a single `io` module.
- **Test applications**: `todo` and `twitter` example apps demonstrating
  end-to-end integration, with shared HTTP helpers and assertion macros.
- **Docker infrastructure**: consolidated local development setup.
- **CI**: GitHub Actions workflow running `make test` with all features.
- **Makefile**: `cargo make` targets including `fmt`, `test`, and
  `test-apps` workflows.

### Documentation

- Architecture and component specs (`docs/architecture-spec.md`,
  `docs/components.md`).
- Per-module specs and implementation plans for inbox, outbox, commanding,
  and eventing.
- Developer guidelines, codestyle guide, ADRs, and contracts reference.
- Reviews of the `todo` and `twitter` test apps.
- LLM tooling and attribution conventions (see `CLAUDE.md`, `AGENTS.md`).

### Conventions

- Every feature module exposes a single `pub mod io` as its only public
  interface; internal sub-modules (`commanding`, `eventing`, `http`,
  `infra_sqlx_pg`, ...) remain private.
- Commits follow the [Conventional Commits](https://www.conventionalcommits.org/)
  specification.

[0.1.0]: https://github.com/your-org/mulac/releases/tag/v0.1.0
