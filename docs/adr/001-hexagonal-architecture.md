# ADR-001: Hexagonal Architecture (Ports and Adapters)

**Status:** Accepted  
**Date:** 2026-05-12

## Context

Each crate in this repository implements a component from the mulac architecture (Inbox, Command Dispatcher, Event Dispatcher, Outbox). Each component needs to interact with external infrastructure (PostgreSQL, AMQP brokers) while keeping its core logic testable and infrastructure-independent. Without an explicit structure, domain logic, application orchestration, and database code tend to collapse into a single layer that is hard to test, hard to extend, and impossible to reason about in isolation.

## Decision

All crates follow a four-layer hexagonal architecture:

```
domain  ←  application  ←  features  ←  infra adapters
```

Dependency arrows point inward only.

| Layer              | Contents                                              | Allowed dependencies           |
| ------------------ | ----------------------------------------------------- | ------------------------------ |
| **domain**         | Core models, status enums, value objects              | No external crates, no I/O     |
| **application**    | Port traits, application-layer envelopes, error types | Domain layer only              |
| **features**       | Use-case orchestrators, repositories                  | Application layer (ports) only |
| **infra adapters** | Concrete storage and transport implementations        | Application + feature layers   |

Layers live as top-level modules under each crate's `src/`: `domain`, `application`, `execution` (use-case orchestrators), and adapters such as `infra_diesel` and `http`.

Public API is exposed exclusively through a root `lib.rs` re-export module (`prelude`). External callers never import from internal layers directly.

## Consequences

**Benefits:**
- Domain and application layers are testable without any database or broker — ports can be replaced with test doubles.
- Infrastructure can be swapped (e.g. switching from Diesel to SQLx) without touching feature modules.
- Dependency direction is enforced at compile time: the domain cannot depend on infrastructure.
- Clear boundary for what is a "core concept" vs. an "implementation detail."

**Trade-offs:**
- More files and modules than a flat structure.
- New features require defining a port before writing the implementation — adds a step but makes the contract explicit.
- Test doubles must be maintained alongside port changes.

## References

- [developer-guidelines.md](../developer-guidelines.md) — import rules and module organization conventions
- inbox crate implementation as the reference for this pattern
