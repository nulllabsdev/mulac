# mulac repo TL;DR

This document is the **fast entrypoint** for humans and LLMs. Read this first when the goal is to review, debug, or extend the codebase without re-scanning the entire repository.

## Checklist

- [x] Read this file first for repo shape and current design decisions
- [x] Start from the **specific feature crate** you are changing (`inbox`, `commanding`, `eventing`, `outbox`)
- [x] Prefer **diff-first / file-scoped** work over whole-codebase exploration
- [x] Import public APIs through `feature::prelude::*`, not internal modules
- [x] Treat `extra_info.errors` as the stable persisted error-history contract

---

## 1. What this repo is

**TL;DR** — `mulac` is a Rust repo for reliable event-driven building blocks: **Inbox**, **Commanding**, **Eventing**, and **Outbox**.

**Detail**

- `inbox`: durable entrypoint for external messages
- `commanding`: command dispatch and durable command queue flow
- `eventing`: event dispatch and durable event queue flow
- `outbox`: durable exit point for publishing outside the system boundary

The high-level lifecycle is usually:

`inbox -> commanding -> eventing -> outbox`

Two test applications exercise the stack:

- `test_apps/todo`
- `test_apps/twitter`

---

## 2. Where to look first

**TL;DR** — Most work should start in one crate, not the whole repo.

**Detail**

Top-level areas:

- `libs/inbox`
- `libs/commanding`
- `libs/eventing`
- `libs/outbox`
- `libs/mulac_diesel` — shared Diesel pool helpers only
- `docs/` — architecture and review notes
- `test_apps/` — integration-style app coverage

For feature work, first check:

| Task                        | Start here                                                                                            |
| --------------------------- | ----------------------------------------------------------------------------------------------------- |
| Inbox processing            | `libs/inbox/src/execution/inbox_consumer.rs`                                                          |
| Command retries / storage   | `libs/commanding/src/execution/command_consumer.rs`, `libs/commanding/src/infra_diesel/repository.rs` |
| Event retries / storage     | `libs/eventing/src/execution/event_consumer.rs`, `libs/eventing/src/infra_diesel/repository.rs`       |
| Outbox retries / publishing | `libs/outbox/src/execution/outbox_consumer.rs`, `libs/outbox/src/infra_diesel/repository.rs`          |
| Lifecycle semantics         | `docs/components.md`                                                                                  |
| End-to-end architecture     | `docs/architecture-spec.md`                                                                           |

---

## 3. Module shape rules

**TL;DR** — Each feature crate exposes **one public interface**: `pub mod prelude`.

**Detail**

Repository rule from `AGENTS.md`:

- internal submodules stay private
- callers import through `feature::prelude::*`
- do not build new external usages on internal module paths

Common structure inside crates:

- `domain/` — domain types, state enums, timeout policy
- `application/` — app-facing envelopes, ports, gateways, `stale_*_sweep.rs` recovery
- `execution/` — `*_consumer.rs` reservation / processing flow, reservation spec, dispatchers
- `http/` — contracts, async execution bridge, Axum and Poem adapters
- `infra_diesel/` — Diesel rows, conversions, storage adapters

Preferred dependency direction:

`domain <- application <- execution <- HTTP and infra adapters`

---

## 4. Lifecycle rules that matter

**TL;DR** — The core safety model is reservation-based processing with at-least-once semantics.

**Detail**

Across inbox / command / event / outbox entry tables:

- states are variants of `received`, `reserved`, `failed`, `completed`, `dead`
- `reservation_id` owns a reserved row
- processing completion/failure must use the matching reservation id
- attempts increment **on reserve**
- retries reschedule work with backoff
- stale sweeps release timed-out reservations back to failed/dead state

When reviewing correctness, pay special attention to:

1. whether reservation ownership is preserved through updates
2. whether `updated == 0` / no-row cases are mapped back to reservation errors
3. whether a row can enter an invalid intermediate state between DB writes

---

## 5. `extra_info` design decisions

**TL;DR** — `extra_info` is nullable JSONB on inbox/command/event/outbox entries, and the stable contract today is `extra_info.errors: string[]`.

**Detail**

Current accepted decisions:

- `extra_info` exists on the durable entry tables for:
  - inbox
  - commanding
  - eventing
  - outbox
- failure messages are appended under `extra_info.errors`
- stale sweeps also append timeout messages under the same key
- each feature crate owns its own domain `ExtraInfo`
- each feature crate has its own Diesel `ExtraInfoJsonb`
- **duplication is intentional for now**; the shared-type consolidation idea was reviewed and rejected

Stable assumptions:

- wire shape: `{"errors": ["..."]}`
- other keys are not part of the stable cross-feature contract

Important current note:

- `outbox::dead()` should remain transactional and preserve reservation ownership semantics

---

## 6. Fast-path file map for `extra_info`

**TL;DR** — If the change touches persisted error history, these are the key files.

**Detail**

| Concern                 | Files                                                                                                                                                                                                |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Inbox `ExtraInfo`       | `libs/inbox/src/domain/mod.rs`, `libs/inbox/src/infra_diesel/repository.rs`, `libs/inbox/src/application/models.rs`, `libs/inbox/src/application/stale_reservation_sweep.rs`                         |
| Command `ExtraInfo`     | `libs/commanding/src/domain/mod.rs`, `libs/commanding/src/infra_diesel/repository.rs`, `libs/commanding/src/execution/command_consumer.rs`, `libs/commanding/src/application/stale_command_sweep.rs` |
| Event `ExtraInfo`       | `libs/eventing/src/domain/mod.rs`, `libs/eventing/src/infra_diesel/repository.rs`, `libs/eventing/src/application/models.rs`, `libs/eventing/src/application/stale_event_sweep.rs`                   |
| Outbox `ExtraInfo`      | `libs/outbox/src/domain/mod.rs`, `libs/outbox/src/infra_diesel/repository.rs`, `libs/outbox/src/application/models.rs`, `libs/outbox/src/application/stale_reservation_sweep.rs`                     |
| App migrations / schema | `test_apps/todo/migrations`, `test_apps/twitter/migrations`, `test_apps/twitter/src/schema.rs`                                                                                                       |
| Strongest storage test  | `libs/outbox/tests/diesel_storage.rs`                                                                                                                                                                |

---

## 7. How to work faster in this repo

**TL;DR** — Use scoped prompts and existing artifacts; do not re-explore everything by default.

**Detail**

Preferred workflow:

1. start from the **commit, diff, file, or crate** in question
2. read only the directly relevant files first
3. expand scope only if a dependency or invariant forces it
4. reuse existing docs in `docs/` instead of rediscovering architecture

Good prompt examples:

- “Review only `libs/outbox/src/infra_diesel/repository.rs` for reservation correctness”
- “Trace `extra_info` through inbox only”
- “Use the existing TL;DR and inspect only files touched by commit `abc123`”

Bad prompt examples:

- “Understand the whole repository”
- “Scan everything related to messaging”

---

## 8. Validation shortcuts

**TL;DR** — Use repo-standard targets first.

**Detail**

Default validation commands:

- `make fmt`
- `make check`
- `make test`

When narrowing scope, outbox integration coverage is especially useful for persistence/lifecycle behavior:

- `libs/outbox/tests/diesel_storage.rs`

---

## 9. If you only have 30 seconds

**TL;DR** — Read this list and then go straight to the owning crate.

**Detail**

- public API goes through `feature::prelude::*`
- reservation ownership is the main safety invariant
- attempts increment on reserve
- `extra_info.errors` is the stable error-history field
- per-crate `ExtraInfo` duplication is intentional for now
- use `make fmt && make check && make test`
- start from the touched crate/file, not the whole repo
