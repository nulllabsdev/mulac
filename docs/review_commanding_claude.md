# Review: `libs/commanding`

Reviewed by: Claude Opus 5.5
Reviewed against: `docs/command-handling-spec.md`, ADR-008, CLAUDE.md module rules, and the kernel wiring in `kernel/src/lib.rs`.
Perspectives: reliability and delivery semantics, domain and API design, layering and maintainability.

Baseline: `cargo test -p mulac-commanding --features axum-openapi,poem,api-testing` passes (30 tests). Clippy reports 2 warnings. No test exercises `DieselPgCommandRepository` against Postgres.

---

## High: correctness and reliability bugs

- [ ] **The stale sweep never dead-letters exhausted commands, so they stay `Failed` forever**
  - Where: `src/infra_diesel/repository.rs:250-267`, `SweepCriteria` at `:283-304`. `StaleCommandSpec::max_attempts` (`src/application/stale_command_sweep.rs:13`) is never read.
  - Scenario: `reserve` increments `attempts`. A command whose handler crashes the process or hangs on its 6th attempt gets swept to `Failed` with `attempts = 6`. `reserve` then filters `attempts < 6`, so the row is never picked up again and never becomes `Dead`. Alerting on `Dead` will miss it.
  - Fix: in the sweep SQL, set `status = CASE WHEN attempts >= $max THEN Dead ELSE Failed END`, and bind `spec.max_attempts`.

- [ ] **One unreadable row fails the whole reserved batch and leaves every row in it `Reserved`**
  - Where: `src/infra_diesel/repository.rs:94-103`
  - Scenario: the `UPDATE … RETURNING` autocommits, then `collect()` over `CommandEnvelope::try_from` stops at the first row whose `meta` does not deserialize (after a schema change, a manual insert or another producer). Up to `limit` healthy commands stay `Reserved` with an attempt used up. No sweeper runs (next item), so they are stuck for good. If a sweeper does run, the bad row poisons every later batch it joins.
  - Fix: convert each row separately. Mark rows that fail conversion `Dead` (or `Failed` with a reason) and return the rest. Alternatively, run the reservation and the conversion in one transaction.

- [ ] **Nothing ever recovers stale reservations: `CommandSweeper` is not wired up**
  - Where: `kernel/src/lib.rs` (`start_persistent`) builds a consumer but no sweeper, and `kernel/src/workers.rs` has no sweep loop. Nothing outside the crate uses `CommandSweeper`.
  - Scenario: if the process crashes or is redeployed mid-batch, or `completed()`/`failed()` hits a transient DB error, rows stay `Reserved` permanently.
  - Fix: expose `CommandSweeper` from `PersistentKernelHandle` and add a `run_command_sweep_worker` with a configurable timeout. Document in the spec that it is required.

- [ ] **Retries emit duplicate events with new IDs, so event-store dedup cannot catch them**
  - Where: `src/application/command_dispatcher.rs:76` (`event_id: Uuid::now_v7()`) and `:122-124` (one event per `dispatch` call, each on its own connection and transaction)
  - Scenario: say event 1 is recorded, event 2 fails, and the command is retried. Or all events are recorded but `completed()` fails, and the row is swept and re-run. Either way the events are recorded again with new `event_id`s. `on_conflict_do_nothing` on `event_entries.id` only deduplicates when the ID is the same, so the duplicates are stored.
  - Fix: derive event IDs deterministically, for example UUIDv5/v8 from `(command_id, event_index)`. Add a batch `record_all(&[NewEventEnvelope])` port that writes all events in one transaction.

- [ ] **The spec requires handlers to deduplicate, but they never receive the command ID**
  - Where: `CommandHandlerPort::execute(&self, command: C)` at `src/application/command_dispatcher.rs:27-29`; spec line 12 ("handlers must tolerate duplicate invocations or implement their own deduplication")
  - Scenario: handlers see only the payload. They have no `command_id`, `correlation_id` or attempt number to key deduplication on.
  - Fix: pass a `CommandContext { command_id, correlation_id, causation_id, attempt }` alongside `C`.

- [ ] **A reservation can expire mid-batch and the command then runs twice (no fencing)**
  - Where: `src/application/command_consumer.rs:126-128`, `:147`
  - Scenario: all `limit` rows share one `reserved_at`, but they are processed one after another. If the batch outlasts the sweep timeout, later rows are swept and re-reserved by another worker while this worker still dispatches them. The `reservation_id` check happens only at `completed()`, after the side effects have run. This is latent today because no sweeper runs, and becomes real as soon as one is wired.
  - Fix (any of these): re-check or extend the lease before each dispatch, reserve one row at a time, or make the timeout configurable relative to `limit × expected handler time`.

- [ ] **Scheduling and leases mix the app clock and the DB clock**
  - Where: DB `now()` at `repository.rs:64,73`. App `Utc::now()` at `repository.rs:182` (retry `scheduled_at`), `:248` (sweep `now`), `infra_diesel/conversions.rs:21` (insert `scheduled_at`) and `stale_command_sweep.rs:32` (sweep cutoff).
  - Scenario: the cutoff comes from the app clock but is compared against a `reserved_at` written by the DB clock. An app server whose clock runs ahead by `S` releases live reservations `S` seconds early, causing duplicate execution. Retry delays shift in the same way.
  - Fix: use the DB clock everywhere (`now()`, `now() - $timeout::interval`, `now() + $delay::interval`). Pass durations, not timestamps.

- [ ] **The HTTP `/run` response exposes internal error text**
  - Where: `src/api/contract.rs:58-68` copies `CommandError::to_string()` into `errors`; `Storage(e.to_string())` holds raw Diesel, r2d2 or Postgres messages
  - Scenario: clients see table and constraint names, SQL errors and handler internals. This contradicts the "Display text is safe to return to HTTP clients" note in `api/error.rs:16`.
  - Fix: return `{ command_id, code }` entries with stable codes (`handler_failed`, `storage_unavailable`, …) and log the full text on the server.

## Medium: design and robustness

- [ ] **Permanent failures are retried as if they were transient**
  - Where: payload deserialization at `command_dispatcher.rs:51-52` (`Conversion`) and `HandlerNotFound` both go through `failed()` (`command_consumer.rs:153-157`)
  - Scenario: a malformed payload or an unregistered type is retried 6 times over roughly 10 minutes before reaching `Dead`.
  - Fix: add `CommandError::is_retryable()`, or let handlers return a `Permanent`/`Transient` distinction, and add a `CommandProcessPort::dead(...)` path.

- [ ] **Retry policy lives in the Diesel adapter and is duplicated or uncapped elsewhere**
  - Where: backoff constants and the `Dead` decision at `repository.rs:21-22,172-182`; two `DEFAULT_MAX_ATTEMPTS = 6` (`command_consumer.rs:19`, `stale_command_sweep.rs:17`); the HTTP path always uses the default (`api/service.rs:46`); sweep backoff `attempts * 30s` has no 120 s cap (`repository.rs:256`, spec line 127)
  - Risk: every new backend must reimplement business rules. A worker and the HTTP endpoint can disagree on `max_attempts`.
  - Fix: add one `RetryPolicy { max_attempts, base, cap, jitter }` in `application`, injected into the consumer and the sweeper. Ports receive the computed outcome (`next_status`, `delay`).

- [ ] **`record()` silently accepts a conflicting duplicate `command_id`**
  - Where: `repository.rs:41` (`on_conflict_do_nothing`)
  - Scenario: the same ID arrives with a different `command_type` or payload and still returns `Ok(())`, so the caller believes it was accepted.
  - Fix: return `RecordOutcome::{Inserted, Duplicate}`, or compare a payload hash and return `CommandError::Conflict`.

- [ ] **The batch result drops partial success and command IDs**
  - Where: `command_consumer.rs:117-135` returns `Result<usize, Vec<CommandError>>`; `RunCommandsResponse.processed` is `None` if any command fails (`contract.rs:51-52`)
  - Scenario: with 999 successes and 1 failure, the response reports `processed: null`, and the error does not say which command failed.
  - Fix: return `BatchReport { reserved, completed, failed, errors: Vec<(Uuid, CommandError)> }`.

- [ ] **`/run` has no concurrency limit**
  - Where: `src/api/runtime.rs:47` (`spawn_blocking` per request)
  - Risk: parallel `limit=1000` calls can exhaust Tokio's blocking pool and the r2d2 pool, starving the kernel workers. Dropped requests keep running.
  - Fix: guard the call with a semaphore or single-flight, and return `429`/`409` when busy.

- [ ] **`metadata` is `Option` but required, and the error uses the wrong variant**
  - Where: `application/mod.rs:51,79`; checked twice (`command_gateway.rs:26-30`, `infra_diesel/conversions.rs:17-19`); `CommandError::Conversion` is used for a validation failure
  - Fix: make `metadata: NewCommandMetadata` non-optional, as kernel's typed `NewCommandEnvelope<C>` already does. That removes both runtime checks. Drop `CommandEnvelope.metadata.command_id` too, since it duplicates `command.id`.

- [x] **The application layer depends on the API layer**
  - Where: `src/application/commanding_service.rs:5-10` imported `RunCommandsRequest`/`Response`/`CommandingApiError` from `crate::api`
  - Fixed: `CommandingService` moved to `src/api/service.rs`, matching eventing, inbox and outbox. The public path `commanding::prelude::CommandingService` is unchanged.

- [ ] **No integration tests for `DieselPgCommandRepository`**
  - Every bug above sits in SQL paths that only the pure unit tests touch. `libs/outbox/tests/diesel_storage.rs` is the model to copy.
  - Cover: `SKIP LOCKED` with concurrent reservers, the `Failed` to `Dead` transition at `max_attempts`, sweep plus exhaustion, the poison row, a lost reservation on `completed()`, and duplicate `record()`.

## Low: simplification, consistency and hygiene

- [ ] **The `Criterion` abstraction does no work.** The SQL is hard-coded, so `ScheduledBeforeNow`, `OrderByScheduledAtAsc` and the sweep's `max_attempts` are ignored, and `ReservationCriteria` overwrites the `max_attempts` it was just given (`repository.rs:204-232,283-304`). Either read the spec fields directly and delete `Criterion`, or make the adapters actually translate it. `StaleCommandSpec::criteria()` also calls `Utc::now()`, which makes it impure; pass `now` in or move the timestamp to SQL.
- [ ] **ADR-008 deviation.** `CommandConsumer<R>` and `CommandingService<R>` are generic, while `CommandRecorder` and `CommandSweeper` use `Arc<dyn Port>`. Switching to `Arc<dyn CommandReservePort>` + `Arc<dyn CommandProcessPort>` removes the type parameter and the erasure closure in `AsyncCommandingService`.
- [ ] **`failed()` makes two round trips and a read-modify-write of `extra_info`** (`repository.rs:149-198`). Do it in one `UPDATE … SET status = CASE …, extra_info = jsonb_set(…)`, as the sweep already does.
- [ ] **Double-wrapped error text.** `record()` re-wraps a `CommandError::Conversion` as `Conversion(e.to_string())`, giving "conversion error: conversion error: …" (`repository.rs:36-37`). It also takes a pool connection before validating; validate first.
- [ ] **Inconsistent pool-error mapping.** `reserve` maps pool failures to `Reservation`, while every other method uses `Storage` (`repository.rs:54`).
- [ ] **The public surface leaks internals.** The crate prelude exports `CommandEntry` and `NewCommandEntry` (`lib.rs:25-30`), which nothing outside the crate uses, plus `Criterion` and all four ports. Trim the prelude to what kernel and the apps actually import.
- [ ] **Inner layers import through the crate façade.** `application/ports.rs:14` and `infra_diesel/repository.rs:9` use `crate::prelude`, which also re-exports `api` and `http`. Use `crate::application::prelude` inside the crate.
- [ ] **The queue machinery is copied four times.** Reserve, fail and sweep SQL, the specs and the `MetadataJsonb`/`ExtraInfoJsonb` codecs are near-identical in commanding, eventing, inbox and outbox, so the sweep bug above also exists in `eventing/src/assembly/infra_diesel.rs:476-494`. Extract a shared table-parameterised queue into `mulac_diesel`. The hand-written Jsonb codecs are unnecessary: Diesel's enabled `serde_json` feature already implements `ToSql`/`FromSql<Jsonb, Pg>` for `serde_json::Value`.
- [ ] **Clippy:** `single_match` at `repository.rs:294`, `unnecessary_lazy_evaluations` at `infra_diesel/conversions.rs:47`.
- [ ] **Docs drift.** ~~`commanding_service.rs:23` names feature `tokio`~~ (fixed in `api/service.rs`). The spec describes async ports, a `failed()` without `reason`, and an `EventDispatch` variant (the code has `Eventing`). It also says the sweep "does not increment attempts", but `reserve` already does, so state clearly when an attempt is counted.
- [ ] **Schema and retention.** The `(status, reserved_at)` index the spec recommends is missing from the test-app migrations. Consider a partial index `WHERE status IN (0, 4)` for reservation. `Completed` rows are never archived (`CommandStatus::Archive` is unused), so the table and its indexes grow without bound.
- [ ] **No ordering or concurrency key.** Two commands for the same aggregate can run in parallel on different workers. Consider an optional `ordering_key` with one in-flight command per key.
- [ ] **Observability.** There are no `tracing` spans or metrics in the library, and errors do not carry `command_id`, `command_type` or attempt. Add spans around reserve, dispatch, complete and fail, plus counters for completed, failed, dead and swept.
- [ ] **Nits:**
  - Events always get `source: None` (`command_dispatcher.rs:79`).
  - Reservation `ORDER BY scheduled_at` has no tie-break; add `id` (UUIDv7) for FIFO order.
  - `RunTaskFailed` reads "queue processing task failed" (`api/error.rs:29`).
  - `extra_info.errors` stores unbounded handler strings; truncate them.
  - `ReservableCommandSpec` and `StaleCommandSpec` have public, unvalidated fields: `limit = 0` and `max_attempts <= 0` silently reserve nothing.

---

## Suggested order

1. Fix sweep dead-lettering, per-row conversion and sweeper wiring together with Diesel integration tests (these three are directly testable).
2. Make event IDs deterministic, add batch event recording and pass `CommandContext` to handlers (this is the duplicate-delivery story).
3. Move to the DB clock only, add lease fencing and a single `RetryPolicy`.
4. Make the API-shape changes: `BatchReport`, sanitized errors, required metadata, the concurrency guard.
5. Do the cleanups, and extract the shared queue machinery into `mulac_diesel`.
