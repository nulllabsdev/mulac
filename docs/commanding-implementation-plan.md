# Commanding Implementation Plan

## Context

The `write_side` crate has a scaffolded `commanding/` module with status models and empty stubs. This plan implements it fully, following the inbox crate as the reference pattern. The spec is at `docs/command-handling-spec.md`. Scope is commanding only; eventing remains stubbed (a minimal `EventEnvelope` placeholder is defined to satisfy `CommandHandlerPort`).

---

## Checklist

- [x] Task 1 — Cargo.toml and module scaffold
- [x] Task 2 — Domain layer (`commanding/domain/mod.rs`)
- [x] Task 3 — Application layer (`commanding/application/mod.rs`)
- [x] Task 4 — Record commands (`commanding/application/command_recorder.rs`)
- [x] Task 5 — Command consumer (`commanding/application/command_consumer.rs`)
- [x] Task 6 — Stale command sweep (`commanding/application/stale_command_sweep.rs`)
- [x] Task 7 — Dispatcher (`commanding/application/command_dispatcher.rs`)
- [x] Task 8 — Gateway (`commanding/application/command_gateway.rs`)
- [x] Task 9 — Diesel infra (`commanding/assembly/infra_diesel.rs`)
- [x] Task 10 — Public API facade (`lib.rs`)
- [x] Verification

---

## Critical Files

- `write_side/src/lib.rs` — needs `pub mod prelude` facade
- `write_side/src/commanding/mod.rs` — needs restructuring
- `write_side/src/commanding/model.rs` — `CommandStatus` moves to domain layer
- `write_side/src/commanding/application/command_gateway.rs` — full rewrite
- `write_side/src/commanding/application/command_dispatcher.rs` — implement
- `write_side/src/commanding/recorder.rs` — rename + implement → `record_commands.rs`
- `write_side/Cargo.toml` — add deps and feature flag
- `write_side/src/eventing/assembly/application.rs` — minimal `EventEnvelope` stub (new file)
- Reference: `inbox/src/assembly/domain.rs`, `inbox/src/assembly/application.rs`, `inbox/src/assembly/infra_diesel.rs`, `inbox/src/record_messages.rs`, `inbox/src/inbox_consumer.rs`, `inbox/src/stale_reservation_sweep.rs`

---

## Tasks

### Task 1 — Cargo.toml and module scaffold

- [ ] Add deps to `write_side/Cargo.toml`: `uuid` (v7+serde), `chrono`, `serde`+`serde_json`, `thiserror`, `diesel` (postgres+r2d2+uuid+chrono, optional)
- [ ] Add `[features]` block: `default = ["diesel"]`, `diesel = ["dep:diesel"]`
- [ ] Create `write_side/src/commanding/assembly/` directory with `mod.rs` (re-exports `domain`, `application`; `infra_diesel` under `#[cfg(feature = "diesel")]`)
- [ ] Update `write_side/src/commanding/mod.rs`: add `assembly`, `record_commands`, `command_consumer`, `stale_command_sweep`; remove `model` (merged into domain)
- [ ] Create `write_side/src/eventing/assembly/mod.rs` + `application.rs` with a minimal `EventEnvelope { payload: String }` stub so `CommandHandlerPort` compiles

---

### Task 2 — Domain layer (`commanding/domain/mod.rs`)

- [x] Move `CommandStatus` (with all conversions and `as_str`) from `model.rs` into `domain.rs`
- [x] Define `Criterion` in `application/criteria.rs` (application query selection, not domain state): `StatusIn(Vec<CommandStatus>)`, `ScheduledBeforeNow`, `MaxAttempts(i32)`, `ReservedBefore(DateTime<Utc>)`, `OrderByScheduledAtAsc`
- [x] Leave `model.rs` as a re-export shim for backwards compat
- Note: `CommandEntry` and `NewCommandEntry` belong in `infra_diesel` (Task 9), not domain

---

### Task 3 — Application layer (`commanding/application/mod.rs`)

- [ ] Define `NewCommandMetadata { command_id: Uuid, correlation_id: Option<Uuid>, causation_id: Option<Uuid>, source: Option<String> }` with `serde` derives
- [ ] Define `NewCommandEnvelope { command_type: String, payload: String, metadata: Option<NewCommandMetadata> }` — gateway input
- [ ] Define `CommandMetadata { command_id: Uuid, correlation_id: Option<Uuid>, causation_id: Option<Uuid>, source: Option<String> }` (read-side decoded)
- [ ] Define `CommandEnvelope` (flat — no infra types): `id: Uuid`, `reservation_id: Uuid`, `command_type: String`, `payload: String`, `attempts: i32`, `metadata: Option<CommandMetadata>` — constructed by infra, returned from reservation
- [ ] Define `CommandError` enum variants: `Storage`, `Reservation`, `HandlerNotFound`, `HandlerExecution`, `EventDispatch`, `MissingReservation`, `Conversion` (all wrapping `String` or `Box<dyn Error>`)
- [ ] Define ports that only reference application-layer types (no feature-layer spec types):
  - `CommandStorePort`: `fn record(&self, envelope: &NewCommandEnvelope) -> Result<(), CommandError>`
  - `CommandProcessPort`: `fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), CommandError>` + `fn failed(&self, id: Uuid, reservation_id: Uuid, max_attempts: i32) -> Result<(), CommandError>`
  - Note: `CommandReservePort`, `CommandSweepPort`, `CommandHandlerPort` reference feature-layer spec types and are defined in their respective feature modules (Tasks 5, 6, 7)

---

### Task 4 — Record commands (`commanding/application/command_recorder.rs`)

- [ ] Define `CommandRecorderRepository { store: Arc<dyn CommandStorePort> }`
- [ ] Define `CommandRecorder { repository: CommandRecorderRepository }` with constructor `new(store: Arc<dyn CommandStorePort>)`
- [ ] Implement `record(&self, envelope: &NewCommandEnvelope) -> Result<(), CommandError>`:
  - Delegates directly to `self.repository.store.record(envelope)`
  - Conversion from `NewCommandEnvelope` → `NewCommandEntry` happens inside the infra adapter
- [ ] Delete old `recorder.rs` stub

---

### Task 5 — Command consumer (`commanding/application/command_consumer.rs`)

- [ ] Define `ReservableCommandSpec { limit: usize, max_attempts: i32 }` with `DEFAULT_MAX_ATTEMPTS: i32 = 6` and `fn criteria(&self) -> Vec<Criterion>`
- [ ] Define `CommandConsumer<R>` storing `Arc<R>`, with `R` implementing both `CommandReservePort` and `CommandProcessPort`, plus `Arc<CommandDispatcher>`
- [ ] Implement `consume(&self, spec: &ReservableCommandSpec) -> Result<(), Vec<CommandError>>`:
  - Reserve entries via `repository.reserve(spec)`
  - For each `CommandEnvelope`, reconstruct `NewCommandEnvelope` and pass to `dispatcher.dispatch()`
  - On success: call `repository.completed(id, reservation_id)`
  - On failure: call `repository.failed(id, reservation_id, spec.max_attempts)`
  - Collect all errors; return `Err(errors)` if non-empty

---

### Task 6 — Stale command sweep (`commanding/application/stale_command_sweep.rs`)

- [ ] Define `StaleCommandSpec { timeout: Duration, max_attempts: i32 }` with `fn criteria(&self) -> Vec<Criterion>` returning `[ReservedBefore(Utc::now() - timeout)]`
- [ ] Define `CommandSweeper { port: Arc<dyn CommandSweepPort> }` with constructor
- [ ] Implement `sweep(&self, spec: &StaleCommandSpec) -> Result<u64, CommandError>`: delegates to `self.port.sweep(spec)`

---

### Task 7 — Dispatcher (`commanding/application/command_dispatcher.rs`)

- [ ] Define a minimal `EventDispatchPort` trait in `eventing/assembly/application.rs`: `async fn dispatch(&self, event: EventEnvelope) -> Result<(), CommandError>`
- [ ] Define `CommandDispatcher { handler: Arc<dyn CommandHandlerPort>, event_dispatcher: Arc<dyn EventDispatchPort> }` with constructor
- [ ] Implement `dispatch(&self, envelope: &NewCommandEnvelope) -> Result<(), CommandError>`:
  - Call `self.handler.execute(envelope)` to get `Vec<EventEnvelope>`
  - For each event, call `self.event_dispatcher.dispatch(event)`
  - Return first error encountered, or `Ok(())`

---

### Task 8 — Gateway (`commanding/application/command_gateway.rs`)

- [ ] Rewrite `CommandGateway` with a `variant` field: `Direct { dispatcher: Arc<CommandDispatcher> }` or `TwoPhased { recorder: Arc<CommandRecorder> }`
- [ ] Implement `dispatch(&self, envelope: NewCommandEnvelope) -> Result<(), CommandError>`:
  - Direct: call `dispatcher.dispatch(&envelope)`
  - TwoPhased: call `recorder.record(&envelope)`
- [ ] Remove old `Command` enum and `CommandError::NotImplemented` placeholder

---

### Task 9 — Diesel infra (`commanding/assembly/infra_diesel.rs`) `[cfg(feature = "diesel")]`

Follow `inbox/src/assembly/infra_diesel.rs` exactly:

- [ ] Define `CommandEntry` struct (Queryable, all DB columns) and `NewCommandEntry` struct (Insertable, application-provided fields only)
- [ ] Define `MetadataJsonb(serde_json::Value)` newtype with `FromSql`/`ToSql` for Diesel JSONB
- [ ] Define Diesel schema for `command_entries` table (all columns)
- [ ] Implement conversion `NewCommandEnvelope` → `NewCommandEntry` (used inside `DieselPgCommandRepository::record()`)
- [ ] Implement conversion `CommandEntry` → `CommandEnvelope` (used inside `DieselPgCommandRepository::reserve()`)
- [ ] Define `DbPool` type alias and `build_pool(url: &str) -> DbPool`
- [ ] Define `DieselPgCommandRepository(DbPool)`; implement `CommandStorePort`: convert envelope → entry, INSERT with `ON CONFLICT DO NOTHING`
- [ ] Implement `CommandReservePort` on `DieselPgCommandRepository`: CTE with `SELECT … FOR UPDATE SKIP LOCKED`, update to `Reserved`, assign UUID v7 `reservation_id`, increment `attempts`, return `Vec<CommandEnvelope>`
- [ ] Implement `CommandProcessPort` on `DieselPgCommandRepository`:
  - `completed()`: set status=Completed, clear reservation fields, set `processed_at`
  - `failed()`: if `attempts >= max_attempts` → Dead; else → Failed with `scheduled_at = now + attempts * 30s` (cap 120s)
- [ ] Implement `CommandSweepPort` on `DieselPgCommandRepository`: UPDATE where `status=Reserved AND reserved_at < cutoff`, transition to Failed/Dead, clear reservation fields (do not increment `attempts`)

---

### Task 10 — Public API facade (`lib.rs`)

- [ ] Create/rewrite `write_side/src/lib.rs` with `pub mod prelude` re-exporting:
  - Gateway: `CommandGateway`
  - Dispatcher: `CommandDispatcher`
  - Record: `CommandRecorder`, `CommandRecorderRepository`
  - Consumer: `CommandConsumer`, `ReservableCommandSpec`
  - Sweep: `CommandSweeper`, `StaleCommandSpec`
  - Application: `NewCommandEnvelope`, `NewCommandMetadata`, `CommandEnvelope`, `CommandMetadata`, `CommandError`, `CommandStatus`, all port traits
  - Infra (feature-gated): `CommandEntry`, `NewCommandEntry`, `DieselPgCommandRepository`, `DbPool`, `build_pool`

---

## Verification

- [x] `make check` — all crates compile cleanly
- [x] `make test` — all tests pass
