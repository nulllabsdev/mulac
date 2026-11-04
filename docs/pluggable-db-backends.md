# Pluggable DB backends in mulac (Diesel/PG + sqlx/PG)

## Context

When building an app on mulac today, persistence splits into two layers with very different flexibility:

- **Layer 1 — your domain persistence** (command handlers + event subscribers). Already fully library-agnostic: the kernel only requires `CommandHandlerPort` / `EventSubscriberPort`, neither of which knows about any DB. `twitter` uses Diesel, `todo` uses sqlx — both work unchanged.
- **Layer 2 — mulac's own infrastructure tables** (the durable command / event / inbox / outbox queues). **Hardcoded to Diesel.** `kernel::start_persistent` (`kernel/src/lib.rs:344`) takes a concrete `mulac_diesel::DbPool` and constructs the Diesel adapters directly.

The consequence is visible in `todo`: it runs **two pools against the same Postgres** — a `sqlx::PgPool` for its todos and a Diesel `DbPool` for mulac's queues (`todoapp.rs:61`, `application.rs:170,228`). Because they are different libraries on different connections, the domain write and the queue/outbox write **cannot share a transaction**, which undercuts the whole point of an inbox/outbox design.

This document plans how to make Layer 2 swappable and ship a **first-class, maintained sqlx-pg backend** beside the Diesel one, so an app can run the entire stack (queues + domain) on a **single pool of its chosen library** — unlocking a true transactional outbox. This realizes the branch `refactor/remove-diesel-and-sqlx-dependency-from-code`.

**Design decisions:** (1) make both layers swappable AND ship a turnkey sqlx-pg adapter; (2) keep the existing **synchronous** ports — sqlx adapters block internally via the proven `block_on_blocking` pattern (do **not** make ports async); (3) keep adapters **inside the core libs behind feature flags** (a new `sqlx-pg` feature per crate), not separate adapter crates.

The abstraction we need already exists: every storage concern is a synchronous port (`CommandStorePort`, `CommandProcessPort`, `CommandReservePort`, `CommandSweepPort`, and the eventing/inbox/outbox equivalents), and Diesel code is already isolated in `infra_diesel`. The work is: add a parallel `infra_sqlx_pg`, decouple the kernel, and prove cross-backend wire compatibility.

## Approach

Mirror the existing `infra_diesel` adapters with a feature-gated `infra_sqlx_pg` set in each of the four libs, decouple the kernel into backend-agnostic wiring + two thin entrypoints, and convert `todo` to a single sqlx pool as the demonstration.

### Naming & coexistence (foundational)

Cargo features are additive, so a workspace build can enable both `diesel` and `sqlx-pg` at once. To avoid duplicate `impl Port for Struct` conflicts (E0119), **Diesel structs keep their names**; **sqlx structs get a `Sqlx` infix**:

- `CommandStoreSqlxStorage`, `CommandConsumerSqlxStorage`, `EventStoreSqlxStorage`, `EventConsumerSqlxStorage`, `InboxStoreSqlxStorage`, `InboxConsumerSqlxStorage`, `OutboxStoreSqlxStorage`, `OutboxConsumerSqlxStorage`.

Each holds a `sqlx::PgPool` with `pub fn new(pool) -> Self`. The kernel selects one at runtime via separate entrypoints.

### The blocking bridge

The libs cannot depend on the kernel (circular), so add a private helper per lib, gated on `sqlx-pg`, identical to the kernel's existing one:

```rust
fn block_on<F: Future + Send + 'static>(f: F) -> F::Output where F::Output: Send + 'static {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}
```

This is the same pattern `todo`'s `OutboxSubscriber` already uses successfully, driven by the `spawn_blocking` workers in `kernel/src/workers.rs`. **Constraint to document loudly:** `block_in_place` panics on a current-thread runtime — apps must use a multi-threaded runtime (`#[tokio::main]` default / `flavor = "multi_thread"`), and consumers must be driven via `spawn_blocking` (already true). The one inline path is `PersistentKernelState::dispatch_command`'s synchronous drain (`lib.rs:490`), fine on multi-thread; note it in the entrypoint docs.

## Implementation steps

### 1. Per-crate `infra_sqlx_pg` adapters (commanding → eventing → inbox → outbox)

Mirror the Diesel layout exactly. Each crate keeps its Diesel adapter in `src/infra_diesel/` (`repository.rs`, `reservation.rs`, `entity.rs`, `models.rs`, `conversions.rs`, `schema.rs`), declared in `lib.rs` as `#[cfg(feature = "diesel")] mod infra_diesel`; add a parallel `#[cfg(feature = "sqlx-pg")] mod infra_sqlx_pg` with the same file split.

- **Entity rows:** define parallel rows with `#[derive(sqlx::FromRow)]` (e.g. `CommandEntrySqlx`) — do not reuse the Diesel `Queryable` entity. jsonb columns typed as `Option<sqlx::types::Json<CommandMetadata>>` (`meta`) and `Option<sqlx::types::Json<ExtraInfoJsonb>>` (`extra_info`). Bind inserts with `.bind(sqlx::types::Json(value))` so the Postgres jsonb v1 wire byte is emitted.
- **Reuse domain types** verbatim (`CommandEnvelope`, `CommandMetadata`, `ExtraInfo`, `CommandStatus`, `i32::from(status)` mapping). Add `TryFrom<CommandEntrySqlx> for CommandEnvelope`.
- **Storage structs + port impls** placed to mirror Diesel, in each crate's new `src/infra_sqlx_pg/repository.rs`:
  - commanding: `CommandStorePort`, `CommandReservePort`, `CommandProcessPort`, and `CommandSweepPort`.
  - eventing: all ports.
  - inbox: `InboxStorePort`, `InboxReservePort`, `InboxProcessPort`, and `InboxSweepPort`. (`InboxTransportPort`/`AcknowledgeHandle` are async AMQP — **out of scope**.)
  - outbox: all ports, including `dead` and the transactional `failed`/`sweep`. (`OutboxPublisherPort` is AMQP — out of scope.)

**SQL translation:** `reserve`/`sweep` are already raw `diesel::sql_query` strings — copy verbatim into `sqlx::query_as::<_, RowSqlx>` / `sqlx::query`; same `$1..$n` placeholders; `.bind::<DieselType,_>(v)` → `.bind(v)`; bind `Vec<i32>` for `status = ANY($1)`; generate `reservation_id = Uuid::now_v7()` in Rust (same as Diesel). `completed`/`failed`/`dead` use the Diesel DSL today — re-express as raw SQL `UPDATE ... RETURNING`, run `failed`/`dead` inside `let mut tx = pool.begin().await?; ...; tx.commit().await?` (read attempts+extra_info, append error via existing `append_error`, status Dead if `attempts>=max_attempts` else Failed, backoff `min(max(attempts,1)*30,120)`s). Map `sqlx::Error::RowNotFound` → `MissingReservation`. Wrap every awaited query in the local `block_on(...)`. Use **runtime** `sqlx::query`/`query_as` only — never the compile-time `query!` macros (avoids a build-time `DATABASE_URL`).

Reference templates: `libs/commanding/src/infra_diesel/repository.rs`, `libs/commanding/src/infra_diesel/reservation.rs`, and `libs/outbox/src/infra_diesel/repository.rs` (hardest: `dead` + transactional `failed`/`sweep`).

### 2. Feature wiring

- `libs/Cargo.toml` `[workspace.dependencies]`: add `sqlx = { version = "0.8", default-features = false, features = ["runtime-tokio-rustls","postgres","uuid","chrono","json","derive"] }` (match `todo`'s sqlx so unification doesn't fork builds); ensure `tokio` has `rt-multi-thread`.
- Each lib's `[features]`: keep `default = ["diesel"]`; add `sqlx-pg = ["dep:sqlx", "dep:tokio"]`; add `sqlx`/`tokio` as optional deps (inbox/outbox already have optional `tokio` for amqp — extend the union with `rt-multi-thread`).
- **Generalize shared `#[cfg(feature = "diesel")]` gates** that the sqlx path also needs, to `#[cfg(any(feature = "diesel", feature = "sqlx-pg"))]`: the `Criterion` enum in `application/criteria.rs` (commanding, eventing, inbox); `ReservableCommandSpec::criteria()` + its imports/tests in `execution/reservable.rs`; `StaleCommandSpec::criteria()` in `application/stale_command_sweep.rs`; the equivalent spec/criteria in eventing/inbox/outbox. The Diesel `schema::table!` module stays `diesel`-only (the sqlx adapter uses raw SQL table names). Audit every `#[cfg(feature="diesel")]`: storage/entity/schema → stays `diesel`; shared spec/criteria/domain → `any(...)`.
- Each lib's `lib.rs` `prelude` block: add `#[cfg(feature="sqlx-pg")] pub use ...::{...SqlxStorage}`; leave existing diesel-only entity/storage re-exports gated on `diesel`.

### 3. Kernel decoupling (`kernel/src/lib.rs`, `kernel/Cargo.toml`)

- Extract the backend-agnostic tail of `start_persistent` (registries/dispatchers/gateways/consumers, current `lib.rs:351-426` minus the four `*Storage::new(db_pool)` constructions) into a private `assemble_persistent(self, command_store, command_reserve, command_process, event_store, event_reserve, event_process, drain_rounds)` taking `Arc<dyn Port>` trait objects (clone the consumer storage Arc to pass as both reserve+process, as the current code already does).
- Add two thin entrypoints that build the respective storages and delegate:
  - `#[cfg(feature="diesel")] pub fn start_persistent_diesel(self, db_pool: mulac_diesel::DbPool, drain_rounds)`
  - `#[cfg(feature="sqlx-pg")] pub fn start_persistent_sqlx(self, pool: sqlx::PgPool, drain_rounds)`
- Keep `start_persistent` as a `#[cfg(feature="diesel")] #[deprecated(note="use start_persistent_diesel")]` alias forwarding to the diesel one — no break for `twitter` or external callers.
- `kernel/Cargo.toml` `[features]`: `default = ["diesel"]`; `diesel = ["dep:mulac_diesel","commanding/diesel","eventing/diesel","inbox/diesel","outbox/diesel"]`; `sqlx-pg = ["dep:sqlx","commanding/sqlx-pg","eventing/sqlx-pg","inbox/sqlx-pg","outbox/sqlx-pg"]`. Switch the lib deps to `default-features=false` (features now forwarded), make `mulac_diesel` optional, add optional `sqlx`.
- `prelude` re-exports: gate `mulac_diesel::{DbPool,build_pool}` and the Diesel storage structs on `diesel`; add the `Sqlx` storage structs gated on `sqlx-pg`; keep the backend-agnostic types (consumers, repositories, dispatcher, gateway, recorder, specs) ungated. `block_on_blocking` stays as-is.

### 4. Ship canonical infra-table DDL for sqlx

mulac ships no infra migrations today (apps own DDL). Add `kernel/migrations/` with the canonical DDL for the four tables and a feature-gated `#[cfg(feature="sqlx-pg")] pub async fn migrate_infra(pool: &sqlx::PgPool)` wrapping `sqlx::migrate!("./migrations")`. Base the schema on `test_apps/twitter/migrations/2025-01-01-000001_infrastructure/up.sql` (the complete shape with `outbox_entries.last_error` and all indexes); ensure `extra_info jsonb` is present on `command_entries`/`event_entries`/`outbox_entries` (the entities read it). No new crate — a dir + one fn is enough. Document that the four tables are wire-shared across backends.

### 5. Convert `todo` to a single sqlx pool (the demonstration)

- `test_apps/todo/src/assembly/application.rs:158-229` `start_mulac`: drop the `database_url` param and the `kernel::prelude::build_pool` call (170-171); change the final `.start_persistent(db_pool, 1)` → `.start_persistent_sqlx(pool, 1)`; signature becomes `pub async fn start_mulac(pool: sqlx::PgPool) -> Result<MulacHandle, KernelError>`.
- `test_apps/todo/src/assembly/bin/todoapp.rs:61`: `start_mulac(pool.clone(), &database_url)` → `start_mulac(pool.clone())`; add `kernel::migrate_infra(&pool).await?` after the existing `migrate(&pool)`.
- `test_apps/todo/Cargo.toml`: `kernel = { ..., default-features = false, features = ["sqlx-pg"], package = "mulac-kernel" }` — todo drops the Diesel/r2d2 build entirely. Now one pool backs domain + queues, so the outbox write can share a transaction with command/event persistence (a follow-up can wrap them in one `tx`).
- Keep `twitter` on `start_persistent_diesel` so both backends stay exercised.

## Risks / edge cases

- **Multi-threaded runtime required** for `block_in_place`; document in lib `block_on` and in the sqlx entrypoint. The inline drain in `dispatch_command` runs on the caller's worker thread — fine on multi-thread.
- **No compile-time sqlx macros** — runtime `query`/`query_as` only; only `#[derive(FromRow)]` is compile-time (needs no DB), keeping the `margo` publish pipeline offline-safe.
- **Feature unification** builds both backends in a full workspace build — safe due to distinct struct names. CI must build three configs (see Verification).
- **jsonb parity** is the key compatibility risk — covered by the cross-backend test below.
- **Publishing (margo registry):** bump 0.3.0 → 0.4.0 (public `prelude` surface + `start_persistent` deprecation are API changes); publish bottom-up — `mulac_diesel` → commanding → eventing/inbox (inbox depends on commanding) → outbox → `mulac-kernel`.

## Verification

1. **Build matrix:** `cargo build` (default diesel); `cargo build -p mulac-kernel --no-default-features --features sqlx-pg`; `cargo build -p mulac-kernel --features "diesel,sqlx-pg"`. All must compile.
2. **Cross-backend wire compatibility** (new integration test, both features on, guarded by a live `DATABASE_URL`): Diesel-write → sqlx-read and sqlx-write → Diesel-read of a command with populated `meta` and an appended `extra_info` error; assert id/type/payload/attempts/status and jsonb fields round-trip. Assert raw `SELECT status` ints (2 Reserved, 5 Completed, 4 Failed, 7 Dead) and `extra_info = {"errors":[…]}` are identical from both adapters. Assert backoff `≈ now + min(attempts*30,120)s`.
3. **Reservation safety:** two concurrent reservations (one diesel + one sqlx) over N rows with `LIMIT k` return disjoint id sets, total ≤ N (validates `FOR UPDATE SKIP LOCKED` survives the blocking bridge).
4. **End-to-end `todo` on one pool:** `cargo run -p test_app_todo -- migrate` then `serve`; exercise the existing `test_apps/todo/tests/*` (create/complete/inbox/outbox) and confirm green with the single sqlx pool and `start_persistent_sqlx`.
5. **Regression:** `twitter` tests stay green on the Diesel path.
