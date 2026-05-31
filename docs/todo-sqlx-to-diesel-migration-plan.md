# Migrate the `todo` test app from sqlx to diesel

## Context

`test_apps/todo` is the **only** crate in the `mulac` workspace still using `sqlx`. Every
other database-touching crate — the four libs (`inbox`, `outbox`, `commanding`,
`eventing`), the `mulac-kernel`, and the sibling `test_apps/twitter` app — already runs on
**diesel 2** (`r2d2` blocking pool) via the shared `mulac_diesel::DbPool` type. The goal of
this branch (`chore/drop-sqlx-from-todo`) is to remove that lone sqlx island so the whole
workspace shares one persistence stack.

Key finding that makes this tractable: the todo app **already builds a diesel pool**. Inside
`start_mulac` (`src/assembly/application.rs:170`) it calls `kernel::io::build_pool(database_url)`
to get a `DbPool` for the kernel's `start_persistent`, *in addition to* the sqlx `PgPool` it
uses for its own queries. So diesel is already a (transitive) dependency and the kernel
already demands a diesel pool. The migration is really: **collapse the app's sqlx pool onto
the diesel pool the kernel already uses**, converting ~11 queries to diesel.

The `twitter` app is a complete, working blueprint for the identical kernel + poem +
inbox/outbox structure. We mirror it. Per the agreed decisions: queries use the **typed diesel
DSL** (with the `serde_json` feature so `jsonb` columns map to `serde_json::Value`), and the
work lands as **5 atomic commits, each compiling with green tests**.

## What sqlx surface exists today (the work to convert)

- **Pool / migrations**: `src/assembly/infra_sqlx_pg.rs` — `connect()` (sqlx pool),
  `migrate()` (`sqlx::migrate!("./migrations")`), `fetch_todo()`, `record_event_payload()`,
  `OutboxSubscriber`, and the `entity::TodoRow` (`#[derive(sqlx::FromRow)]`).
- **AppState** (`src/lib.rs:42`): `pub pool: PgPool`.
- **Per-feature write infra** (each is `async fn ...(pool: &PgPool, ...)` using
  `sqlx::query_as::<_, TodoRow>`): `task_create.rs` (INSERT, wrapped in a tx),
  `task_complete.rs`, `task_reopen.rs`, `task_update.rs`, `task_delete.rs` (DELETE…RETURNING),
  `task_schedule_due_dates.rs`, plus read-only `task_list.rs` and `task_get.rs`.
- **inbox/outbox modules in `src/lib.rs`**: `record_received` (INSERT … ON CONFLICT DO
  NOTHING), `mark_processed`, `mark_failed`, outbox `list`.
- **Handlers**: each `*Handler::execute` wraps its async infra call in
  `kernel::io::block_on_blocking(async move { ... .await })`.
- **HTTP handlers**: async, call infra directly with `.await` on `&state.pool`.
- **domain.rs**: `TodoStatus` derives `sqlx::Type` with `#[sqlx(type_name = "text", ...)]`.
- **Migrations** (`migrations/`): flat numbered files `0001_init.sql` (todos, inbox_messages,
  outbox_messages), `0002_write_side.sql` (command_entries, event_entries — kernel tables),
  `0003_extra_info.sql` (adds `extra_info` jsonb). All use `IF NOT EXISTS`, so re-running is
  idempotent.
- **Tests** (`tests/utils.rs`): `start_test_app()` returns `(_, PgPool, _)`; row structs derive
  `sqlx::FromRow`; cleanup is a `DELETE FROM <table>` loop; `connect`/`migrate` used directly.
- **Cargo.toml**: the `sqlx` dependency (to be removed); `Makefile` comment says "sqlx
  migrations".

## Target end-state (mirror `test_apps/twitter`)

- `Cargo.toml`: drop `sqlx`; add `diesel = { version = "2", features = ["postgres", "r2d2",
  "uuid", "chrono", "serde_json"] }` and `diesel_migrations = "2"`.
- `src/schema.rs`: hand-written `diesel::table!` for the three **app** tables only — `todos`,
  `inbox_messages`, `outbox_messages` (kernel tables `command_entries`/`event_entries` are the
  kernel's concern; the app never queries them outside tests, and tests read them via raw SQL).
- `src/assembly/infra_diesel.rs` (renamed from `infra_sqlx_pg.rs`): re-export
  `kernel::io::{DbPool, build_pool}`; `MIGRATIONS`/`run_migrations`; sync diesel `fetch_todo`,
  `record_event_payload`, `OutboxSubscriber`; `entity::TodoRow` deriving
  `diesel::Queryable` + `diesel::Selectable`.
- All infra functions become **synchronous**, take `&DbPool`, do `let mut conn = pool.get()?;`.
- `AppState.pool: DbPool`; one pool shared by HTTP layer, command handlers, event subscribers,
  and `start_persistent` (exactly as twitter does).
- Command `*Handler::execute` calls sync infra directly (it already runs on the kernel's
  blocking worker thread — `block_on_blocking` is removed).
- Async HTTP handlers wrap blocking diesel calls in a `run_blocking` helper (copy twitter's
  `run_blocking` in `application.rs` → `tokio::task::spawn_blocking`).
- `start_mulac(pool: DbPool)` — no `database_url` param, no internal `build_pool`.
- Migrations in diesel layout (timestamped dirs, `up.sql`/`down.sql`), embedded via
  `diesel_migrations::embed_migrations!`.

Reference files to copy patterns from:
- `test_apps/twitter/src/assembly/infra_diesel.rs` (pool, `run_migrations`, `fetch_*`,
  `record_event_payload`, `OutboxSubscriber`).
- `test_apps/twitter/src/assembly/application.rs` (`run_blocking`, `start_mulac(pool)`).
- `test_apps/twitter/src/tweet_post.rs` / `tweet_delete.rs` (DSL insert/update/get_result,
  `.optional()` for not-found).
- `test_apps/twitter/src/assembly/bin/twitterapp.rs` (`build_pool` + `run_migrations` +
  `start_mulac(pool.clone())` + `AppState::new(pool, …)`).
- `test_apps/twitter/tests/utils.rs` (`DbPool` test setup, `QueryableByName` rows, `TRUNCATE`).
- `test_apps/twitter/migrations/` and any `diesel.toml` for migration layout.

## The diesel query map (DSL form)

| sqlx today | diesel DSL replacement |
|---|---|
| `fetch_todo` SELECT by id | `todos::table.find(id).first::<TodoRow>(&mut conn).optional()? → NotFound` |
| `list` (all / by status) | `todos::table.order(todos::created_at.asc()).load::<TodoRow>` (+ `.filter(todos::status.eq(s))`) |
| `create_from_command` INSERT…RETURNING | `diesel::insert_into(todos::table).values((..)).get_result::<TodoRow>` (optionally inside `conn.transaction`) |
| `complete`/`reopen` UPDATE…RETURNING | `diesel::update(todos::table.find(id)).set((status.eq(..), updated_at.eq(now))).get_result::<TodoRow>().optional()?` |
| `update_from_command` | `diesel::update(...).set((title.eq, description.eq, updated_at.eq)).get_result().optional()?` |
| `set_due_date` | `diesel::update(...).set((due_at.eq, updated_at.eq)).get_result().optional()?` |
| `delete_from_command` DELETE…RETURNING | `diesel::delete(todos::table.find(id)).get_result::<TodoRow>().optional()?` |
| `record_event_payload` INSERT outbox | `diesel::insert_into(outbox_messages::table).values((id.eq, event_type.eq, payload.eq(value), status.eq("pending"), created_at.eq(now)))` |
| `record_received` INSERT…ON CONFLICT | `diesel::insert_into(inbox_messages::table).values((..)).on_conflict_do_nothing().execute()` → `rows == 0` ⇒ `Conflict` |
| `mark_processed` / `mark_failed` | `diesel::update(inbox_messages::table.find(id)).set((..)).execute()` |
| outbox `list` | `outbox_messages::table.order(created_at.asc()).load::<OutboxRow>` |

Error mapping: `diesel::result::Error → AppError::Storage(anyhow::anyhow!(e))`; "not found"
via `.optional()?` returning `None → AppError::NotFound` (read paths) / the existing
`rows == 0 → "todo not found"` convention (write paths) so `interpret_dispatch_error` still
maps to 404. `serde_json::Value ↔ Jsonb` is handled automatically by the `serde_json` feature.

---

## Checklist — 5 atomic commits (each compiles, `make test` green)

### ☐ Commit 1 — `chore(todo): add diesel deps and schema` (scaffolding, no behavior change)
- [ ] `Cargo.toml`: add `diesel = { version = "2", features = ["postgres","r2d2","uuid","chrono","serde_json"] }` and `diesel_migrations = "2"`. **Keep sqlx for now.**
- [ ] Add `src/schema.rs` with `diesel::table! { todos { .. } }`, `inbox_messages { .. }`,
  `outbox_messages { .. }` (column types per the table at the bottom of this plan), plus
  `diesel::allow_tables_to_appear_in_same_query!(...)`.
- [ ] Declare `mod schema;` in `src/lib.rs` (allow it to be unused this commit).
- [ ] Optional: add `diesel.toml` pointing `print_schema.file = "src/schema.rs"` for future regen.
- [ ] Verify: `cargo build` + `make test` green (schema unused, sqlx still active).

### ☐ Commit 2 — `refactor(todo): run migrations with diesel`
- [ ] Convert `migrations/` to diesel layout — one timestamped dir per existing file, each with
  `up.sql` (existing SQL, keep `IF NOT EXISTS`) and a matching `down.sql` (reverse):
  `..._init` (todos/inbox_messages/outbox_messages), `..._write_side`
  (command_entries/event_entries), `..._extra_info` (add `extra_info`). Preserve order via
  timestamps.
- [ ] In `infra_sqlx_pg.rs`: add `pub const MIGRATIONS: EmbeddedMigrations =
  embed_migrations!("migrations");` and `pub fn run_migrations(database_url: &str) ->
  anyhow::Result<()>` that builds a short-lived `kernel::io::build_pool` connection and calls
  `conn.run_pending_migrations(MIGRATIONS)`. Remove the sqlx `migrate()` fn.
- [ ] Update re-exports in `src/lib.rs` `io` (`migrate` → `run_migrations`).
- [ ] `bin/todoapp.rs`: replace `migrate(&pool).await?` with `run_migrations(&database_url)?` in
  both the `migrate` and `serve` arms (app/AppState still uses the sqlx `connect()` pool).
- [ ] `tests/utils.rs`: replace `migrate(&pool).await.unwrap()` with
  `run_migrations(&database_url).unwrap()`.
- [ ] Update the `Makefile` comment ("sqlx migrations" → "diesel migrations") if present.
- [ ] Verify: `make migrate` then `make test`. Diesel creates `__diesel_schema_migrations`;
  the old `_sqlx_migrations` table is left orphaned (harmless). Idempotent on existing DBs.

### ☐ Commit 3 — `feat(todo): migrate write-side infra to diesel`
Scope: command handlers' write paths + the outbox writer, which can flip independently because
`start_mulac` already has a diesel pool to hand them. The HTTP read path stays on sqlx this commit.
- [ ] Rename `src/assembly/infra_sqlx_pg.rs` → `src/assembly/infra_diesel.rs`; update
  `mod infra_sqlx_pg;` references (in `assembly/mod.rs`). Convert `entity::TodoRow` to
  `#[derive(Queryable, Selectable)]` with `#[diesel(table_name = crate::schema::todos)]`.
- [ ] Convert `record_event_payload` and `OutboxSubscriber` to **sync** diesel (`&DbPool`,
  `pool.get()?`, DSL insert). `OutboxSubscriber::handle` calls it directly (drop
  `block_on_blocking`).
- [ ] In each feature's `infra_*` module (`task_create`, `task_complete`, `task_reopen`,
  `task_update`, `task_delete`, `task_schedule_due_dates`): rename to `infra_diesel`, make the
  fn **sync**, take `&DbPool`, use the DSL replacements from the query map. `task_create` may
  wrap its insert in `conn.transaction(|c| …)` to match today's tx.
- [ ] In each `*Handler::execute`: call the sync infra directly (remove the
  `block_on_blocking(async move { … .await })` wrapper and its import).
- [ ] In `start_mulac` (`application.rs`): build the diesel pool (already done at line 170) and
  pass **that** `DbPool` to every `*Handler::new(...)` and `OutboxSubscriber::new(...)` instead
  of the sqlx `pool`. The sqlx `pool` param is now unused by handlers — prefix `_pool` to keep
  the signature stable for this commit (the HTTP read side still uses the sqlx pool via AppState).
- [ ] Keep `fetch_todo`, inbox (`record_received`/`mark_processed`/`mark_failed`) and outbox
  `list` on **async sqlx** for now; HTTP create/get still call `fetch_todo(&state.pool,…).await`.
- [ ] Verify: `make test` green. Writes now go through diesel; reads still through sqlx (two
  connections to the same DB — already the situation today with the kernel pool).

### ☐ Commit 4 — `feat(todo): migrate read-side and AppState to diesel`
Scope: the remaining read/inbox/outbox infra + the AppState pool type + bin/tests wiring.
- [ ] Convert `fetch_todo` to sync diesel (`.optional()? → NotFound`); convert `task_list::list`
  and `task_get::get` to sync diesel.
- [ ] Convert the inbox `record_received`/`mark_processed`/`mark_failed` and the outbox `list`
  in `src/lib.rs` to sync diesel; convert the outbox `OutboxRow` to `#[derive(Queryable)]`.
- [ ] Add `run_blocking` helper to `application.rs` (copy twitter's `spawn_blocking` wrapper).
  In each async HTTP handler, wrap the blocking calls — `state.mulac.dispatch_command(...)`,
  `fetch_todo`, `record_received`/`mark_*`, outbox `list` — in `run_blocking(move || …).await`.
- [ ] `src/lib.rs`: change `AppState.pool` from `sqlx::PgPool` to
  `kernel::io::DbPool` (re-exported as `DbPool`); update `AppState::new`.
- [ ] `start_mulac(pool: DbPool)`: drop the `database_url` param and the internal `build_pool`;
  pass the received pool to handlers/subscribers and to `start_persistent(pool, 1)`.
- [ ] `bin/todoapp.rs` (`serve` arm): `let pool = build_pool(&database_url)?;`
  `run_migrations(&database_url)?;` `let kernel = start_mulac(pool.clone())…;`
  `AppState::new(pool, kernel.state())`. Drop the sqlx `connect()` call. Update `io` re-exports
  (`connect` removed; export `build_pool`).
- [ ] `tests/utils.rs`: `start_test_app()` returns `(String, DbPool, OwnedMutexGuard<()>)`;
  build the pool with `build_pool` (OnceLock-shared like twitter); switch row structs to
  `#[derive(diesel::QueryableByName)]` with `#[diesel(sql_type = …)]`; rewrite `fetch_*`
  helpers and the cleanup with `diesel::sql_query(...)` (`TRUNCATE … RESTART IDENTITY CASCADE`
  or the existing per-table deletes). Helper signatures take `&DbPool`. Test files are
  unaffected (they only destructure `pool` and pass it through).
- [ ] Verify: `make test` green. Runtime now uses a **single** diesel pool end-to-end; no sqlx
  calls remain at runtime (the crate is still linked).

### ☐ Commit 5 — `chore(todo): drop sqlx dependency`
- [ ] `Cargo.toml`: remove the `sqlx` dependency line.
- [ ] `domain.rs`: drop `sqlx::Type` from `TodoStatus`'s derive list and delete the
  `#[sqlx(type_name = "text", rename_all = "snake_case")]` attribute (the `as_str()` /
  `TryFrom<TodoRow>` conversions already cover persistence).
- [ ] Remove any lingering `use sqlx::…` imports and the `TodoRow` re-export name if it changed.
- [ ] Verify: `grep -rn sqlx test_apps/todo/src test_apps/todo/tests test_apps/todo/Cargo.toml`
  returns nothing; `cargo build`, `cargo clippy`, `make test` all green.

---

## Schema (`src/schema.rs`) — exact column types

```rust
diesel::table! {
    todos (id) {
        id -> Uuid,
        title -> Text,
        description -> Nullable<Text>,
        status -> Text,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
        due_at -> Nullable<Timestamptz>,
    }
}
diesel::table! {
    inbox_messages (id) {
        id -> Uuid,
        message_type -> Text,
        payload -> Jsonb,
        status -> Text,
        received_at -> Timestamptz,
        processed_at -> Nullable<Timestamptz>,
        error -> Nullable<Text>,
    }
}
diesel::table! {
    outbox_messages (id) {
        id -> Uuid,
        event_type -> Text,
        payload -> Jsonb,
        status -> Text,
        created_at -> Timestamptz,
        published_at -> Nullable<Timestamptz>,
        attempts -> Int4,
    }
}
diesel::allow_tables_to_appear_in_same_query!(todos, inbox_messages, outbox_messages);
```

## Verification (end-to-end, after each commit and at the end)

1. **DB up**: `cd test_apps && make up` (postgres:16 on `127.0.0.1:5433`; `DATABASE_URL=postgres://todo:todo@127.0.0.1:5433/todo`).
2. **Migrate**: `cd test_apps/todo && make migrate` → "migrations applied"; confirm
   `__diesel_schema_migrations` exists and `todos`/`inbox_messages`/`outbox_messages`/
   `command_entries`/`event_entries` are present.
3. **Tests**: `make test` — all integration tests (`todo_create`, `todo_complete`, `todo_delete`,
   `todo_get`, `todo_list`, `todo_reopen`, `todo_update`, `todo_due_date`, `inbox`) pass.
4. **Smoke run**: `make serve`, then exercise the API and the inbox/outbox flow:
   - `curl -s -XPOST localhost:3000/api/todos -H 'content-type: application/json' -d '{"title":"buy milk"}'`
   - `curl -s localhost:3000/api/todos` and `GET /api/todos/{id}`
   - `PATCH`/`complete`/`reopen`/`due-date`/`DELETE` endpoints
   - `POST /api/messages/commands` then `GET /api/messages/outbox` — confirm the event was
     recorded `pending` (proves the diesel `OutboxSubscriber` ran).
5. **Workspace sanity**: from repo root run the workspace `Makefile` `test`/`check` to confirm
   nothing else regressed; `grep -rn sqlx test_apps/todo` is empty.

## Notes / risks
- **Migration tooling table swap**: switching runners leaves the old `_sqlx_migrations` table
  orphaned and creates `__diesel_schema_migrations`. Because every `up.sql` keeps `IF NOT
  EXISTS`/`ADD COLUMN IF NOT EXISTS`, the first diesel run is safe on already-provisioned DBs
  and on fresh ones. Optional cleanup: `DROP TABLE _sqlx_migrations;` (not required).
- **Blocking-in-async**: today the async HTTP handlers already call the synchronous
  `dispatch_command` (kernel diesel writes) inline; commit 4's `run_blocking` wrapper makes this
  correct, matching twitter. Command/event handler `execute` already runs on the kernel's
  `spawn_blocking` worker, so calling blocking diesel there directly is correct.
- **Conventional commits + LLM attribution**: per `CLAUDE.md`, commit subjects are
  `<type>(todo): <subject>`; if an LLM authors a change add `Author: claude::claude-opus-4-8::<effort>`
  (no `Co-authored-by`).
