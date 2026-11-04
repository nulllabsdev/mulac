# Bring eventing, inbox, and outbox into alignment with commanding

Status: implemented (stages 1–6). Eventing, inbox, and outbox use unified repositories and generic consumers/services; transactional timeout policies and reusable HTTP endpoints are in place. Baseline: commit `46856ec`, including commanding’s HTTP-boundary refactor. Requirements below take precedence over illustrative reference code. If later repository changes conflict with these requirements, report the conflict rather than silently updating the scope.

## How to read this plan

Required behavior, compatibility constraints, safety rules, and explicitly identified architectural decisions are binding. Reference implementations illustrate possible approaches; they are not requirements for identical internal structure.

Choose private types, helpers, file decomposition, and algorithms to satisfy those requirements with the simplest maintainable design.

If repository behavior or reference code conflicts with a requirement, report the conflict before changing that behavior. Do not silently copy the reference or expand the task's scope.

## 1. Summary and agreed decisions

Apply the commanding pattern to eventing, inbox, and outbox. That means layered modules, unified Diesel repositories, consumers and services that are generic over their repository, reusable HTTP endpoints, and policy-driven transactional bulk sweeps.

Confirmed decisions:

- **Clean replacement.** Remove the types and helpers listed in §2 "Public API changes" and update every caller. Add no compatibility aliases.
- **Sweeps change on purpose; explicit failures do not.** Sweep behavior changes as listed in §3. Explicit `failed()` and `dead()` paths keep their current delays, clocks, and statuses.
- **Inbox gets both endpoints.** Add submission and processing endpoints.
- **HTTP adaptation stays outside application.** Shared contracts, validation, response conversion, and blocking-execution bridges belong to the HTTP boundary. Preserve public service types and Cargo feature names. Commanding’s private `http` module illustrates this boundary; its exact file decomposition is not required.
- **ADR-007 is superseded and ADR-008 is amended.** One repository per queue replaces use-case-split repositories. Consumers and services become generic over that repository. Recorders and sweepers take `Arc<dyn …Port>`. Dispatcher, gateway, and publisher dependencies keep their current types. Commanding already follows this pattern. Record the decision in ADR-012 (stage 1).
- **No format changes.** Keep database schemas, serialized contracts, HTTP status codes, and Cargo feature names unchanged.

Non-goals:

- No redesign of explicit-failure retry policies.
- No automatic start of sweep workers.
- No repair of stranded rows (defined in §5 "Rollout notes").
- No shared, table-parameterized queue in `mulac_diesel`. Each crate keeps its own repository.
- No `SKIP LOCKED`, batch-size option, or per-row update loop in sweeps.

## 2. Module structure and public interfaces

### Layered organization

Organize each crate into private domain, application, Diesel infrastructure, and HTTP boundaries. Commanding groups shared HTTP adaptation and framework adapters under `http`; private file decomposition is an implementation choice. The crate's only public interface is `pub mod prelude` (see `CLAUDE.md`). Move the contents of the old `assembly` module into these layers, then delete `assembly`. Reference layout: `libs/commanding/src/`.

| Layer                 | Responsibilities                                                                                                                                                              |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Domain                | Status enums and their conversion errors, `ExtraInfo`, `ReservationTimeoutPolicy`                                                                                             |
| Application           | Recorders, consumers, dispatchers and gateways where present, specifications, criteria, ports, envelopes, metadata, application errors, conversions between application types |
| Diesel infrastructure | Repository, database entities, JSONB models, schema, conversions to and from database entities                                                                                |
| Shared HTTP boundary  | HTTP contracts, synchronous execution bridges, error mapping, asynchronous blocking adaptation, test fixtures                                                                 |
| HTTP adapters         | Framework-specific extraction, responses, reusable routes                                                                                                                     |

Specific moves:

- Inbox `InboxMessage` moves from domain into application. So do outbox `OutboxEntry`, `NewOutboxEntry`, and `OutboxEntryMetadata`. Diesel-derived records stay in infrastructure.
- Conversions between application types that are currently gated behind the `diesel` feature move into application and must compile with `--no-default-features`. Example: eventing's `From<EventMetadata> for NewEventMetadata`, now in `assembly/infra_diesel.rs`.
- Delete `libs/eventing/src/model.rs`, which is an empty placeholder. Delete `libs/outbox/src/model.rs`, which `lib.rs` never declares and so is never compiled.
- Leave the AMQP modules in place: `inbox/src/amqp_consumption.rs`, `outbox/src/amqp_publisher.rs`, and `outbox/src/confirmed_amqp_publisher.rs`. Limit changes to imports, construction, and visibility/re-exports needed to satisfy the prelude convention. Acknowledgment and publishing behavior must not change.

Split each crate's Diesel code into files where that aids readability; commanding's `infra_diesel/` is one example. Keep the repository and its port implementations together so the ports it satisfies are easy to see.

When a stage finishes, each migrated feature module must expose only `pub mod prelude`; implementation submodules remain private. `scripts/check_prelude.py` does not check this today; it only rejects `mod io`. Extending the script to enforce the rule is optional.

### Unified repositories

| Crate    | New repository             | Implements                                                                                          |
| -------- | -------------------------- | --------------------------------------------------------------------------------------------------- |
| eventing | `DieselPgEventRepository`  | `EventStorePort`, `EventReservePort`, `EventProcessPort`, `EventSweepPort`                          |
| inbox    | `DieselPgInboxRepository`  | `InboxStorePort`, `InboxReservePort`, `InboxProcessPort`, `InboxSweepPort`                          |
| outbox   | `DieselPgOutboxRepository` | `OutboxStorePort`, `OutboxReservePort`, `OutboxProcessPort` (including `dead()`), `OutboxSweepPort` |

Each repository has a `new(pool: DbPool)` constructor, like `DieselPgCommandRepository`.

Recorders and consumers:

- **Recorders take `Arc<dyn …StorePort>` directly.** Reference: `libs/commanding/src/application/command_recorder.rs`, since removed; ADR-014 later removed `EventRecorder` and `InboxRecorder` as well. Keep their method names and argument ownership:
  - `EventRecorder::record(&NewEventEnvelope)`
  - `InboxRecorder::publish(NewInboxMessageEnvelope)`, which takes its argument by value
  - `OutboxRecorder::record(&NewOutboxEnvelope)`
- **Consumers become generic:** `EventConsumer<R>`, `InboxConsumer<R>`, `OutboxConsumer<R>`. Each holds one `Arc<R>`, where `R` implements the reserve and process ports. Reference: `libs/commanding/src/execution/command_consumer.rs`. Keep their method names: `consume`, `process`, `publish_batch`.
- **Other dependencies stay separate.** Dispatcher, command gateway, and publisher remain separate constructor arguments.
- **No `'static` on synchronous code.** Synchronous consumers and services must not require `R: 'static`.

### Public API changes

Each crate's prelude after the change is its current prelude, minus the "Removed" column, plus the "Added" column. Everything else stays, including names that differ from commanding. Do not rename them to match.

| Crate    | Removed                                                                                                                                                                                    | Added                                                                |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------- |
| eventing | `EventRecorderRepository`, `EventConsumerRepository`, `EventStoreStorage`, `EventConsumerStorage`                                                                                          | `DieselPgEventRepository`; `axum::event_routes`; `poem::EventingApi` |
| inbox    | `InboxRecorderRepository`, `InboxConsumerRepository`, `InboxStoreStorage`, `InboxConsumerStorage`, `repository`                                                                            | `DieselPgInboxRepository`; `axum::inbox_routes`; `poem::InboxApi`    |
| outbox   | `OutboxRecorderRepository`, `OutboxConsumerRepository`, `OutboxStoreStorage`, `OutboxConsumerStorage`, `consumer_repository`, `recorder_repository`, `reservation_sweeper`, `append_error` | `DieselPgOutboxRepository`; `axum::outbox_routes`; `poem::OutboxApi` |
| kernel   | The eventing and inbox items removed above                                                                                                                                                 | `DieselPgEventRepository`, `DieselPgInboxRepository`                 |

These names stay unchanged even though commanding uses different ones:

- `EventSweeper`, `StaleEventSpec`
- `ReservationSweeper`, `StaleReservationSpec` (inbox and outbox)
- `ReservableEventSpec`, `ReservableInboxSpec`, `ReservableOutboxSpec`
- `DbPool` and `build_pool` (inbox and outbox)
- `EventEntry`, `NewEventEntry`

Timeout policies remain internal and are not new public prelude exports; only their behavior and the constructor described below are required. Do not copy commanding’s public mutable fields merely for parity.

Criteria types stay crate-private and are not added to any prelude. Inbox currently has a separate `SweepCriterion`, and outbox's sweep spec has no criteria at all. Whether to merge or add criteria types is up to you.

### Services, kernel, and runtime

**Synchronous services.** ADR-014 later removed these services. The synchronous HTTP execution bridges become generic over their consumer repository: `EventingService<R>`, `InboxService<R>`, `OutboxService<R>`. Reference: `libs/commanding/src/http/execution.rs` before ADR-013, which removed commanding's synchronous service.

- `InboxService::from_postgres(pool)` and `OutboxService::from_postgres(pool, publisher)` stay. Define them on the `DieselPg…Repository` specialization.
- `InboxService::from_postgres` uses one `DieselPgInboxRepository` for both its recorder and its consumer.

**Asynchronous services.** `AsyncEventingService`, `AsyncInboxService`, and `AsyncOutboxService` stay non-generic, as `AsyncCommandingService` is. Reference: `libs/commanding/src/http/execution.rs`, which since ADR-013 wraps the consumer directly instead of a synchronous service. Requirements:

- `'static` bounds appear only where work moves into a Tokio blocking task.
- Inbox's `submit` and `run` share one `InboxService` instance.
- `AsyncOutboxService::unconfigured()` keeps working. It validates the request and returns `PublisherNotConfigured` (HTTP 503) without touching storage.
- `From<…Service<R>>` conversions keep working, so `impl Into<Async…Service>` remains the HTTP entry point.

**Kernel** (`kernel/src/lib.rs`, `kernel/src/workers.rs`):

- `run_event_worker` becomes generic, like `run_command_worker`.
- `PersistentKernelState`, `PersistentKernelHandle::event_consumer()`, and `KernelHandle::event_consumer(repo)` follow their command counterparts, using `DieselPgEventRepository`.
- Persistent assembly uses one `DieselPgEventRepository` for both the event recorder and the event consumer, the same way it already shares the command repository.
- Prelude re-exports follow the table above.

**Everything else that must be updated:**

- Test doubles in each shared HTTP testing module implement both consumer ports on one type. Reference: `libs/commanding/src/http/testing.rs`.
- Examples, AMQP construction sites, each crate's `tests/`, `test_apps/http_adapters`, and downstream type annotations.

## 3. Timeout policies and bulk sweeping

### Sweep rules: current → target

"Exhausted" means `attempts >= max_attempts`.

| Queue    | Retry delay                                       | Exhausted rows                                                                          | Clock                  |
| -------- | ------------------------------------------------- | --------------------------------------------------------------------------------------- | ---------------------- |
| eventing | `attempts × 30 s`, uncapped (unchanged)           | currently always `Failed` → becomes `Dead`, with `scheduled_at` = database time + delay | application → database |
| inbox    | `attempts × 30 s`, uncapped (unchanged)           | currently always `Failed` → becomes `Dead`, with `scheduled_at` = database time + delay | application → database |
| outbox   | `min(max(attempts, 1) × 30 s, 120 s)` (unchanged) | `Dead`, keeping the previous `scheduled_at` (unchanged)                                 | application → database |

Unchanged in every crate:

- Sweeps do not increment `attempts`.
- Appended error messages: eventing `"event reservation timed out"`, inbox `"inbox reservation timed out"`, outbox `"publication reservation timed out"`.
- Sweep port count types: eventing `u64`, inbox `usize`, outbox `u64`.

Explicit-failure paths are out of scope and keep their current rules:

- eventing: capped at 120 s
- inbox: `max(attempts, 1) × 30 s`, uncapped
- outbox: capped at 120 s

All three use the application clock. Outbox's `failed()` may call the new policy's delay function, provided its behavior stays the same.

### Policy

Each crate adds a `ReservationTimeoutPolicy` to `domain`, using the crate's own status type. `libs/commanding/src/domain/reservation_timeout_policy.rs` illustrates the internal policy responsibility, not a required public API. It must provide:

- constructor `new(reserved_before, max_attempts)`
- selects `Reserved` rows
- the retryable outcome is `Failed`
- the outcome is `Dead` at or above `max_attempts`
- the delay is 30 s per attempt

The policy is pure: no database access and no clock reads.

Outbox's policy differs in two ways:

- The delay is capped, as in the table.
- A `Dead` outcome keeps the row's previous `scheduled_at`.

Both rules must live in the policy, not in SQL. How the policy exposes them is up to you.

Inbox's `StaleReservationSpec` gains `with_max_attempts`, matching eventing and outbox. Every sweep spec keeps the default `max_attempts = 6`. Document on each spec that sweeps and consumers should use the same limit.

### Sweep invariants

The transaction, blocking-lock, application-side policy evaluation, and single set-based update requirements below are explicit architectural decisions, not illustrative algorithms. Unbounded candidate locking is an accepted trade-off. Internal SQL construction, helpers, and outcome representation remain implementation choices.

Every repository's sweep must:

1. Run in one transaction. Any error rolls back every row.
2. Read the database time once, with `transaction_timestamp()`, and use it for the cutoff, `updated_at`, and the new `scheduled_at`. Never compare reservations against application time. If a spec has a `criteria` method, it takes this timestamp as an argument instead of calling `Utc::now()`.
3. Select `Reserved` rows with `reserved_at < cutoff`. The comparison is strict, and rows with a null `reserved_at` are never selected.
4. Keep the selected rows locked from selection until the update, using blocking row locks as commanding does.
5. Compute each row's new status and `scheduled_at` with the domain policy, not in SQL.
6. Apply every outcome in one set-based statement. Clear `reservation_id` and `reserved_at`. Append the timeout message to `extra_info.errors` and keep the existing errors.
7. Return the number of affected rows, or zero when nothing was selected.

`impl CommandSweepPort for DieselPgCommandRepository` in `libs/commanding/src/infra_diesel/repository.rs` shows one way to satisfy these. Its trade-off is accepted: all candidates are loaded and locked at once. Any design that meets the invariants is fine.

## 4. Reusable HTTP endpoints

Expose these through each crate's framework prelude:

| Crate    | Axum factory             | Poem OpenAPI object         | Relative endpoints          |
| -------- | ------------------------ | --------------------------- | --------------------------- |
| eventing | `event_routes(service)`  | `EventingApi::new(service)` | `POST /run`                 |
| inbox    | `inbox_routes(service)`  | `InboxApi::new(service)`    | `POST /run`, `POST /submit` |
| outbox   | `outbox_routes(service)` | `OutboxApi::new(service)`   | `POST /run`                 |

References: `libs/commanding/src/http/axum/routes.rs` (`command_routes`) and `libs/commanding/src/http/poem/routes.rs` (`CommandingApi`). Factories and endpoint objects accept `impl Into<Async…Service>` and delegate to the existing adapters. Commanding has no Poem example; the doc comment on `CommandingApi` shows the intended mounting.

The submission path changes for apps that adopt the reusable inbox routes. Mounted under `/inbox`, the reusable endpoint serves `POST /inbox/submit`, while today's examples use `POST /inbox`. This is intended. Applications that need the old URL keep using `InboxAdapter` with their own routes.

Preserve:

- Batch validation and response bodies.
- Inbox's `202 Accepted` submission response. A duplicate message ID is accepted and leaves the stored row unchanged.
- Error sanitization and existing HTTP status codes, including outbox's 503 when no publisher is configured.
- One set of Poem schema names across crates. `mulac_http` keeps ownership of the Poem `RunRequest` and `RunResponse` schemas. `test_apps/http_adapters/tests/poem_schema.rs` guards this.

Applications own mount prefixes, authentication, authorization, and middleware. Do not mount the new endpoints in existing applications.

Examples:

| Example                                                                                                                                                                                    | Change                                                             |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------ |
| `eventing/examples/axum_eventing.rs`                                                                                                                                                       | Use `event_routes`                                                 |
| `inbox/examples/axum_inbox.rs`                                                                                                                                                             | Use `inbox_routes`. The submission URL becomes `/api/inbox/submit` |
| `outbox/examples/axum_outbox.rs`                                                                                                                                                           | Use `outbox_routes`                                                |
| `eventing/examples/axum_eventing_openapi.rs`, `inbox/examples/axum_openapi.rs`, `inbox/examples/poem_inbox.rs`, `outbox/examples/axum_outbox_openapi.rs`, `outbox/examples/poem_outbox.rs` | Keep their application-owned paths. Update only construction code  |

`poem_inbox.rs` defines its own local `InboxApi`. Rename it if the example ever imports the library type.

## 5. Implementation sequence and acceptance tests

### Stages

Each stage is one reviewable change. It must compile, pass its done criteria, and update every affected caller: libs, kernel, test apps, and examples.

1. **Eventing.**
   - Add ADR-012, which supersedes ADR-007 and amends ADR-008 (§1). Mark both in their headers and in `docs/adr/README.md`.
   - Reorganize modules, unify the repository, remove the wrappers, and make the consumer and service generic.
   - Update the kernel and the commanding integration.
   - Move the existing sweep onto `DieselPgEventRepository` unchanged.
2. **Inbox.**
   - Apply the same structure and interfaces, including the kernel's inbox re-exports and `InboxService::from_postgres`.
   - Keep transport acknowledgment and inbox-to-command behavior.
   - Move the existing sweep unchanged.
3. **Outbox.**
   - Apply the same structure and interfaces.
   - Keep publication validation, publisher confirmations, and `dead()`.
   - Move the existing sweep unchanged.
4. **Sweeps.**
   - Add `ReservationTimeoutPolicy` and inbox's `with_max_attempts`.
   - Replace all three sweeps as described in §3.
   - Add the PostgreSQL tests.
5. **HTTP.**
   - Add the reusable endpoints.
   - Migrate the examples listed in §4.
   - Extend `test_apps/http_adapters`.
6. **Documentation.**
   - Update the `diesel` row in ADR-006, `docs/developer-guidelines.md`, `docs/implementation-guide.md`, `docs/eventing-spec.md`, `docs/inbox-spec.md`, `docs/outbox-spec.md`, `docs/command-handling-spec.md`, and `kernel/SPEC.md`.
   - Leave historical plans alone (`docs/*-implementation-plan.md`, `docs/outbox-implementation-checklist.md`).
   - Add the rollout notes to `CHANGELOG.md`.

### Done criteria

| Stages | Must pass                                                                                                                                                                                                                                                       |
| ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1–5    | `make ci`; `cargo test --no-default-features` for each crate changed; test apps build (`cargo build --manifest-path test_apps/todo/Cargo.toml`, same for `twitter`)                                                                                             |
| 2, 3   | Also: `cargo check --no-default-features --features amqp` and `--features amqp,axum,poem` for the crate. Make these checks a permanent part of `make ci`. `scripts/check_http_features.py` currently forbids `lapin`, so put the checks next to it or adjust it |
| 4      | Also: the PostgreSQL tests below                                                                                                                                                                                                                                |
| 5      | Also: `test_apps/http_adapters` covers the new endpoints (included in `make ci`)                                                                                                                                                                                |
| 6      | Current documentation must not present removed APIs as supported; historical, ADR, changelog, and migration references may name them explicitly                                                                                                                 |
| Final  | All of the above, plus `make test-apps-up && make test-apps-test` (Todo and Twitter against their Docker databases)                                                                                                                                             |

### Unit and API tests

- Each new repository satisfies all four of its storage-port bounds.
- Consumers and synchronous services accept a repository type that contains non-`'static` borrows.
- The simplified recorders keep storage delegation, identity, and validation.
- Metadata and envelope conversions work without the `diesel` feature.
- Timeout policies cover attempts below, exactly at, and above both default and custom limits.
- Outbox's policy keeps its delay cap and its rule for scheduling `Dead` entries.
- Reusable endpoints cover:
  - valid and invalid bodies
  - content types
  - batch results and processing errors
  - sanitized task failures
  - wrong-method rejection (405)
  - custom mount prefixes
  - Poem schemas
- Inbox submission covers duplicate IDs, generated IDs, validation, and storage failures.
- Existing tests for custom adapters keep passing.

### PostgreSQL regression tests

Test setup:

- **Never touch tables an application uses.**
- **Single-connection tests** use temporary tables inside a test transaction, as commanding's `bulk_sweep_applies_policy_and_preserves_unselected_rows` does. This also makes the exactly-at-cutoff case deterministic, because the fixture's `now()` equals the sweep's `transaction_timestamp()`.
- **Multi-connection tests** each create a uniquely named schema and drop it afterwards. Fixtures must match the current effective schema, including `extra_info`. The Twitter base infrastructure migration alone is insufficient; include subsequent schema additions. `docs/outbox_entries.sql` is another reference. Configure every connection to use its test schema, with no fallback to application tables.
- **Environment variables.** New eventing and inbox tests read `DATABASE_URL_TEST`. Outbox tests keep `OUTBOX_TEST_DATABASE_URL`. Existing destructive outbox tests may retain their setup only when run against a dedicated disposable database containing no application data.
- **Running.** The command below also runs legacy outbox tests that drop tables. Never point either URL at an application database. To run only isolated new tests, use their explicit test-target or name filters rather than the unfiltered command. Mark each test `#[ignore = "requires …"]`, then run them with:

  ```sh
  # Both URLs must point to dedicated disposable test databases.
  DATABASE_URL_TEST=… OUTBOX_TEST_DATABASE_URL=… \
    cargo test --manifest-path libs/Cargo.toml --all-features -- --ignored
  ```

The suite must cover every behavior below; test grouping and synchronization mechanisms are implementation choices. Concurrency tests must establish actual transaction contention, cover both completion/sweep orderings, and terminate within a bounded timeout; scheduling two tasks or relying on sleeps alone is not evidence of contention.

- Old reservations are swept. Recent rows, rows with a null `reserved_at`, completed rows, and rows reserved exactly at the cutoff are untouched.
- Exhausted rows become `Dead`, with both the default and a custom limit.
- Attempt counts do not change.
- Reservation fields are cleared, and existing errors are kept.
- `updated_at` and `scheduled_at` come from the same database time.
- Outbox `Dead` rows keep their previous `scheduled_at`.
- A second sweep returns zero and appends no error.
- A forced update failure rolls back every row.
- **Concurrent sweeps.** Two sweeps start together over the same N stale rows. Their counts sum to N, and each row has exactly one timeout error appended.
- **Completion racing a sweep.** `completed()` and a sweep run concurrently on the same reservation. Exactly one wins:
  - The row is `Completed` and the sweep did not count it.
  - Or the sweep counted it, the row is `Failed` or `Dead`, and `completed()` returned the queue’s existing ownership-loss error: eventing `MissingReservation`, inbox `ReservationNotOwned`, or outbox `Reservation(...)`. Do not normalize these errors as part of this work.

  The sweep never changes a `Completed` row.

### Rollout notes (for `CHANGELOG.md`)

- No schema migration and no data cleanup are included.
- **Removed APIs:** list each one with the type or constructor that replaces it.
- **Exhausted reservations now become `Dead`.** Eventing and inbox sweeps used to leave them `Failed`.
- **All sweeps use database time.**
- **Bulk sweeps lock all candidates at once**, as commanding's do.
- **Stranded rows stay as they are.** These are eventing and inbox rows that an old sweep set to `Failed` with `attempts >= max_attempts`. Reservation skips them and the new sweep never selects them, so they stay `Failed` until an operator acts. Include a query that finds them.

## Implementation verification

- `make ci` passed, including reusable endpoint tests, feature isolation, and
  permanent AMQP-only / AMQP+Axum+Poem compilation checks. The final run used
  a workspace `TMPDIR` and four build jobs because native compilation hit the
  machine's `/tmp` quota; no dependency features were changed to bypass checks.
- All three migrated crates passed `cargo test --no-default-features`; the
  `api-testing` configurations also passed, including borrowed-repository tests.
- Ten isolated PostgreSQL regression tests passed across the three queues,
  covering policy outcomes, strict cutoff, rollback, duplicate inbox identity,
  concurrent sweeps, and both completion/sweep orderings. Fixtures use temporary
  tables or unique schemas and enforce bounded database waits.
- Eight legacy outbox PostgreSQL tests passed on a dedicated disposable database.
- `make test-apps-up` and `make test-apps-test` passed for Todo and Twitter.
  Both application builds are also checked separately.
- Explicit failure/dead behavior, transport acknowledgments, schemas, serialized
  contracts, and existing custom-adapter routes remain unchanged. No production
  migration, stranded-row repair, or automatic sweep worker was added.
