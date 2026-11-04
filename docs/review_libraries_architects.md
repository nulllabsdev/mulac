# Libraries architecture review

Review date: 2026-11-04. Code reviewed: `ad0b769`. `aeaed28`, the latest commit when this was written,
changes only documentation, and line references were checked there.
Written by Claude Code (`claude-opus-5-5`).

## Conclusion

**The structure is sound, but the libraries don't yet deliver the reliability they promise. I wouldn't
publish them as a reliability foundation until findings 1, 2 and 4 are addressed.**

There are three main gaps:

1. **Handoffs aren't atomic, and that can lose events.** One failed event insert can leave a business
   change committed, zero events recorded, and the command marked `Completed`. This was reproduced against
   real PostgreSQL, using the same logic as the `twitter` app's `tweet_like` handler.
2. **Giving up has no real state and no way back.** Retries stop after about 7 minutes. Rows then stay
   `Failed`, looking almost the same as rows that will retry, and no API can requeue them.
3. **Half the pipeline is unproven.** The kernel assembles 2 of the 4 queues, neither app uses `mulac-inbox`
   or `mulac-outbox`, and CI never runs their SQL.

These are design gaps, not regressions. The recent work improved things, and the `ad0b769` reservation fix
passes its tests on real PostgreSQL.

There are 20 findings: 1 critical, 6 high, 10 medium and 3 low. No code was changed by this review.

**Start with the decision in finding 1,** because it changes port signatures and affects most later work.

## How this review was done

The brief was a team of two software architects. One reviewer applied two lenses, and no separate agents ran,
so this isn't two independent opinions.

- **Runtime lens:** delivery guarantees, failure, recovery and operations.
- **Structure lens:** boundaries, APIs, dependencies and how the code will evolve.

The earlier reviews looked mainly for code defects. This one asks whether the design delivers what the
libraries promise. AMQP is out of scope, as the project owner decided earlier, so there are no AMQP findings.

Each finding states its evidence:

- **Probe:** reproduced with the real consumers, repositories and kernel registries against a throwaway
  PostgreSQL 16 database, using the schema from `test_apps/twitter/migrations`. Probes were not kept, so these
  scenarios should become regression tests when the issues are fixed.
- **Test run:** existing tests, run against that database.
- **Measured:** counted or compared across the repository.
- **Read:** concluded from reading the code.

## Where the handoffs are

```text
Inbox row
   │ (1)
   ▼
Command row
   │ (2)
   ▼
Handler's business write
   │ (3)
   ▼
Event row
   │ (4)
   ▼
Outbox row
   │ (5)
   ▼
Broker
```

Each arrow is a separate database transaction today. Handoffs 1 to 4 usually touch the same PostgreSQL
database, so they could be covered by one transaction per item. Only handoff 5 truly can't be.

## Summary

| #   | Severity | Lens      | Finding                                                      | Evidence        |
| --- | -------- | --------- | ------------------------------------------------------------ | --------------- |
| 1   | Critical | Runtime   | Events can be lost silently after one failed insert          | Probe           |
| 2   | High     | Runtime   | `Failed` means two things, and retries stop after 7 minutes  | Probe           |
| 3   | High     | Runtime   | One lease covers a whole batch                               | Probe           |
| 4   | High     | Runtime   | Crash recovery isn't wired up                                | Read, probe     |
| 5   | High     | Structure | The full pipeline isn't assembled or used                    | Measured, probe |
| 6   | High     | Structure | CI doesn't test the guarantees                               | Read, test run  |
| 7   | High     | Structure | The queue core is copied four times and has started to drift | Measured, probe |
| 8   | Medium   | Runtime   | Rows out of attempts slow every reservation                  | Probe           |
| 9   | Medium   | Runtime   | Rolling deploys can lose or burn work                        | Probe           |
| 10  | Medium   | Runtime   | Failures are hard to see                                     | Probe, measured |
| 11  | Medium   | Runtime   | `drain_rounds` ties requests to the queue                    | Probe           |
| 12  | Medium   | Runtime   | External input chooses the internal command                  | Probe           |
| 13  | Medium   | Structure | The schema has no owner                                      | Measured        |
| 14  | Medium   | Structure | Ports aren't uniform or implementable by others              | Measured        |
| 15  | Medium   | Structure | The layer rules aren't followed                              | Measured        |
| 16  | Medium   | Structure | Docs and code disagree                                       | Read, probe     |
| 17  | Medium   | Runtime   | One malformed row can block a whole sweep                    | Probe           |
| 18  | Low      | Structure | Abstractions that no longer do anything                      | Read, measured  |
| 19  | Low      | Structure | Gateways couple crates and hide different guarantees         | Read            |
| 20  | Low      | Both      | Smaller items                                                | Read            |

## Critical

### 1. Events can be lost silently after one failed insert

**Where:** `libs/commanding/src/execution/command_dispatcher.rs:100-106`, `test_apps/twitter/src/tweet_like.rs:85-88`,
`test_apps/todo/src/task_create.rs:85`

**What happens:** the handler commits its business write on its own connection. Only then are its events inserted,
each on another connection. If an insert fails, the command is retried and the handler runs again over work that
already happened. Both common handler styles lose the event:

| Handler style                          | What the retry does                  | Command ends as      | Business rows | Events |
| -------------------------------------- | ------------------------------------ | -------------------- | ------------- | ------ |
| Idempotent, like `tweet_like.rs:85-88` | Sees the work is done, emits nothing | `Completed`          | 1             | 0      |
| Plain insert, like `task_create.rs:85` | Hits a unique-key violation          | `Failed`, attempts 6 | 1             | 0      |

**Why it matters:** in the first case, every status reports success while subscribers and the outbox never hear
about the change. The contract requires idempotent handlers (`docs/contracts.md:102`), and the natural way to write
one causes this. The non-atomic handoff is documented (`README.md:43`, `docs/contracts.md:103`), but this consequence
isn't. A process killed between the two writes should reach the same retry through the sweeper, so a restart could
trigger it with no database error at all (Read, not probed).

**A shared pool isn't enough:** `docs/pluggable-db-backends.md:10` blames separate pools, and line 85 expects one
pool to allow a shared transaction. `todo` now runs one Diesel pool for both and still can't share one, because no
port accepts a connection.

**Recommendation:** decide this first. Add a transactional path for handlers whose state lives in the same PostgreSQL
database:

```rust
pub trait TransactionalCommandHandler<C, E>: Send + Sync {
    fn execute(&self, conn: &mut PgConnection, command: C) -> Result<Vec<E>, CommandError>;
}
```

The consumer would open the transaction, run the handler, insert the events and mark the command completed, all on
that connection. Handoffs 1 and 4 in the diagram can work the same way.

- **What it fixes:** a failure can no longer separate the business write from its events, and a failed `completed`
  can no longer cause a replay. For this path, it also removes most of the need for the stable retry IDs in `TODO.md`,
  so make this decision before designing those.
- **What it costs:** this handler type depends on the backend, and each item holds a connection while its handler runs,
  so slow network calls don't belong inside it.
- **What stays the same:** the plain `CommandHandlerPort` remains for handlers with no local database. External side
  effects and broker publication stay at-least-once.

**Do this now either way:** state in the contract that a handler finding its work already done must return the same
events, and fix `tweet_like` so the reference app doesn't teach the bug.

**Evidence:** Probe, using real `CommandConsumer` and repositories, with the first event insert failing once.

## High

### 2. `Failed` means two things, and retries stop after 7 minutes

**Where:** `libs/*/src/infra_diesel/reservation.rs` (`write_failure`), `libs/outbox/src/application/ports.rs:72-78`,
`libs/outbox/src/infra_diesel/repository.rs:191-194`

**What happens:** six attempts with linear backoff give a short retry window. "Exhausted" below means a row has used
all its attempts.

| Queue                        | Delays between attempts (seconds) | Total       |
| ---------------------------- | --------------------------------- | ----------- |
| Commanding, eventing, outbox | 30, 60, 90, 120, 120              | 7 minutes   |
| Inbox                        | 30, 60, 90, 120, 150              | 7.5 minutes |

A downstream outage longer than that exhausts every row it touches. Attempts are the only budget, so a broker outage
spends them the same way a bug would.

**Why it matters:**

- **The state is ambiguous.** Since `ad0b769`, exhausted rows stay `Failed`, so `Failed` means both "will retry" and
  "gave up". Only `attempts` separates them, and the limit isn't stored on the row. It's a value each caller passes
  when reserving, so a row's meaning depends on who's asking.
- **There's no way back.** Nothing can requeue a row, so recovery means hand-written SQL.
- **`dead()` can't help operators.** Outbox's "administrative" transition needs a live reservation token and `Reserved`
  status, which an exhausted row doesn't have.
- **All failures are treated alike.** A missing handler, an unparsable payload and a broker outage all spend the same
  budget.

**Recommendation:**

- Give exhausted rows their own state, while keeping the `ad0b769` rule that nothing is discarded automatically. This
  reverses part of a deliberate choice, so it's listed under [decisions](#decisions-for-the-project-owner).
- Move retry rules into one configurable policy, with a longer default and jitter.
- Add an operator port to list and requeue exhausted rows.
- Separate permanent from transient failures.

**Evidence:** Probe, walking one row through real `reserve` and `failed` on all four queues. The window was computed
from the schedule each `failed` wrote, with rows forced due between attempts.

### 3. One lease covers a whole batch

**Where:** `libs/commanding/src/infra_diesel/repository.rs:73-75`, `libs/commanding/src/execution/command_consumer.rs:32-34`,
`libs/commanding/src/http/contract.rs:34`

**What happens:** reservation sets `reserved_at` and spends an attempt for every row at once. Rows then run one at a
time, with no lease renewal and no check that the row is still owned. Batches can hold 1000 rows, so at 1 second per
item the last one starts about 16 minutes after its reservation. A 5-minute sweep timeout would then reclaim rows that
are simply waiting their turn.

- **Double execution, reproduced.** A slow batch of 4 commands, with a sweep mid-batch and a second consumer, ran
  commands 2, 3 and 4 twice, **at the same moment**. Timings were compressed: 1 second per handler, a 500 ms timeout,
  and rows forced due so the second consumer didn't wait out the 30-second backoff. The first worker only found out afterwards,
  through `missing reservation` errors.
- **Attempts spent on rows that never ran, reproduced.** Six rounds of "reserve, then crash, then sweep" left 3 rows
  exhausted with **zero handler runs**. A healthy restart returned `Ok(0)`. So a row that crashes the process can use up
  the attempts of everything reserved alongside it.

**Why it matters:** `docs/contracts.md:97` tells callers a reserved entry won't be processed concurrently. That's
technically true once the sweep has released it, but the effect is what the promise rules out.

**Recommendation:** make one call just before each item that renews the lease, confirms ownership and spends the
attempt, instead of spending it at reservation. For example:
`UPDATE ... SET reserved_at = now(), attempts = attempts + 1 WHERE id = ? AND reservation_id = ?`.
That one change addresses both problems for about one extra query per item, and a row that never starts spends nothing.
A single long-running item still needs a timeout longer than itself. Until then, document that the sweep timeout must
exceed the duration of the **batch**, not the item.

**Evidence:** Probe, using real consumers, repository and sweep.

### 4. Crash recovery isn't wired up

**Where:** `kernel/src/lib.rs:5-46`, `kernel/src/lib.rs:530-533`, `kernel/src/workers.rs:15-19`

**What happens:** nothing outside tests builds a sweeper, and the kernel neither runs one nor exports the types. A
process killed mid-batch leaves rows `Reserved`, and reservation only selects `Received` and `Failed`, so those rows
never run again.

Shutdown has the same gap. `PersistentKernelHandle::wait()` cancels the token and returns without joining workers, and a
batch already running can't be stopped between items.

**Why it matters:** the docs say this is the application's job (`kernel/SPEC.md:82`), but both reference apps skip it, and
a kernel user would need a direct dependency to do it. The default assembly therefore has no crash recovery.

**Recommendation:** add a sweep worker beside `run_command_worker`, run it by default in persistent mode with a
configurable timeout, and export its types. Let consumers stop between items, and make `wait()` join the workers.

**Evidence:** Read, plus the [finding 3](#3-one-lease-covers-a-whole-batch) probe showing a sweep is what releases such rows.

### 5. The full pipeline isn't assembled or used

**Where:** `kernel/src/lib.rs:294-297`, `kernel/SPEC.md:30-38`, `docs/architecture-spec.md:35-46`,
`test_apps/twitter/src/inbox.rs:227`, `test_apps/todo/src/assembly/infra_diesel.rs:59`

**What happens:** the architecture spec's "full flow" runs inbox to commands to events to outbox. In the code:

- The kernel assembles only commands and events. `start_persistent` rejects an inbox store, and `outbox_subscriber` is a
  public builder method that always fails.
- The libraries have no event-to-outbox adapter. `NewEventMetadata` has no routing key, and `NewOutboxMetadata` requires one.
- Of the mulac crates, `todo` and `twitter` depend only on the kernel, and they use their own `inbox_messages` and `outbox_messages`
  tables. Outside its own crate, `InboxConsumer` appears only as a kernel re-export, and no app uses `OutboxConsumer` or `OutboxRecorder`.

**Why it matters:** half the library has never been run the way it's described, except in its own tests.

**The good news:** the parts do fit. A hand-assembled pipeline with real repositories passed a message through all four
stages, published it once and preserved its correlation ID. The only missing piece was a subscriber of about 25 lines.

**Recommendation:** ship that adapter with a routing policy port, let the kernel assemble all four queues, and move one
reference app onto the real libraries. Remove `outbox_subscriber` until it works.

**Evidence:** Measured with `grep` across `kernel` and `test_apps`, plus a Probe of the hand-assembled flow.

### 6. CI doesn't test the guarantees

**Where:** `.github/workflows/ci.yml:23-32`, `Makefile:10-17`

**What happens:**

- 45 tests are `#[ignore]`d: 44 database tests and 1 broker test. `ci.yml` has no database and doesn't run them.
- Inbox and outbox SQL therefore never runs in CI. `test-app-todo.yml` does have PostgreSQL, but `todo` doesn't use those crates,
  so it only exercises commanding and eventing.
- The "Format check" step runs `make fmt`, which calls `cargo fmt` without `--check`. It rewrites files and can't fail on
  formatting.
- `ci.yml` doesn't run `make ci`, so clippy and `check_http_features.py` don't run there.
- A comment in `ci.yml` refers to `test-app-twitter.yml`, which doesn't exist.

**Why it matters:** reservation, ownership checks and sweeps are the core of the promise, and they're the least tested
code in CI.

**Recommendation:** add a PostgreSQL service to `ci.yml`, run the database tests, use `make ci`, and make formatting use
`--check`. This is small, it comes first, and it makes every later fix verifiable. The older outbox tests need
`--test-threads=1` until their race is fixed.

**Evidence:** Read. The tests themselves are fine when run: all 44 pass, with the 8 older outbox tests run on one thread. See
[status of earlier findings](#status-of-earlier-findings).

### 7. The queue core is copied four times and has started to drift

**Where:** `libs/*/src/execution/item_boundary.rs`, `libs/*/src/infra_diesel/reservation.rs`,
`libs/*/src/infra_diesel/models.rs`, `libs/*/src/http/`

**What happens:** ADR-014 gave the four crates the same shape, which makes the duplication easy to measure. Lines below
differ from commanding after renaming the queue's name:

| File (lines in commanding)          | Eventing | Inbox | Outbox |
| ----------------------------------- | -------- | ----- | ------ |
| `item_boundary.rs` (81)             | 0        | 0     | 0      |
| `infra_diesel/reservation.rs` (265) | 8        | 16    | 24     |
| `infra_diesel/models.rs` (82)       | 0        | 4     | 5      |
| `domain/mod.rs` (150)               | 4        | 25    | 17     |
| `http/error.rs` (90)                | 1        | 32    | 19     |
| `http/poem/adapter.rs` (83)         | 1        | 44    | 29     |

`ad0b769` shows the cost. One reliability fix added 4 copies of `item_boundary.rs`, 4 of `reservation.rs` (1,110 lines) and 4
of `reservation_isolation.rs` (1,434 lines).

Behaviour has already drifted, with no reason recorded beyond "existing timing is retained":

| Queue                | `failed()` delay | Sweep delay | Exhausted row's schedule |
| -------------------- | ---------------- | ----------- | ------------------------ |
| Commanding, eventing | Capped at 120 s  | Uncapped    | Moves forward            |
| Inbox                | Uncapped         | Uncapped    | Moves forward            |
| Outbox               | Capped at 120 s  | Capped      | Kept as it was           |

**Why it matters:** every lifecycle fix costs four times as much and can drift four ways. The sqlx plan in
`docs/pluggable-db-backends.md` would double the repositories. HTTP multiplies too, with 4 queues on 2 frameworks.

**Recommendation:** extract a shared crate, such as `mulac_queue`, holding the item boundary, status codes, `ExtraInfo`
handling, retry policy, reservation SQL parameterized by table, and the shared run request. Each queue keeps its envelopes,
decoding and consumer. Keep separate crates, and don't merge them. Do this **after** finding 1, so the ports only change once.

**Evidence:** Measured. The `failed()` column was confirmed by Probe, and the sweep column comes from the code and
`docs/item-failure-isolation.md`.

## Medium

### 8. Rows out of attempts slow every reservation

**Where:** `libs/commanding/src/infra_diesel/repository.rs:64-67`, and the same query in the other queues

**What happens:** `attempts < $2` isn't in the `(status, scheduled_at)` index, so PostgreSQL visits every exhausted `Failed`
row on every reservation. The same rows as `Dead` cost nothing. Real `reserve(10)` on the commanding table, with 400 eligible rows:

| Older rows in the table      | Median  | p95     | Plan       | Buffers |
| ---------------------------- | ------- | ------- | ---------- | ------- |
| None                         | 2.0 ms  | 2.7 ms  | Sequential | 28      |
| 10,000 exhausted `Failed`    | 3.0 ms  | 3.2 ms  | Sequential | 220     |
| 100,000 exhausted `Failed`   | 8.7 ms  | 9.9 ms  | Sequential | 1,951   |
| 200,000 exhausted `Failed`   | 16.4 ms | 18.4 ms | Sequential | 3,874   |
| 1,000,000 exhausted `Failed` | 87.6 ms | 89.6 ms | Sequential | 19,259  |
| 200,000 `Dead`               | 1.5 ms  | 1.8 ms  | Index      | 37      |
| 1,000,000 `Dead`             | 1.6 ms  | 2.3 ms  | Index      | 35      |
| 1,000,000 `Completed`        | 1.7 ms  | 1.9 ms  | Index      | 35      |

**How serious this is:** the growth is linear, about 55 times slower at a million rows, but 88 ms isn't an emergency by itself,
so this is Medium rather than the High first expected. These rows only pile up, and an outage (finding 2) can create many at once.
This was a clean, cached table with small payloads and no concurrent load, so real numbers will differ. A table that's mostly
`Completed` could also get a different plan, which wasn't tested.

**Retention is a separate concern:** `Completed` rows **don't** slow reservation. They only grow storage. `Archive` has no writer,
and no retention path exists, so that still needs a decision.

**Recommendation:** a separate exhausted state (finding 2) fixes this with the existing index. A partial index can't do it, because
the limit is a value the caller passes. Add a documented retention path.

**Evidence:** Probe, using `EXPLAIN (ANALYZE, BUFFERS)` for the plan and buffers, and 30 real `reserve` calls per case for timing.

### 9. Rolling deploys can lose or burn work

**Where:** `kernel/src/event_subscriber_registry.rs:32-40`, `kernel/src/command_handler_registry.rs:24-27`, `docs/contracts.md:136`

**What happens:** reservation can't filter by type, so any worker takes any row. While old and new binaries run side by side:

- **An old binary loses new events.** With no subscriber for the type, it returns `Ok`, and the event becomes `Completed` after one
  attempt. The new binary's subscriber never sees it. The contract calls zero subscribers valid, which is reasonable until deploys overlap.
- **An old binary burns new commands.** `HandlerNotFound` counts as an ordinary failure. After 6 attempts the command is
  exhausted, and the new binary will never run it.

Stored payloads also have no version, so a row written by old code that new code can't parse fails the same way.

**Recommendation:** have reservation select only the types a worker can handle, since the registries already know them. Treat an
unknown type as "not mine", which spends no attempt. Decide on a payload version field. If deploys never overlap, record that as a
requirement instead.

**Evidence:** Probe with the real kernel registries and repositories.

### 10. Failures are hard to see

**Where:** `libs/commanding/src/infra_diesel/reservation.rs:78-93`, `libs/commanding/src/execution/command_consumer.rs:62`,
`libs/commanding/src/execution/command_consumer.rs:23`

**What happens:**

- **Queue crates don't log.** `tracing` appears only in `mulac_http` and `kernel/src/workers.rs`, so what's logged depends on who
  called the consumer.
- **Quarantined rows make no sound.** A malformed row became `Failed` while `consume` returned `Ok(0)`, with no log and no error.
- **`Ok(n)` hides what happened.** `Ok(0)` doesn't mean the queue is empty, and one failure turns the HTTP response into
  `processed: null`, which hides how many items succeeded.
- **Panics are mislabelled.** A handler panic came back as `Storage("... stage processing: panicked")`. Eventing does the same, inbox
  uses `Recording` and outbox uses `Publish`, so alerts keyed on the error type would mislead.
- **Correlation IDs never reach logs,** even though they travel correctly with each message.
- **No metrics or standard queries** exist for queue depth, oldest row, or exhausted rows.

**Recommendation:** return a report from consumers, for example `{ reserved, completed, failed, quarantined, errors }`. Add `tracing`
spans with item, reservation and correlation IDs. Add a `Panicked` error variant, and document the queries for queue health.

**Evidence:** Probe for quarantine and panic mapping, Measured for `tracing`, Read for the rest.

### 11. `drain_rounds` ties requests to the queue

**Where:** `kernel/src/lib.rs:477-497`, `test_apps/todo/src/task_create.rs:160-163`

**What happens:** after recording a command, `dispatch_command` drains the queues on the caller's thread, processing **anyone's** due
commands and events, up to 64 of each per round. Apps use this so they can read their own write straight afterwards.

In the probe, another caller's failing command was already queued. The second caller's command was recorded and completed, yet the
caller received `Err(Command(HandlerExecution("someone else's command failed")))`.

**Why it matters:** a caller told its accepted command failed may retry it under a new ID, and that's a duplicate. Request latency
also depends on unrelated work. `todo` calls this blocking method directly from an async handler, while `twitter` wraps it in
`run_blocking`, so the API makes the mistake easy.

**Recommendation:** separate "recorded" from "processed" in the return type, and never return another caller's error. If apps need
read-your-writes, execute only the command just dispatched.

**Evidence:** Probe with the real kernel in persistent mode.

### 12. External input chooses the internal command

**Where:** `libs/inbox/src/application/conversions.rs:52-55`, `libs/inbox/src/http/contract.rs:48-49`

**What happens:** an inbound message's `routing_key` becomes `command_type` with no allowlist or translation. In the probe,
`routing_key: "AnyInternalCommand"` was stored as exactly that command type, and the kernel's registry routes on that field.

**Why it matters:** anyone who can reach `/submit` or publish to the queue can invoke any registered handler, including internal-only
ones, with any payload. Host authentication is the only barrier. It also ties external message names to internal type names, so
renaming a command breaks producers.

**Recommendation:** add a required mapping step at the inbox boundary, a port or an explicit table from external key to command. Hosts
that really want pass-through can opt in.

**Evidence:** Probe.

### 13. The schema has no owner

**Where:** `libs/*/src/infra_diesel/schema.rs`, `test_apps/*/migrations/`, `docs/outbox_entries.sql`,
`libs/inbox/tests/reservation_isolation.rs:45-52`

**What happens:** the libraries define tables in Rust but ship no migrations, so every host copies the DDL by hand. The copies have
already diverged. `command_entries` has `(status, scheduled_at)` in `twitter`, and `todo` adds `(reservation_id)` under different index
names. Test fixtures create yet another shape, with `command_type` and `event_type` on the same table, so tests can't catch a mismatch
with real migrations. `docs/pluggable-db-backends.md:79` already notes this and proposes a fix that was never made.

**Why it matters:** finding 8 shows that indexes decide how this performs, and a column added by a library upgrade has no delivery path.

**Recommendation:** ship versioned migrations with the libraries, offer an optional `migrate()`, point test fixtures at the same SQL,
and check the schema at startup.

**Evidence:** Measured.

### 14. Ports aren't uniform or implementable by others

**Where:** `libs/*/src/application/ports.rs`, `libs/inbox/src/lib.rs:24-42`, `libs/commanding/src/application/mod.rs:37-40`

**What happens:** ADR-014 aligned the file layout, but the same concept still has four spellings. ADR-014 kept some of these on purpose.

| Concept               | Commanding           | Eventing             | Inbox                 | Outbox               |
| --------------------- | -------------------- | -------------------- | --------------------- | -------------------- |
| Store                 | `record(&envelope)`  | `record(&envelope)`  | `store(envelope)`     | `record(&entry)`     |
| Consume               | `consume`            | `consume`            | `process`             | `publish_batch`      |
| Sweeper               | `CommandSweeper`     | `EventSweeper`       | `ReservationSweeper`  | `ReservationSweeper` |
| Sweep count           | `u64`                | `u64`                | `usize`               | `u64`                |
| Reserve failure       | `Reservation`        | `Reservation`        | `Storage`             | `Storage`            |
| Lost reservation      | `MissingReservation` | `MissingReservation` | `ReservationNotOwned` | `Reservation(text)`  |
| `reservation_id` read | `Uuid`               | `Uuid`               | `Option<Uuid>`        | `Option<Uuid>`       |

- **Custom backends:** the README invites them, but inbox can't have one, because `InboxMessage` isn't exported. This is earlier
  finding 12, still open. `ad0b769` also raised the bar for all of them: reservation must now decode and quarantine atomically, described
  only in doc comments, with no test suite a backend could run.
- **Invalid states are representable:** `metadata: Option<...>` is required in practice, and is checked at runtime in several places:
  `libs/commanding/src/application/command_gateway.rs:27`, `libs/eventing/src/application/gateway.rs:26` and `:38`, and
  `libs/commanding/src/infra_diesel/conversions.rs:20`.

**Recommendation:** unify the names and return types before publishing, since this only gets harder later. Add a backend conformance
suite, a generic test function any implementation can run, to replace the copied `reservation_isolation.rs` files. Make metadata required
on new envelopes.

**Evidence:** Measured.

### 15. The layer rules aren't followed

**Where:** `docs/adr/001-hexagonal-architecture.md:15-27`, `libs/commanding/src/application/ports.rs:15`,
`libs/commanding/src/application/command_gateway.rs:8`, `libs/commanding/src/execution/command_consumer.rs:5-6`

**What happens:** ADR-001 says dependencies point inward, in layers named `features` under `assembly/`. Neither name exists now, and:

- `application` and `execution` depend on each other in all four crates. Ports use the reservation spec from `execution`, gateways use
  dispatchers from `execution`, and consumers use ports from `application`.
- `domain` uses `chrono` and `thiserror`, where ADR-001 allows no external crates.
- The domain layer holds status enums, but the real lifecycle rules live in SQL inside `infra_diesel`, including the attempt limit,
  backoff and allowed transitions. A second backend would have to rediscover them.

**Why it matters:** the part of hexagonal design that counts does hold, since Diesel stays out of `domain`, `application` and `execution`.
But the most important business rules sit in the layer that's meant to be replaceable.

**Recommendation:** move reservation specs into `application` and gateways into `execution` to break the cycle. Put the state machine and
retry policy in `domain`. Update ADR-001 to describe what's actually wanted.

**Evidence:** Measured with `grep`.

### 16. Docs and code disagree

**What happens:** several statements in the contracts don't match the code.

| Document says                                                                               | Code does                                                                                    |
| ------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Inbox dedupe has a configurable 24-hour window, per source (`docs/contracts.md:50`, `:57`)  | Permanent and global: `on_conflict(id)` (`libs/inbox/src/infra_diesel/repository.rs:33`)     |
| Inbound messages require a stable ID and a source (`docs/contracts.md:38`)                  | All `Option`, and a missing ID is generated (`libs/inbox/src/application/conversions.rs:32`) |
| Commands require correlation ID, causation ID and created-at (`docs/contracts.md:74`)       | The first two are `Option`, and there's no created-at field                                  |
| Events require a routing key, used by the outbox (`docs/contracts.md:112`, `:148`)          | `NewEventMetadata` has no routing key                                                        |
| Exhausted rows become `Dead` (`docs/implementation-guide.md:134-135`, `CHANGELOG.md:97-99`) | They stay `Failed`, and `CHANGELOG.md:30-32` says so in the same section                     |

The dedupe mismatch can lose data. In the probe, `billing` and `shipping` sent the same `message_id`. Both calls returned `Ok`, one row was
stored, and the `shipping` message was dropped, where the contract promises it would be kept.

Also:

- **"Failed, not Dead" has no ADR,** though it reverses earlier behaviour.
- **The changelog stops at 0.1.0,** while tags run up to `0.3.0` and the workspace version is `0.3.0` with many breaking changes unreleased.
- **`docs/pluggable-db-backends.md` is out of date.** It describes `todo` on sqlx and code under `assembly/`.

**Recommendation:** treat `contracts.md` as the promise, and change either it or the code for each row above. Add the missing ADR. Fix the
changelog before the next release.

**Evidence:** Read, plus a Probe for dedupe.

### 17. One malformed row can block a whole sweep

**Where:** `libs/commanding/src/infra_diesel/repository.rs:247-253`, and the same SQL in the other three repositories

**What happens:** `ad0b769` made reservation and failure recording tolerate malformed `extra_info`, but the sweep still calls `jsonb_set`
directly. In all four queues, one `Reserved` row with `extra_info` of `[1]`, JSON `null` or `42` failed the whole sweep, and the healthy
stale row beside it stayed `Reserved` too.

**How likely this is:** new reservations can't normally create this. It needs a row reserved before the upgrade, or one edited by hand.
But it stops recovery for that entire queue until someone fixes the row, and it can appear on the first sweep after upgrading.

**Recommendation:** use the same normalization as `append_failure`, with a regression test.

**Evidence:** Probe. This confirms F4 in [the principal review](review_libraries_principal.md), which couldn't run PostgreSQL.

## Low

### 18. Abstractions that no longer do anything

- **`Criterion`:** three crates build a list, and the repository ignores most of it (`libs/commanding/src/infra_diesel/repository.rs:181-192`), because the SQL
  is hard-coded. Outbox has already dropped it.
- **`max_attempts` on `failed()`:** three of four repositories ignore it (`_max_attempts`), so it only exists for outbox's schedule rule.
- **Timeout policy:** `ending_status`, `max_attempts` and `status_after_timeout(_attempts)`, plus `max_attempts` on the stale specs, are "retained for
  compatibility" (`libs/commanding/src/domain/reservation_timeout_policy.rs:13-16`, `:32-34`). That's a lot of compatibility for an unstable 0.x API.
- **Kernel:** `KernelConfig` is stored in an unused `_config` field, and `KernelError::Database`, `Amqp` and `Migration` are never constructed
  (`kernel/src/lib.rs:175`, `:609-614`).

**Recommendation:** remove them together, as one breaking change before publishing.

### 19. Gateways couple crates and hide different guarantees

`InboxConsumer` holds the concrete `CommandGateway` enum (`libs/inbox/src/execution/inbox_consumer.rs:14`), so `mulac-inbox` always pulls in
`mulac-commanding` and `mulac-eventing`. Commanding, by contrast, reaches eventing through the `EventDispatchPort` trait.

`CommandGateway` and `EventGateway` also hide opposite promises behind one `dispatch` call. `Ok` from `Direct` means "it ran", and `Ok` from `TwoPhased`
means "it's recorded".

**Recommendation:** add a `CommandDispatchPort` matching the event one. Consider separate types, or at least a return value, that say which happened.

### 20. Smaller items

- **Unbounded sweep:** it locks and loads every stale row in one transaction, with no limit. This comes from R4 in the principal review.
- **No limit on concurrent `/run` calls:** `limit` bounds one request. Many requests can tie up Tokio's blocking pool and the database pool.
- **ADR-015 side effects (my own change):** `Run…Response::from_result` is a contract conversion that now also logs
  (`libs/commanding/src/http/contract.rs:90-100`), which is a surprising effect for a conversion. Poem parse failures log `queue="http"`
  rather than the real queue (`libs/mulac_http/src/poem.rs:69`).
- **`check_prelude.py` is narrower than its name:** it only rejects `mod io`, so it doesn't enforce the prelude rule in `CLAUDE.md`. `mulac_diesel` breaks that
  rule and the check still passes. This comes from F6 in the principal review, and I confirmed it by reading the script.

## What to keep

These are good decisions, and later work should build on them:

- **Reservation with `FOR UPDATE SKIP LOCKED`** and ownership checks on every finalization. In the double-execution probe, the stale worker's writes were correctly
  rejected, so duplicates didn't corrupt state.
- **Per-row decoding and item-level panic containment** from `ad0b769`. All 36 of the newer database tests pass.
- **Single-transaction sweeps on database time.**
- **Feature isolation,** which `check_http_features.py` really does enforce across a feature matrix.
- **Diesel kept out of `domain`, `application` and `execution`.** The only other mentions are feature-gated re-exports in each `lib.rs`.
- **Error IDs at the HTTP boundary,** with the changes noted in finding 20.
- **Synchronous ports.** They keep the core simple, and queue work is mostly blocking anyway. This decision only appears inside a plan document, so it deserves its own ADR.
- **The ADR habit itself,** which made this review much easier.

## Where the lenses disagreed

- **What to do first.** The structure lens wanted the shared core first, to stop paying for every fix four times. The runtime lens wanted finding 1 first. **Outcome:** CI first, then
  finding 1, then the shared core, because finding 1 changes the ports the core would be built around.
- **Is finding 1 really critical?** The structure lens noted that the non-atomic handoff is documented, and [the principal review](review_libraries_principal.md) treats related
  behaviour as a design constraint. The runtime lens replied that the docs disclaim atomicity, not event loss, and that a reference app following the contract loses events silently.
  **Outcome:** Critical.
- **Merge the four crates?** Rejected. A shared core removes the duplication and keeps the boundaries between queues, which are real.
- **Make ports async?** Rejected. It would add a lot of churn to solve a problem nobody has shown.

## Decisions for the project owner

1. **Atomic handoff (finding 1).** Add a transactional handler path? This is the recommendation. The alternative is keeping today's design and making "re-emit events on replay" a strict,
   documented handler rule.
2. **Exhausted rows (finding 2).** "Failed, not Dead" was a deliberate choice. I recommend keeping its rule that nothing is discarded automatically, while giving these rows their own state. The
   meaning problem in finding 2 is the main reason, and finding 8's scan cost is a supporting one.
3. **Overlapping deploys (finding 9).** Are they supported? If so, this finding needs fixing. If not, say so as a requirement.
4. **Two web frameworks.** Poem and Axum for each of 4 queues means 8 adapter sets. The reference apps use Poem. Does `offers` need Axum? That code isn't in this repository.
5. **Direct mode.** In `kernel` and `test_apps`, only `kernel/tests/startup.rs` calls `start()`, and both apps use persistent mode. If `offers` doesn't need it either, removing direct mode would simplify the kernel and finding 19.

## Fix order

1. **Make results trustworthy (finding 6, and earlier finding 18).** Run the database tests in CI, and fix the formatting check.
2. **Reduce the silent loss now (finding 1, the "do this now" part).** Correct the contract and `tweet_like`.
3. **Decide atomic handoff (finding 1),** and record it in an ADR. It replaces the design question in `TODO.md`.
4. **Fix the lifecycle (findings 2, 3, 4, 8, 17).** Add the terminal state, requeue, retry policy, per-item lease and wired recovery.
5. **Extract the shared core (findings 7, 13, 14, 15, 18).** Include the conformance suite and migrations. Doing this before step 3 would mean refactoring twice.
6. **Assemble the pipeline (findings 5, 9, 12, 19),** with one reference app on the real libraries.
7. **Improve operations (findings 10, 11, 16, 20).** Some of these are cheap and can be done sooner, especially the docs in finding 16.

## Status of earlier findings

This checks the 24 findings in [`review_libraries_unified.md`](review_libraries_unified.md) against the code at `ad0b769`. That document still lists finding 1 as open.

| Earlier #      | Status           | Notes                                                                                                                                            |
| -------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1              | Fixed            | Fixed by `ad0b769`. The tests pass on real PostgreSQL                                                                                            |
| 2              | Open, in backlog | Unchanged. Finding 1 above would replace most of this design                                                                                     |
| 3–6, 11, 17    | Crossed off      | AMQP, not reviewed here                                                                                                                          |
| 7, 9           | Resolved         | By ADR-015                                                                                                                                       |
| 8              | Open             | Read: the raw request structs are unchanged. Not re-probed                                                                                       |
| 10             | Partly           | Rows no longer strand this way, but bridges and workers still can't set the limit                                                                |
| 12             | Open             | See finding 14                                                                                                                                   |
| 13             | Open             | Unchanged                                                                                                                                        |
| 14             | Partly           | Retry timing now uses database time. First scheduling still uses `Utc::now()`, for example `libs/commanding/src/infra_diesel/conversions.rs:22`  |
| 15             | Open             | Unchanged                                                                                                                                        |
| 16             | Open             | Unchanged                                                                                                                                        |
| 18             | Open             | Reproduced: 3 of 4 `diesel_storage` tests fail in parallel with `relation "outbox_entries" does not exist`. All 8 older tests pass on one thread |
| 19             | Appears fixed    | No re-wrapping found in either repository's `record`. Not re-probed                                                                              |
| 20, 21, 22, 23 | Open             | Unchanged. For 22, see finding 20                                                                                                                |
| 24             | Open by design   | Unchanged                                                                                                                                        |

Test run against the throwaway database:

| Suite                                                             | Result                                                                         |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `reservation_isolation`, `timeout_sweep`, commanding's sweep test | 36 of 36 passed, in parallel                                                   |
| `diesel_storage`, `lifecycle`, one thread                         | 8 of 8 passed                                                                  |
| `diesel_storage`, default parallel                                | 1 passed, 3 failed. Cargo stopped there, so `lifecycle` didn't run in parallel |
| Broker test in `confirmed_amqp_publisher.rs`                      | Not run                                                                        |

## How this relates to the principal review

[`review_libraries_principal.md`](review_libraries_principal.md) reviewed the same code and appeared while this one was being written. It's a sound review with a different focus, and I read it before finishing.

- **Agreement:** the `ad0b769` fix, its F2 (custom inbox backends), F4 (sweep), F5 (test race) and F6 (`mulac_diesel` and the prelude check).
- **Added here:** it couldn't run PostgreSQL. These runs confirm its F4 and F5, and supply the "real PostgreSQL validation" it lists as outstanding.
- **Different conclusion:** it says no evidence establishes an unconditional production-critical regression. That's still true, because finding 1 isn't a regression and needs a failed insert or a crash to trigger. Its R1 covers duplicate IDs on retry, but
  doesn't identify the lost events.
- **Not repeated:** its F1, F3, R2 and R3 are AMQP topics, outside this review.

## What wasn't checked

- **AMQP:** no transport or publisher behaviour was reviewed, and the broker test wasn't run.
- **Load:** the scan numbers come from one idle local database. Nothing here is a load or concurrency test at production scale.
- **Crash paths:** process death was modelled by abandoning a reservation, not by killing a process.
- **Probe timing:** retries and leases were compressed by forcing rows due and shortening timeouts, as noted in each finding.
- **The `offers` application:** it isn't in this repository, so its use of these APIs is unknown.
- **`test_apps`:** read as evidence of how the libraries are used, not reviewed in their own right.
- **Exhaustive reading:** the HTTP adapters and test files were sampled and compared, not read line by line in all four crates.
