# Unified libraries review

Review date: 2026-11-04. Code reviewed: `1dfeaae`. Line references were checked at
`30b8df2`, which only adds review documents on top of that commit.
Merged by Claude Code (`claude-opus-5-5`).

This document merges two reviews and is the one to work from:

- [`review_libs_claude.md`](review_libs_claude.md)
- [`review_libraries_codex.md`](review_libraries_codex.md)

Both stay in place as the original records. Where they disagreed, or where merging
changed something, [How the reviews were reconciled](#how-the-reviews-were-reconciled)
says so.

## Conclusion

**The libraries are not ready to publish yet.** The layering is sound and consistent
across the four queue crates, but failure handling can strand or duplicate work.

The review has 24 findings. The 6 AMQP findings (3, 4, 5, 6, 11 and 17) are crossed off at
the project owner's request. They stay visible for the record. Crossing them off changes
the fix list, not the code, so those issues still exist.

Finding 2 is in the backlog ([`TODO.md`](../TODO.md)). Findings 7 and 9 are resolved by
[ADR-015](adr/015-http-errors-by-id.md). That leaves 15 on the current fix list: 1 high, 8 medium,
5 low, and 1 design limitation.

No code was changed by this review.

## Scope and evidence

| Source        | Coverage                                                                                                                                                         |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Codex review  | Reports four reviewers covering all six libraries and the kernel. This count could not be confirmed independently.                                               |
| Claude review | One automated pass over the branch diff for eventing, inbox and outbox, plus one reader across commanding, `mulac_http`, `mulac_diesel` and parts of the kernel. |

Each finding states its evidence:

- **Probe:** reproduced in a temporary program outside the repository. Probes were not
  kept, so these scenarios should become regression tests when the issues are fixed.
- **Test run:** reproduced by running the existing tests.
- **Read:** concluded from reading the code.

No failure scenario was reproduced against a real database or broker, except finding 18.
Not every file was read line by line, and `test_apps` was only searched for specific usages.

## Summary

| #      | Severity               | Finding                                                    | Crates                        | Evidence                            | Source     |
| ------ | ---------------------- | ---------------------------------------------------------- | ----------------------------- | ----------------------------------- | ---------- |
| 1      | High                   | One bad row strands a whole reserved batch                 | all four queues               | Read; probe for the outbox trigger  | Both       |
| 2      | High (backlog)         | Retries generate new event and command IDs                 | commanding, inbox             | Probe for events; read for commands | Both       |
| ~~3~~  | ~~High~~               | ~~AMQP messages without a UUID ID lose dedupe~~            | ~~inbox~~                     | ~~Probe~~                           | ~~Both~~   |
| ~~4~~  | ~~High~~               | ~~The inbox AMQP worker can stall forever~~                | ~~inbox~~                     | ~~Read~~                            | ~~Claude~~ |
| ~~5~~  | ~~High~~               | ~~The inbox queue is always declared non-durable~~         | ~~inbox~~                     | ~~Read~~                            | ~~Both~~   |
| ~~6~~  | ~~High (legacy risk)~~ | ~~`AmqpPublisher` marks unroutable messages as published~~ | ~~outbox~~                    | ~~Read~~                            | ~~Claude~~ |
| 7      | Medium (resolved)      | Internal type names leak in error text                     | all four queues, `mulac_http` | Probe                               | Claude     |
| 8      | Medium                 | A JSON array is accepted as a run request                  | all four queues, `mulac_http` | Probe                               | Claude     |
| 9      | Medium (resolved)      | Batch errors return raw storage text                       | all four queues               | Read                                | Claude     |
| 10     | Medium                 | A non-default sweeper limit can strand rows                | all four queues, kernel       | Read                                | Claude     |
| ~~11~~ | ~~Medium~~             | ~~The publisher timeout excludes the lock wait~~           | ~~outbox~~                    | ~~Probe~~                           | ~~Codex~~  |
| 12     | Medium                 | Custom inbox backends can't be written                     | inbox                         | Probe                               | Both       |
| 13     | Medium                 | The kernel prelude exports the wrong command envelope      | kernel                        | Probe                               | Codex      |
| 14     | Medium                 | Scheduling mixes application and database clocks           | all four queues               | Read                                | Codex      |
| 15     | Medium                 | Glob imports can make `axum` ambiguous                     | all four queues               | Probe                               | Claude     |
| 16     | Medium                 | Kernel workers are capped at about 10 items per second     | kernel                        | Read                                | Claude     |
| ~~17~~ | ~~Medium~~             | ~~An inbox AMQP test has undefined behaviour~~             | ~~inbox~~                     | ~~Read~~                            | ~~Codex~~  |
| 18     | Medium                 | Outbox database tests destroy each other's data            | outbox                        | Test run                            | Both       |
| 19     | Low                    | Doubled `conversion error` prefix                          | eventing, commanding          | Read                                | Claude     |
| 20     | Low                    | Duplicated gateway dispatch logic                          | eventing                      | Read                                | Claude     |
| 21     | Low                    | A test imports a private module path                       | inbox                         | Read                                | Claude     |
| 22     | Low                    | `mulac_diesel` has no prelude or pool settings             | `mulac_diesel`                | Read                                | Claude     |
| 23     | Low                    | Duplicated code and an overstated test comment             | several                       | Read                                | Claude     |
| 24     | Design limitation      | One failing subscriber affects all the others              | kernel                        | Read                                | Claude     |

## High: delivery and ingestion

### 1. One bad row strands a whole reserved batch

**Where:**

- `libs/commanding/src/infra_diesel/repository.rs:94–103`
- `libs/eventing/src/infra_diesel/repository.rs:110–113`
- `libs/inbox/src/infra_diesel/repository.rs:110–116`
- `libs/outbox/src/infra_diesel/repository.rs:107–113`

**What happens:** `reserve` commits an `UPDATE ... RETURNING` that marks rows `Reserved` and
spends an attempt, and only then decodes them. One failure discards the whole batch, so
healthy rows stay `Reserved` even though no handler or publisher received them.

The failure can happen in two places. Inbox and outbox decode typed JSONB metadata inside
`.load()`. Commanding and eventing load it untyped and convert it afterwards.

Nothing in the kernel or test apps constructs a sweeper, so those rows stay stuck unless the
application schedules one. With a sweeper, the bad row can strand its neighbours each cycle
until they go `Dead` without ever being delivered.

**Trigger without database corruption:** recording a public `NewOutboxEntry` directly through
`OutboxStorePort` with `meta.sequence = Some(0)`. `OutboxRecorder` validates the sequence, but
the repository doesn't, and that metadata serializes but can't be deserialized.

**Evidence:** a probe confirmed the serialization asymmetry. The reservation consequences come
from reading the code, not from running PostgreSQL.

**Fix:** cover both decoding and conversion. Load raw metadata alongside the entry and reservation
IDs so each row can be validated separately, quarantine only the malformed entries, and return the
healthy ones. Validate metadata on write as well. Roll the reservation back if loading or quarantine
handling fails. Replacing only the final `collect()` isn't enough, and rollback alone doesn't ensure
progress past a poison row.

### 2. Retries generate new event and command IDs

**Status:** in the backlog ([`TODO.md`](../TODO.md)). It's not on the current fix list, but it's
still a real defect.

**Where:**

- Events: `libs/commanding/src/execution/command_dispatcher.rs:63–68`, and the dispatch loop at `:100–104`
- Commands: `libs/inbox/src/application/conversions.rs:57`

**What happens:** the stores rely on `on_conflict_do_nothing`, which only helps when a retry reuses
the same ID. These sites call `Uuid::now_v7()` on every attempt instead.

- A command produces two events. The first is recorded and the second fails, so the retry records the
  first event again under a new ID.
- An inbox message's command is recorded, then `completed` fails. The later retry records a second
  command, so the handler runs twice.

Event-store, outbox and downstream dedupe can't recognize these repeats. An application can only
dedupe with a separate stable business key, if it has one.

**Evidence:** a probe with the real wrapper and dispatcher, a handler returning two events and a sink
failing its second call stored three events for two logical events. The inbox case comes from reading
the code.

**Fix:** handle the two boundaries separately.

- **Inbox to command:** there is one command per inbox entry, so derive or persist a stable command
  ID from the durable entry ID, with a namespace.
- **Command to events:** durably retain produced envelopes and replay their identities, or require a
  stable application identity for each logical event. Deriving IDs from command ID plus event position
  is only safe if retries reproduce the same ordered list. If the first attempt emits `[A, B]` and a
  retry emits `[B]`, then B gets A's stored ID and is silently discarded as a duplicate. The current
  handler interface doesn't guarantee deterministic output.

### ~~3. AMQP messages without a UUID ID lose dedupe~~

~~**Where:** `libs/inbox/src/amqp_consumption.rs:106–110`, `libs/inbox/src/application/conversions.rs:32`~~

~~**What happens:** a stable AMQP message ID such as `"order-123"` becomes `None` when UUID parsing
fails, exactly as if the message had no ID. Inbox conversion then generates a fresh UUID on every
delivery. If storage succeeds but the ack is lost, redelivery creates another inbox entry, and the
primary-key conflict check can't catch it. Non-UUID correlation IDs are dropped the same way at
`libs/inbox/src/amqp_consumption.rs:111–115`.~~

~~**Evidence:** a probe with the real conversion code and `Delivery::mock` produced different inbox
UUIDs for two deliveries carrying `"order-123"`.~~

~~**Fix:** preserve opaque transport IDs, map them deterministically to storage identities, or reject
them explicitly rather than treating them as absent. A message with no ID at all has nothing to derive
from, so require a producer-supplied idempotency key or accept duplicates. Don't hash payloads, because
that merges distinct messages with identical content.~~

### ~~4. The inbox AMQP worker can stall forever~~

~~**Where:** `libs/inbox/src/amqp_consumption.rs:400` and `:396`~~

~~**What happens:** on a store error the worker discards the error, nacks with requeue and sleeps
10 seconds. An AMQP payload containing a NUL byte is valid UTF-8, so it passes the poison-message
check, but Postgres rejects it for a text column. That delivery then repeats indefinitely, and nothing
is logged.~~

~~Separately, a `spawn_blocking` failure returns through `?` at line 396 without acking or nacking, so
the delivery stays unacked until the channel closes.~~

~~**Fix:** log the error, treat permanent failures as poison messages, and settle the delivery on every
exit path.~~

### ~~5. The inbox queue is always declared non-durable~~

~~**Where:** `libs/inbox/src/amqp_consumption.rs:231–237`~~

~~**What happens:** every constructor reaches `queue_declare` with default options, which in the pinned
lapin version means `durable = false`. Callers can't override or skip this, even though they supply the
channel.~~

- ~~An existing durable queue rejects the declaration with `406 PRECONDITION_FAILED`, so the worker
  fails to start.~~
- ~~A queue the worker creates itself is transient, so messages not yet stored are lost on a broker restart.~~

~~**Evidence:** checked against the lapin source and RabbitMQ's documented declaration rules. No live
broker test was run.~~

~~**Fix:** let applications own the topology, use a passive existence check, or accept explicit
declaration options.~~

### ~~6. `AmqpPublisher` marks unroutable messages as published~~

~~**Where:** `libs/outbox/src/amqp_publisher.rs:74`~~

~~**What happens:** `Confirmation::Ack(_)` counts as success, including `Ack(Some(returned))`, which is how
the broker reports an unroutable mandatory message. The consumer then calls `completed`, and the message
is lost with no retry. With the default `mandatory: false`, the broker drops unroutable messages and acks
them anyway. This publisher also never sets persistent delivery.~~

~~`ConfirmedAmqpPublisher` already rejects `Ack(Some(_))` and sets persistent delivery.~~

~~**Classification:** this is a documented legacy risk, not a regression. `docs/outbox-spec.md:82` says
`AmqpPublisher` keeps its behaviour and points to `ConfirmedAmqpPublisher` for strict delivery.~~

~~**Fix:** move applications that need routed, persistent publication to `ConfirmedAmqpPublisher`, and
document the legacy adapter's limits. Prefer deprecation and a migration path; changing or removing it
needs an explicit release decision.~~

## Medium: HTTP contract and API

### 7. Internal type names leak in error text

**Status:** resolved by [ADR-015](adr/015-http-errors-by-id.md). Unparsable bodies now return only a
logged error ID, on Poem as well as Axum, so the struct rename below is no longer needed. The rest of
this finding is kept as the record of the problem.

**Where:**

- `libs/commanding/src/http/contract.rs:66`
- `libs/eventing/src/http/contract.rs:66`
- `libs/inbox/src/http/contract.rs:190`
- `libs/outbox/src/http/contract.rs:66`

**What happens:** a non-object body on Axum's `POST /run` returns `400` with the parser's text, which
now names a private struct. All four crates were confirmed. For a body of `5`:

```text
invalid type: integer `5`, expected struct RawRunOutboxRequest
```

It used to name the public request type. `null` and `[]` leak the name the same way, and
`run_request_error_messages` has no non-object case, so tests miss it.

This is a regression from the alignment work. Commanding introduced the pattern in `3373fa1`, and the
other three crates copied it. ADR-014 says HTTP error text doesn't change, which is wrong for these bodies
until this is fixed. Poem has a separate older leak, `expected struct RunRequestData`, from
`libs/mulac_http/src/poem.rs`.

**Fix:** name the raw struct after the public type, inside a private module. In a probe, this reproduced
the previous text exactly for all seven bodies tried. `#[serde(expecting = "struct RunOutboxRequest")]`
also works, except that `[]` loses the words `with 1 element`. `#[serde(rename = "...")]` does **not**
work, because the message keeps the Rust struct's name. Add non-object bodies to the fixture tables.

### 8. A JSON array is accepted as a run request

**Where:** the same raw structs as finding 7, and `RunRequestData` in `libs/mulac_http/src/poem.rs:67`.

**What happens:** serde's derived struct parsing also accepts sequences, so `[5]` parses as `limit = 5`
and starts a real batch. This was confirmed in all four crates' `from_json` and in Poem's `RunRequest`.
The OpenAPI schema declares an object, and the fixture tables list `[]` as invalid, so arrays clearly
aren't intended. This predates the alignment work.

**Fix:** reject non-objects before parsing in each `from_json` and in `parse_run_request`, for example
with `Value::is_object`. This deliberately turns `[5]` from a successful request into a `400`.

### 9. Batch errors return raw storage text

**Status:** resolved by [ADR-015](adr/015-http-errors-by-id.md). `errors[]` now holds one logged ID per
failure. The rest of this finding is kept as the record of the problem.

**Where:**

- `libs/commanding/src/http/execution.rs:46`
- `libs/eventing/src/http/execution.rs:45`
- `libs/inbox/src/http/execution.rs:59`
- `libs/outbox/src/http/execution.rs:48`

**What happens:** `from_result` copies each error's text into the `200` response body. During a database
outage, that text can carry r2d2 or Diesel details such as hosts, users or SQL. ~~Outbox can also add lapin text.~~ Inbox `submit` sanitizes the same class of error to `inbox message could not be stored`, and a test
enforces it.

**Fix:** decide whether `errors[]` is for operators or clients. If for clients, return sanitized messages
and log the detail. The host's authentication limits the exposure today.

### 10. A non-default sweeper limit can strand rows

**Where:** the same `execution.rs` lines as finding 9, and `kernel/src/workers.rs:22` and `:47`.

**What happens:** HTTP bridges and kernel workers always reserve with the default of 6 attempts, and
neither can be configured. The READMEs say to keep sweeper and consumer limits aligned, but that isn't
possible with a non-default limit. With a sweeper set to 10, a row swept at 6 attempts becomes `Failed`
because 6 is below 10, yet `reserve` requires `attempts < 6`. It's never retried and never goes `Dead`.

**Fix:** make the limit configurable on the bridges and workers, ideally from one shared value.

### ~~11. The publisher timeout excludes the lock wait~~

~~**Where:** `libs/outbox/src/confirmed_amqp_publisher.rs:69–75`~~

~~**What happens:** `connection.lock().await` runs before the publication timeout starts. Concurrent batches
sharing a publisher can wait many times the configured bound, holding blocking threads and reservations
long enough for premature sweeping.~~

~~**Evidence:** a probe held the connection mutex. A publication with a 10 ms timeout was still pending when
an outer timeout expired after about 101 ms.~~

~~**Fix:** apply one deadline across lock acquisition, connection and publication, while keeping connection
cleanup on failure.~~

### 12. Custom inbox backends can't be written

**Where:**

- `libs/inbox/src/lib.rs:24–42`
- `libs/inbox/src/application/models.rs:1–11` and `:36–40`
- The contradicting claim at `libs/inbox/README.md:55`

**What happens:** `InboxReservePort::reserve` must return `InboxMessageEnvelope`, whose `msg` field is an
`InboxMessage`. `InboxMessage` and `InboxStatus` aren't exported, and there's no public constructor or
equivalent conversion. An external backend can only return an empty list, return an error, or delegate to
an existing repository. `InboxMessageEnvelope::status()` also returns a type callers can't name. The README,
added during the alignment, says custom repositories can implement the ports.

**Evidence:** importing both types from the prelude fails with `E0432`.

**Fix:** export the types through the prelude or add a complete public constructor. Otherwise correct the
README.

### 13. The kernel prelude exports the wrong command envelope

**Where:** `kernel/src/lib.rs:9–24`, with the typed versions at `:99` and `:107`

**What happens:** the prelude exports commanding's non-generic `NewCommandEnvelope`, not the kernel's typed
`NewCommandEnvelope<C>`, and omits `ApplicationCommand`. Typed dispatch can't be used through the mandated
prelude, so callers go through crate-root exports, as `test_apps/todo/src/lib.rs:110` does.

**Evidence:** compile probes gave `E0432` for `ApplicationCommand` and `E0107` when supplying a type
parameter to the prelude's `NewCommandEnvelope`.

**Fix:** export the typed API through the prelude, and give the raw gateway envelope an explicit name.

### 14. Scheduling mixes application and database clocks

**Where:**

| Crate      | Retry scheduling                                     | Initial scheduling                                   |
| ---------- | ---------------------------------------------------- | ---------------------------------------------------- |
| commanding | `libs/commanding/src/infra_diesel/repository.rs:182` | `libs/commanding/src/infra_diesel/conversions.rs:22` |
| eventing   | `libs/eventing/src/infra_diesel/repository.rs:196`   | `libs/eventing/src/infra_diesel/conversions.rs:15`   |
| inbox      | `libs/inbox/src/infra_diesel/repository.rs:192`      | `libs/inbox/src/infra_diesel/conversions.rs:10`      |
| outbox     | `libs/outbox/src/infra_diesel/repository.rs:189`     | `libs/outbox/src/application/record_events.rs:54`    |

**What happens:** these paths use application `Utc::now()`, while reservation eligibility uses PostgreSQL
`now()`. An application clock ahead of PostgreSQL delays new entries and retries. A clock behind it by more
than the backoff makes failed entries immediately eligible, so attempts can run out quickly.

The sweep path already uses database time. The [parity plan](queue-commanding-parity-implementation-plan.md)
kept explicit-failure paths on the application clock as out of scope, so this is deferred work rather than an
oversight.

**Fix:** compute persisted scheduling timestamps from the same database clock as reservation eligibility.

### 15. Glob imports can make `axum` ambiguous

**Where:**

- `libs/commanding/src/http/mod.rs:11`
- `libs/eventing/src/http/mod.rs:11`
- `libs/inbox/src/http/mod.rs:12`
- `libs/outbox/src/http/mod.rs:11`

**What happens:** with the `axum` feature on, this fails with `E0659`, because `axum` could mean the
dependency or the adapter module brought in by the prelude glob:

```rust
use commanding::prelude::*;
use axum::Router;
```

The `poem` re-export has the same shape, but only the `axum` case was compiled.

**Fix:** document an absolute path for the dependency, which keeps both the public API and the prelude
convention. A probe confirmed this compiles:

```rust
use commanding::prelude::*;
use ::axum::Router;
```

### 16. Kernel workers are capped at about 10 items per second

**Where:** `kernel/src/workers.rs:8–9`

**What happens:** `BATCH_SIZE` is 10 and `POLL_INTERVAL` is 1 second, both fixed, and the worker sleeps even
after a full batch. A backlog drains at about 10 items per second per worker.

**Fix:** make both configurable, and skip the sleep when a batch was full.

## Medium: test reliability

### ~~17. An inbox AMQP test has undefined behaviour~~

~~**Where:** `libs/inbox/src/amqp_consumption.rs:158–176`~~

~~**What happens:** the test zero-initializes `Delivery`, sets every field except `acker`, then calls
`assume_init()`. lapin's `Acker` contains `Arc`-backed fields, for which zero isn't a valid value. Producing
that value is undefined behaviour, and `mem::forget` doesn't repair it.~~

~~**Evidence:** checked against the pinned lapin source. Miri wasn't available.~~

~~**Fix:** use lapin's public `Delivery::mock`, set the required properties, and remove the unsafe code.~~

### 18. Outbox database tests destroy each other's data

**Where:** `libs/outbox/tests/diesel_storage.rs:81–88`, `libs/outbox/tests/lifecycle.rs:71–78`

**What happens:** the mutex protects only table recreation and is released before the rest of each test.
Under normal parallel execution, one test can drop `outbox_entries` while another is using it. This also
means the parity plan's `--ignored` command fails as written.

**Evidence:** on a disposable PostgreSQL database, six of these eight tests failed in parallel with
`relation "outbox_entries" does not exist`, and all eight passed with `--test-threads=1`. The tests weren't
run at an earlier commit, so the lock lifetime is the diagnosed cause rather than a proven pre-existing
failure.

**Fix:** use per-test schemas and pools, as the newer timeout-sweep fixtures do, or hold a shared guard for
the whole test.

## Low: maintainability

### 19. Doubled `conversion error` prefix

**Where:** `libs/eventing/src/infra_diesel/repository.rs:26`, `libs/commanding/src/infra_diesel/repository.rs:37`

`map_err(…::conversion)` re-wraps an error that's already `Conversion`, which gives
`conversion error: conversion error: event_id is required: metadata is missing`. A plain `?` fixes it.

### 20. Duplicated gateway dispatch logic

**Where:** `libs/eventing/src/application/gateway.rs:36`

`EventDispatchPort::dispatch` repeats the inherent method's guard and match, and retypes the error text
instead of using `EventError::missing_event_metadata()`. The trait impl could call the inherent method and map
its error, keeping the current `EventDispatch` messages.

### 21. A test imports a private module path

**Where:** `libs/inbox/src/http/contract.rs:237`

The test uses `crate::http::testing::prelude::invalid_submissions()`. `CLAUDE.md` says callers must never import
through internal sub-modules, and `crate::prelude::testing` exposes the same fixtures.

### 22. `mulac_diesel` has no prelude or pool settings

**Where:** `libs/mulac_diesel/src/lib.rs`

The crate exports `DbPool` and `build_pool` at its root instead of through `pub mod prelude`. `build_pool` also
can't set a pool size or timeouts, although callers can build their own pool because `DbPool` is only a type alias.

### 23. Duplicated code and an overstated test comment

- `openapi_wrapper!` is defined twice: `libs/mulac_http/src/poem.rs:5` and `libs/inbox/src/http/poem/types.rs:19`.
- The `1..=1000` limit is hard-coded in `libs/mulac_http/src/poem.rs:116`, separately from each crate's `MIN_LIMIT`
  and `MAX_LIMIT` and the schema annotations.
- The fixture comments in eventing, outbox and commanding say drift between run-request tables "fails a test". No
  test compares the tables across crates; each crate's adapter tests only use its own.

## Design limitation

### 24. One failing subscriber affects all the others

**Where:** `kernel/src/event_subscriber_registry.rs:38`

**What happens:** the registry calls subscribers in order and stops at the first error. The whole event is then
retried, so earlier subscribers receive it again on every attempt, and later ones aren't called until the failing
one succeeds. If it never does, the event goes `Dead` and the later subscribers never receive it.

**Classification:** this follows the contract rather than breaking it. The
[event dispatcher contract](contracts.md#event-dispatcher) defines delivery to subscribers as a unit and names
partial delivery and repeated invocation as a known hazard, and `docs/components.md:94` says the same. It isn't a
release blocker by itself.

**Option:** if applications need independent progress per subscriber, change the contract and add per-subscriber
tracking or one queue row per subscriber. That's a separate design decision, not a local fix.

## Documented choices not treated as defects

These are documented, so neither review counted them as defects on their own. Findings 1 and 2 describe specific
consequences beyond them.

- Non-atomic handoffs between storage, handlers and queues
- At-least-once delivery
- Partial-success batch responses
- Application-owned sweep scheduling

## What's solid

- **Sweeps** run in one transaction, use database time, and apply a pure timeout policy.
- **Lifecycle updates** check reservation ownership, so a lost reservation can't overwrite a newer outcome.
- **Task failures** are sanitized before they reach HTTP clients.
- **Feature isolation** holds, and `scripts/check_http_features.py` enforces it for each crate.
- **Layering** is consistent: domain, application, execution, HTTP and infrastructure are kept apart in all four
  queue crates.
- **`mulac_diesel`** had no high-confidence defects beyond the API gap in finding 22.

## Validation results

| Check                                                                       | Result                                                                             |
| --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `make ci`, during the alignment work that became `1dfeaae`                  | Exit 0: 261 passed, 0 failed, 34 ignored                                           |
| Codex's test commands, in a sandbox                                         | 259 passed, after excluding 2 socket tests the sandbox blocked                     |
| Ignored database and broker tests (20) against real PostgreSQL and RabbitMQ | All passed, 6 only with `--test-threads=1`                                         |
| Ignored doc examples (14)                                                   | Reported as passing, but they're illustrative examples rather than behaviour tests |
| `todo` and `twitter` tests against PostgreSQL                               | 54 of 54 passed                                                                    |
| Feature isolation and AMQP feature combinations                             | Passed                                                                             |
| Clippy, all targets and features                                            | Completes with warnings; `make ci` doesn't deny them for `libs`                    |

The 259 and 261 totals agree, because the second run includes the 2 socket tests. The database and broker run shows
the existing tests pass. It doesn't reproduce the failure scenarios in the findings above.

## Fix order

This merges both reviews' orderings, with one change made during merging: test reliability moves first, because
finding 1's regression tests need a dependable database suite.

1. **Finding ~~17 and~~ 18.** Small, and it makes the later fixes verifiable.
2. **Finding 1.** Prevent reservation failures from stranding healthy work. Test both decoding and conversion
   failures.
3. ~~**Findings 3, 4 and 5, plus migration guidance for 6.** Fix AMQP ingestion and durability, and move strict-delivery
   callers off the legacy publisher without silently changing its contract.~~
4. **Finding 10 ~~and 11~~.** Align attempt limits ~~and enforce publication deadlines~~.
5. **Finding 8.** Enforce the JSON object shape. This is small and can move earlier if convenient.
6. **Findings 12, 13, 14, 15 and 16.** Address API policy, usability and operational limits with the least disruptive
   compatible fixes.
7. **Findings 19 to 23,** as time allows.

**Finding 24 is a separate decision.** Revisit it when applications need per-subscriber progress.

**Finding 2 is in the backlog.** When it's picked up, event identity needs an explicit design, not a positional UUID.

## Related open issues

- **Stale inbox flow docs.** `libs/inbox/flow.md`, `libs/inbox/flow-poem-api.svg` and `libs/inbox/flow.axum-api.svg` still
  describe `InboxService` and `src/api/`. The eventing and outbox flow docs are current.
- **Worker lifecycle.** `PersistentKernelHandle::wait()` cancels the token without joining worker tasks
  (`kernel/src/lib.rs:530`), as `review_command_flows_codex.md` also notes.
- **Breaking changes and `offers`.** The `offers` code isn't in this repository, so check it against the removed APIs in
  `CHANGELOG.md` before publishing to `margo`.

## How the reviews were reconciled

- **Severity scale.** Codex's P1 became High and P2 became Medium.
- **Finding 2.** Codex first rated event IDs P2 and left out inbox command IDs. Both are High here because they cause silent
  duplicates and can trigger on any partial failure.
- ~~**Finding 3.** The Claude review's "no message ID" case moved here from its retry finding, because it shares a cause and a
  fix with Codex's non-UUID case. The correlation ID note was added during merging.~~
- ~~**Finding 6.** The Claude review first rated this plain High. It's now a legacy risk, because the outbox specification keeps
  this behaviour deliberately.~~
- **Finding 7.** The Claude review recommended `#[serde(rename = "...")]`. A probe during merging showed that doesn't change the
  text, so this document recommends a different fix. **`review_libs_claude.md` still has the old recommendation.**
- **Finding 8.** Codex reported no high-confidence `mulac_http` defects, but a probe shows Poem's parser accepts `[5]`.
- **Finding 14.** Codex first cited commanding only. All four queues do this.
- **Finding 24.** The Claude review first rated this High. It's now a design limitation, because the contract documents the behaviour.
- **Unverified reports.** Codex's reviewer count and its probe results for findings 1, 2, ~~3, 11~~ and 13 are reported as written.
  The code they describe was read and agrees, but those probes weren't re-run. Both reviews reproduced finding 12 separately, and
  findings 7, 8 and 15 were re-run during merging.

| Unified | Claude review | Codex review |
| ------- | ------------- | ------------ |
| 1       | 1             | 1            |
| 2       | 3             | 4            |
| ~~3~~   | ~~3~~         | ~~3~~        |
| ~~4~~   | ~~5~~         |              |
| ~~5~~   | ~~6~~         | ~~2~~        |
| ~~6~~   | ~~2~~         |              |
| 7       | 7             |              |
| 8       | 8             |              |
| 9       | 9             |              |
| 10      | 10            |              |
| ~~11~~  |               | ~~5~~        |
| 12      | 11            | 6            |
| 13      |               | 7            |
| 14      |               | 8            |
| 15      | 12            |              |
| 16      | 13            |              |
| ~~17~~  |               | ~~9~~        |
| 18      | open issue    | 10           |
| 19      | 14            |              |
| 20      | 15            |              |
| 21      | 16            |              |
| 22      | 17            |              |
| 23      | 18            |              |
| 24      | 4             |              |
