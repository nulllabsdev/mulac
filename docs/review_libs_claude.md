# Libraries review

Review date: 2026-11-04. Reviewed commit: `1dfeaae` (`wip`), on `refactor/use-prelude`.
Reviewer: Claude Code (`claude-opus-5-5`).

## Conclusion

**The libraries are not ready to publish yet.** The layering is sound and consistent
across the four queue crates, but failure handling can lose, strand or duplicate work.
The review also found one HTTP regression from the recent alignment work (finding 7).
The findings distinguish implementation defects from the documented legacy publisher
semantics (finding 2) and whole-event subscriber delivery policy (finding 4).

No code was changed by this review.

## Scope and method

The review was requested as a team of two staff engineers. It was actually one
automated pass plus one reader, not two independent reviewers.

| Area                          | How it was reviewed                                                                                                   |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `eventing`, `inbox`, `outbox` | Automated `xhigh` review of the branch diff against `upstream/refactor/use-prelude`, plus the code around the changes |
| `commanding`                  | Read the repository, dispatcher, conversions, gateway and HTTP code, but not every file                               |
| `mulac_http`, `mulac_diesel`  | Read all source                                                                                                       |
| `kernel`                      | Read the workers, registries and parts of `lib.rs`                                                                    |

Not covered:

- Not every file was read line by line, for example the Diesel entity and schema files.
- Nothing was failure-tested against a real database or broker.
- `test_apps` was only searched for specific usages.

Each finding states its evidence:

- **Ran:** reproduced in a scratch crate outside the repository.
- **Read:** concluded from reading the code.

A follow-up review reproduced findings 7 and 8 in all four core request parsers and
the shared Poem parser, compiled the non-breaking import solution for finding 12,
and checked finding 10 against the reservation and sweep code. It also corrected
the recommendations below. No database or broker failure scenarios were rerun.

## Summary

| #   | Severity           | Finding                                                | Crates                        | Evidence |
| --- | ------------------ | ------------------------------------------------------ | ----------------------------- | -------- |
| 1   | High               | One bad row strands a whole reserved batch             | all four queues               | Read     |
| 2   | High (legacy risk) | `AmqpPublisher` marks unroutable messages as published | outbox                        | Read     |
| 3   | High               | Retries generate new IDs, which defeats dedupe         | commanding, inbox             | Read     |
| 4   | Design limitation  | One failing subscriber affects all the others          | kernel                        | Read     |
| 5   | High               | The inbox AMQP worker can stall forever                | inbox                         | Read     |
| 6   | High               | The inbox queue is always declared non-durable         | inbox                         | Read     |
| 7   | Medium             | Internal type names leak in error text                 | all four queues, `mulac_http` | Ran      |
| 8   | Medium             | A JSON array is accepted as a run request              | all four queues, `mulac_http` | Ran      |
| 9   | Medium             | Batch errors return raw storage text                   | all four queues               | Read     |
| 10  | Medium             | A non-default sweeper limit can strand rows            | all four queues, kernel       | Read     |
| 11  | Medium             | Custom inbox backends can't be written                 | inbox                         | Ran      |
| 12  | Medium             | Glob imports can make `axum` ambiguous                 | all four queues               | Ran      |
| 13  | Medium             | Kernel workers are capped at about 10 items per second | kernel                        | Read     |
| 14  | Low                | Doubled `conversion error` prefix                      | eventing, commanding          | Read     |
| 15  | Low                | Duplicated gateway dispatch logic                      | eventing                      | Read     |
| 16  | Low                | A test imports a private module path                   | inbox                         | Read     |
| 17  | Low                | `mulac_diesel` has no prelude or pool settings         | `mulac_diesel`                | Read     |
| 18  | Low                | Duplicated code and an overstated test comment         | several                       | Read     |

## Delivery risks and architectural limitations

### 1. One bad row strands a whole reserved batch

**Where:**

- `libs/commanding/src/infra_diesel/repository.rs:103`
- `libs/eventing/src/infra_diesel/repository.rs:113`
- `libs/inbox/src/infra_diesel/repository.rs:110–116`
- `libs/outbox/src/infra_diesel/repository.rs:107–113`

**What happens:** `reserve` commits the `UPDATE` that marks rows `Reserved` and spends an
attempt, but decoding the returned rows and converting them into envelopes can still
fail. For inbox and outbox, typed JSONB decoding happens inside `.load::<...>()`, before
the later `collect()` loop receives any rows. For example, outbox metadata containing
`sequence: 0` fails during this load. Commanding and eventing also have fallible metadata
conversion after loading. Either failure returns an error for the whole batch, so the
consumer gets none of the rows. Every healthy row stays `Reserved` and undelivered.

Nothing in the kernel or test apps constructs a sweeper, so those rows stay stuck unless
the application schedules one. With a sweeper, the bad row can strand its neighbours again
each cycle until they go `Dead` without ever being delivered.

**Fix:** cover both database-row decoding and envelope conversion. Load raw metadata
alongside entry and reservation IDs so it can be validated per row; quarantine only
the malformed entries and return the healthy ones. Validate metadata before recording
it as well. Reservation updates must roll back if loading or quarantine handling fails,
so unrelated rows do not spend attempts without being delivered. Replacing only the
final `collect()` is insufficient, and rollback alone still needs poison-row isolation
to ensure progress.

### 2. `AmqpPublisher` marks unroutable messages as published

**Where:** `libs/outbox/src/amqp_publisher.rs:74`

**What happens:** `Confirmation::Ack(_)` counts as success, including `Ack(Some(returned))`,
which is how the broker reports an unroutable mandatory message. The consumer then calls
`completed`, and the message is lost with no retry. With the default `mandatory: false`,
the broker drops unroutable messages and acks them anyway. This publisher also never sets
persistent delivery, so a confirmed message can still be lost on a broker restart.

`ConfirmedAmqpPublisher` already rejects `Ack(Some(_))` and sets persistent delivery.

**Classification:** this is a documented legacy delivery risk, not a newly introduced
regression. The [outbox specification](outbox-spec.md#confirmedamqppublisher) explicitly
preserves `AmqpPublisher` behaviour and offers `ConfirmedAmqpPublisher` for strict delivery.

**Fix direction:** migrate applications requiring routed, persistent publication to
`ConfirmedAmqpPublisher` and clearly document the legacy adapter's limitations. Prefer a
deprecation and migration path before changing or removing the legacy implementation;
either compatibility change needs an explicit release decision.

### 3. Retries generate new IDs, which defeats dedupe

**Where:**

- Event IDs: `libs/commanding/src/execution/command_dispatcher.rs:65`
- Command IDs: `libs/inbox/src/application/conversions.rs:57`
- AMQP messages with no ID: `libs/inbox/src/application/conversions.rs:32`

**What happens:** the stores rely on `on_conflict_do_nothing`, which only helps when a retry
reuses the same ID. These sites call `Uuid::now_v7()` on each attempt instead.

- A command produces two events. The first is recorded and the second fails, so the retry
  records the first event again under a new ID.
- An inbox message's command is recorded, then `completed` fails. The later retry records a
  second command, so the handler runs twice.
- An AMQP message without a `message_id` is redelivered after a crash between storing and
  acking, so it's stored as a new row.

Downstream consumers cannot dedupe these deliveries by the generated IDs. An application
may still dedupe using a separate stable business key, if one is available.

**Fix direction:** handle the three identity boundaries separately:

- For the one-command-per-inbox-entry handoff, derive or persist a stable command ID
  using the durable inbox entry ID, with an appropriate namespace.
- For command output, durably retain produced event envelopes and replay their identities,
  or require a stable application identity for each logical event. Deriving IDs from
  command ID plus event position is safe only if retries reproduce the same ordered
  logical event list. If the first attempt emits `[A, B]` and a retry emits `[B]`, positional
  identity can give B the already-stored ID of A and silently discard B as a duplicate.
  The current handler interface does not enforce deterministic output or ordering.
- For an inbound message without a stable transport ID, require a producer-supplied
  idempotency key or explicitly accept duplicate processing. There is no source ID to
  derive from in this case; hashing payloads would also conflate distinct legitimate
  messages with identical content.

### 4. One failing subscriber affects all the others (documented design limitation)

**Where:** `kernel/src/event_subscriber_registry.rs:38`

**What happens:** the registry calls subscribers in order and stops at the first error. The
whole event is then retried, so earlier subscribers receive it again on every attempt, and
later ones aren't called until the failing one succeeds. If it never does, the event goes
`Dead` after its attempts run out and the later subscribers never receive it.

**Classification:** the [event dispatcher contract](contracts.md#event-dispatcher)
explicitly defines delivery to subscribers as a unit and acknowledges partial delivery
and repeated invocation. The implementation follows that contract; this is an
architectural limitation, not an implementation regression or an automatic release blocker.

**Design option:** if applications require independent progress for every subscriber,
change the delivery contract and introduce per-subscriber tracking or one queue row per
subscriber. Treat this as a separate design decision rather than a local correctness fix.

### 5. The inbox AMQP worker can stall forever

**Where:** `libs/inbox/src/amqp_consumption.rs:400` and `:396`

**What happens:** on a store error the worker discards the error, nacks with requeue and
sleeps 10 seconds. An AMQP payload containing a NUL byte is valid UTF-8, so it passes the
poison-message check, but Postgres rejects it for a text column. That delivery then repeats
indefinitely, and nothing is logged.

Separately, a `spawn_blocking` failure returns through `?` at line 396 without acking or
nacking, so the delivery stays unacked until the channel closes.

**Fix:** log the error, treat permanent failures as poison messages, and settle the delivery
on every exit path.

### 6. The inbox queue is always declared non-durable

**Where:** `libs/inbox/src/amqp_consumption.rs:234`

**What happens:** the transport always uses `QueueDeclareOptions::default()`, with no way to
configure it. Against an existing durable queue, RabbitMQ answers `406 PRECONDITION_FAILED`
and the worker fails to start. If the worker creates the queue itself, it's non-durable, so
messages not yet stored in the inbox are lost on a broker restart.

**Fix:** make the declaration configurable and default to durable, or support a passive
declare for queues the application owns.

## Medium: HTTP contract and API

### 7. Internal type names leak in error text

**Where:**

- `libs/commanding/src/http/contract.rs:66`
- `libs/eventing/src/http/contract.rs:66`
- `libs/outbox/src/http/contract.rs:66`
- `libs/inbox/src/http/contract.rs:190`

**What happens:** a non-object body on Axum's `POST /run` returns `400` with the parser's text,
which now names the internal struct. For a body of `5`, `RunCommandsRequest::from_json` gives:

```text
invalid type: integer `5`, expected struct RawRunCommandsRequest
```

It used to name the public request type. `null` and `[]` leak the name the same way.
`run_request_error_messages` has no case for non-object bodies, so tests miss it.

This is a regression from the alignment work. Commanding introduced the pattern in
`3373fa1`, and the other three crates copied it. ADR-014 says HTTP error text doesn't change,
which is wrong for these bodies. Poem has a similar older leak, `expected struct RunRequestData`,
from `libs/mulac_http/src/poem.rs`.

**Fix:** add `#[serde(rename = "...")]` with the public name to each raw struct, and add
non-object bodies to the fixture tables.

### 8. A JSON array is accepted as a run request

**Where:** the same `Raw…` structs as finding 7, and `RunRequestData` in
`libs/mulac_http/src/poem.rs`.

**What happens:** serde's derived struct parsing also accepts sequences, so `[5]` parses as
`limit = 5` and starts a real batch. `[5]` was confirmed against commanding's contract and
Poem's `RunRequest`, and over HTTP on eventing, outbox and inbox. The OpenAPI schema declares
an object, and the fixture tables list `[]` as invalid, so arrays clearly aren't intended.
This predates the alignment work.

**Fix:** reject non-objects in each `from_json` and in `parse_run_request`, for example with
`Value::is_object`.

### 9. Batch errors return raw storage text

**Where:**

- `libs/commanding/src/http/execution.rs:46`
- `libs/eventing/src/http/execution.rs:45`
- `libs/inbox/src/http/execution.rs:59`
- `libs/outbox/src/http/execution.rs:48`

**What happens:** `Run…Response::from_result` copies each error's text into the `200` response
body. During a database outage, that text can carry r2d2 or Diesel details such as hosts,
users or SQL, and outbox can add lapin error text. Inbox `submit` sanitizes the same class of
error to `inbox message could not be stored`, and a test enforces it.

**Fix:** decide whether `errors[]` is meant for operators or clients. If for clients, return
sanitized messages and log the detail. The host's authentication limits the exposure today.

### 10. A non-default sweeper limit can strand rows

**Where:** the same `execution.rs` lines as finding 9, and `kernel/src/workers.rs:22` and `:47`.

**What happens:** HTTP bridges and kernel workers always reserve with the default of 6 attempts,
and neither can be configured. The READMEs say to keep the sweeper and consumer limits aligned,
but that isn't possible with a non-default limit. With a sweeper set to 10, a row swept at 6
attempts becomes `Failed` because 6 is below 10, yet `reserve` requires `attempts < 6`. It's
never retried and never goes `Dead`.

**Fix:** make the limit configurable on the bridges and workers, ideally from one shared value.

### 11. Custom inbox backends can't be written

**Where:** `libs/inbox/src/lib.rs:24`, and the claim at `libs/inbox/README.md:55`.

**What happens:** `InboxReservePort::reserve` must return `InboxMessageEnvelope`, which contains
an `InboxMessage`. `InboxMessage` and `InboxStatus` aren't exported, so an external crate can only
return an empty list or an error. The README, added during the alignment, says custom repositories
can implement the ports. `InboxMessageEnvelope::status()` also returns a type callers can't name.

**Fix:** export both types, or correct the README.

### 12. Glob imports can make `axum` ambiguous

**Where:**

- `libs/commanding/src/http/mod.rs:11`
- `libs/eventing/src/http/mod.rs:11`
- `libs/outbox/src/http/mod.rs:11`
- `libs/inbox/src/http/mod.rs:12`

**What happens:** with the `axum` feature on, the following doesn't compile. The error is
`E0659`: `axum` is ambiguous between the dependency and the adapter module brought into
scope by the prelude glob. Project APIs must still be imported through their preludes.

```rust
use commanding::prelude::*;
use axum::Router;
```

The `poem` re-export has the same shape, but only the `axum` case was compiled.

**Fix:** document an absolute path for the external dependency. This preserves both the
existing public API and the prelude import convention, and was compiled successfully:

```rust
use commanding::prelude::*;
use ::axum::Router;
```

Selective imports from the prelude can also avoid importing its `axum` module into the
same scope. Renaming public exports or changing the convention is not necessary.

### 13. Kernel workers are capped at about 10 items per second

**Where:** `kernel/src/workers.rs:8-9`

**What happens:** `BATCH_SIZE` is 10 and `POLL_INTERVAL` is 1 second, both fixed, and the worker
sleeps even after a full batch. A backlog therefore drains at about 10 items per second per worker.

**Fix:** make both configurable, and skip the sleep when a batch was full.

## Low: maintainability

### 14. Doubled `conversion error` prefix

**Where:** `libs/eventing/src/infra_diesel/repository.rs:26`,
`libs/commanding/src/infra_diesel/repository.rs:37`

`map_err(…::conversion)` re-wraps an error that's already `Conversion`, which gives
`conversion error: conversion error: event_id is required: metadata is missing`. A plain `?` fixes it.

### 15. Duplicated gateway dispatch logic

**Where:** `libs/eventing/src/application/gateway.rs:36`

`EventDispatchPort::dispatch` repeats the inherent method's guard and match, and retypes the error
text instead of using `EventError::missing_event_metadata()`. A new variant must be changed in both
places. The trait impl could call the inherent method and map its error, keeping the current
`EventDispatch` messages.

### 16. A test imports a private module path

**Where:** `libs/inbox/src/http/contract.rs:237`

The test uses `crate::http::testing::prelude::invalid_submissions()`. `CLAUDE.md` says callers must
never import through internal sub-modules, and `crate::prelude::testing` exposes the same fixtures.

### 17. `mulac_diesel` has no prelude or pool settings

**Where:** `libs/mulac_diesel/src/lib.rs`

The crate exports `DbPool` and `build_pool` at its root instead of through `pub mod prelude`.
`build_pool` also can't set a pool size or timeouts, although callers can build their own pool
because `DbPool` is only a type alias.

### 18. Duplicated code and an overstated test comment

- `openapi_wrapper!` is defined twice: `libs/mulac_http/src/poem.rs:5` and
  `libs/inbox/src/http/poem/types.rs:19`.
- The `1..=1000` limit is hard-coded in `libs/mulac_http/src/poem.rs:116`, separately from each
  crate's `MIN_LIMIT` and `MAX_LIMIT` and the schema annotations.
- The fixture comments in eventing, outbox and commanding say drift between run-request tables "fails
  a test". No test compares the tables across crates; each crate's adapter tests only use its own.

## What's solid

- **Sweeps** run in one transaction, use database time, and apply a pure timeout policy.
- **Lifecycle updates** check reservation ownership, so a lost reservation can't overwrite a newer outcome.
- **Task failures** are sanitized before they reach HTTP clients.
- **Feature isolation** holds, and `check_http_features.py` enforces it for each crate.
- **Layering** is consistent: domain, application, execution, HTTP and infrastructure are kept apart
  in all four queue crates.

## Suggested fix order

1. **Finding 1.** Prevent reservation failures from stranding healthy work; include both
   Diesel decoding failures and later envelope-conversion failures in regression tests.
2. **Findings 5 and 6, plus migration guidance for 2.** Address permanent ingestion
   failures and queue durability, and move strict-delivery callers off the legacy publisher
   without silently changing its compatibility contract.
3. **Findings 3 and 10.** Define stable identities across retries and align consumer/sweeper
   attempt limits. Event identity requires an explicit persistence or identity design, not
   an unconditional positional UUID substitution.
4. **Findings 7 and 8.** Restore the promised error text and enforce the documented JSON
   object shape, after the data-loss and stranded-work risks.
5. **Findings 9, 11, 12 and 13, then the low-severity items.** Address API policy, usability,
   and operational limitations with the least disruptive compatible fixes.

**Finding 4 is a separate architectural decision.** Revisit it when applications need
per-subscriber progress; the existing whole-event delivery policy is already documented.

## Related open issues

These were found before this review and are still open:

- **Stale inbox flow docs.** `libs/inbox/flow.md`, `libs/inbox/flow-poem-api.svg` and
  `libs/inbox/flow.axum-api.svg` still describe `InboxService` and `src/api/`, and `flow.md` has dead
  source links.
- **Outbox test race.** Six ignored tests in `libs/outbox/tests/diesel_storage.rs` and
  `libs/outbox/tests/lifecycle.rs` fail in parallel with `relation "outbox_entries" does not exist`,
  and pass with `--test-threads=1`. `reset_schema` releases its lock when it returns, so one test can
  drop the table while another is using it. This wasn't re-run at an earlier commit, so that cause is
  a diagnosis rather than a proven pre-existing failure.
- **Worker lifecycle.** `PersistentKernelHandle::wait()` cancels the token without joining worker tasks
  (`kernel/src/lib.rs:530`), as `review_command_flows_codex.md` also notes.
- **Breaking changes and `offers`.** The `offers` code isn't in this repository, so check it against the
  removed APIs in `CHANGELOG.md` before publishing to `margo`.
