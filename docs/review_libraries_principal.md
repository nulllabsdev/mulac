# Library review — two principal-engineer passes

**Date:** 2026-11-04  
**Reviewed revision:** `ad0b76982a3fdb9a8af3a473daf30251d2cba041`  
**Scope:** all six library workspace members; focused integration inspection where necessary. Review only: no implementation changes.

## Executive assessment

The recent item-failure isolation change materially improves correctness: reservation now loads raw JSON, decodes rows individually, persists malformed rows as `Failed` within the reservation transaction, and returns healthy rows only after commit. Consumers isolate recoverable item panics and attempt failure finalization after completion errors. Exhausted attempts intentionally remain `Failed`; this is not itself a stranded-row defect.

Remaining issues include an invalid unsafe test fixture, an incomplete public inbox backend interface, a publisher deadline that excludes lock contention, legacy-row recovery that still assumes valid diagnostic JSON, and destructive/racy database test fixtures. None of the review evidence establishes an unconditional production-critical regression. Several significant delivery risks are deliberate/documented design choices and are listed separately rather than misrepresented as newly discovered defects.

**Recommended order:** repair the unsafe test fixture; make database tests isolated and run them in PostgreSQL; fix recovery normalization and deadline accounting; expose the missing inbox backend types. Independently decide the supported identity/delivery guarantees before changing retry IDs or legacy AMQP behavior.

## Method and coverage

Exactly two reviewers worked on complementary passes:

- **Principal 1 (primary):** persistence, transaction boundaries, ownership fencing, retry eligibility, malformed rows, sweeps, write validation, and database tests.
- **Principal 2 (delegated):** execution/unwinding, async/blocking boundaries, HTTP/error contracts, AMQP, exported APIs, feature isolation, and unsafe test construction.
- Reviewers challenged classifications during consolidation. In particular, fresh IDs on retry and legacy AMQP behavior are retained as design risks, not asserted violations of an exactly-once contract. Database findings are static analysis unless explicitly identified as executed.

| Library      | Inspected areas                                                                                                         | Assessment / linked concerns                                                                                                       |
| ------------ | ----------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| commanding   | Gateway, typed handler wrapper, event handoff, consumer, Diesel record/reserve/finalize/sweep, retry specs, HTTP bridge | Reservation fix structurally sound; sweep normalization F4; output identity R1                                                     |
| eventing     | Gateway/dispatch port differences, subscriber dispatch, consumer, metadata conversion, Diesel lifecycle, HTTP           | Same recovery gap F4; composite subscriber replay R4                                                                               |
| inbox        | Public models/ports, command conversion, consumer, Diesel lifecycle, AMQP transport/worker, HTTP                        | F1, F2, F4; transport and identity constraints R1/R3                                                                               |
| outbox       | Recorder validation, conversion/publishing, both AMQP publishers, Diesel lifecycle, HTTP, integration fixtures          | F3–F5; legacy publication risk R2                                                                                                  |
| mulac_diesel | Complete small pool implementation and queue integrations                                                               | F6; pool tuning is possible through exposed pool types but not convenience builder                                                 |
| mulac_http   | JSON body helpers, Poem wrappers, shared schemas, error reporting, feature boundaries                                   | No additional confirmed production defect from this pass; tracing configuration and admission control remain host responsibilities |

This is a source and available-test review, not a proof of correctness, security audit, load test, or exhaustive execution of every adapter against external services.

## Confirmed code findings

Severity: **P2** = material correctness, usability, or validation issue with a specific trigger; **P3** = lower-priority convention/maintenance issue. “Static” means the relevant code path was inspected, not that its complete external-system consequence was reproduced.

### F1 — P2: AMQP unit fixture constructs an invalid Rust value

**Location:** `libs/inbox/src/amqp_consumption.rs:156–176`.

The fixture zeroes `MaybeUninit<Delivery>`, fills selected fields, but leaves `acker` zeroed before `assume_init()`. The resolved dependency is lapin **4.12.3**: `Acker` contains `KillSwitch(Arc<AtomicBool>)` and `ErrorHolder(Arc<OnceLock<Error>>)`. Zero is not a valid initialized `Arc` pointer. `mem::forget(delivery)` avoids destruction but cannot repair the invalid value created earlier.

**Trigger/impact:** running the AMQP-enabled unit test invokes undefined behavior. A passing run cannot establish safety and optimizer/platform changes can change the result. This is test-only code, not evidence of a production unsafe path.

**Evidence:** source inspection of the fixture and resolved dependency types; the ordinary all-features run reports this test passing. No Miri execution was performed, and the normal result must not be interpreted as a memory-safety check.

**Correction:** use lapin's supported `Delivery::mock` constructor, already used elsewhere in this workspace, then set test fields. Remove the unsafe construction and deliberate leak.

**Regression:** run the conversion test with a safely constructed delivery; cover missing/invalid metadata and invalid UTF-8 without manually initializing third-party internals.

### F2 — P2: External inbox backends cannot construct normal reserved envelopes

**Locations:** `libs/inbox/src/lib.rs:24–42`; `libs/inbox/src/application/models.rs:1–11,37–40,128–145`.

The public reserve port returns `InboxMessageEnvelope`, whose public `msg` field requires `InboxMessage`. That type is only re-exported `pub(crate)` internally and is absent from the crate's public prelude. `InboxStatus` is also absent from the public root prelude. There is no public constructor supplying an equivalent construction path.

**Trigger/impact:** an external custom `InboxReservePort` implementation cannot construct an ordinary nonempty result from its own stored row. Internal unit fixtures compile because they can import crate-private types, masking the advertised backend extension problem. Returning an error/empty batch or forwarding an envelope obtained elsewhere does not solve implementing a backend.

**Evidence:** static export and constructor inspection. No external compile-fail probe was run.

**Correction:** expose the necessary message/status types through `mulac_inbox::prelude`, or add a complete public constructor that accepts publicly nameable state. Do not expose implementation modules.

**Regression:** an external integration test, with default features disabled, constructs a reserved envelope through public imports and implements a fake backend returning it.

### F3 — P2: Strict publisher's advertised timeout excludes queueing

**Location:** `libs/outbox/src/confirmed_amqp_publisher.rs:20–25,69–99`.

`publish_async` waits for `self.connection.lock().await` before starting `tokio::time::timeout`. The public documentation promises a bound for each publication, including connection establishment, but concurrent callers can spend additional time behind other publications before receiving their own full timeout budget.

**Trigger/impact:** several batches share one publisher while broker operations are slow. End-to-end call duration can exceed the configured timeout by the cumulative queue wait, occupying blocking workers and potentially exceeding reservation timeouts. This is not a claim that a single uncontended network operation escapes the existing timeout.

**Evidence:** static ordering of lock acquisition and timeout. No live-broker contention reproduction was performed.

**Correction:** establish one deadline before lock acquisition and apply the remaining budget to acquisition plus connection/publish/confirmation. A caller timing out while waiting must not clear a connection owned by another caller.

**Regression:** hold the connection lock longer than the configured budget and assert a queued publication returns a timeout within that budget; also cover owner success, owner timeout, and queued-call cancellation.

### F4 — P2: Sweep recovery still lets malformed diagnostics block healthy stale rows

**Locations:** `libs/commanding/src/infra_diesel/repository.rs:218–265`; `libs/eventing/src/infra_diesel/repository.rs:237–247`; `libs/inbox/src/infra_diesel/repository.rs:227–237`; `libs/outbox/src/infra_diesel/repository.rs:263–273`.

The new reservation/failure helpers normalize malformed `extra_info`, but sweep SQL still invokes `jsonb_set(COALESCE(extra_info, '{}'), '{errors}', ...)` directly. SQL `NULL` is covered; a JSON scalar such as `42` or JSON `null` is not turned into an object. PostgreSQL cannot set an object path inside a scalar. One such candidate can fail the set-based update and roll back recovery of every other selected stale reservation.

**Trigger/impact:** a pre-upgrade `Reserved` poison row, or externally modified diagnostic JSON, is swept alongside healthy abandoned reservations. This is particularly relevant when upgrading from the batch-stranding implementation. The new reservation path prevents the ordinary creation of this case; this finding does **not** reopen the fixed new-reservation bug.

**Evidence:** static SQL/data-shape analysis; the PostgreSQL consequence was not executed in this environment and should be verified with the regression below before release.

**Correction:** give sweep diagnostics the same malformed-value preservation/normalization semantics as failure recording. Preserve original invalid data, append a canonical string error, retain row locks and transactionality, and keep outcomes `Failed` at exhaustion.

**Regression:** isolated PostgreSQL schemas with healthy stale rows plus `extra_info` values SQL NULL, JSON null, number, array, and invalid `errors`; all selected rows must release successfully with evidence retained. Forced database failures must still roll back the sweep.

### F5 — P2: Older outbox database tests race over a shared destructive schema

**Locations:** `libs/outbox/tests/lifecycle.rs:71–78`; `libs/outbox/tests/diesel_storage.rs:81–88`.

`reset_schema` holds `DB_LOCK` only while dropping/recreating `outbox_entries`; the guard is released before the test records or queries rows. Another test in the same binary can reset the table while the first test is executing. Separate test binaries/processes also have separate locks. These are ignored tests, but enabling them with normal parallelism exposes the race.

**Trigger/impact:** running ignored database tests concurrently causes nondeterministic missing rows/tables and misleading lifecycle failures. Using an incorrectly designated database also destroys the shared table; a variable named TEST is not isolation.

**Evidence:** static guard-lifetime and DDL inspection; concurrent PostgreSQL execution was unavailable. The newer `reservation_isolation` tests already use unique schemas and do not have this particular defect.

**Correction:** migrate these old fixtures to unique per-test schemas with pool search-path customization and schema-specific teardown. A mutex covering a whole test is only a partial workaround because it cannot coordinate separate processes.

**Regression:** run both integration binaries repeatedly with multiple test threads against a disposable database; verify an unrelated sentinel schema/table remains unchanged.

### F6 — P3: mulac_diesel remains outside the required prelude-only interface convention

**Location:** `libs/mulac_diesel/src/lib.rs:1–9`.

The whole public API is root-level `DbPool` and `build_pool`, without a `pub mod prelude`. Other libraries import these root symbols, contrary to the repository's stated public interface rule.

**Impact/evidence:** confirmed convention inconsistency, not a runtime defect. The current `scripts/check_prelude.py` only rejects legacy `mod io` declarations; its success does not validate the broader convention.

**Correction:** introduce the public prelude and migrate callers. Decide root-export compatibility through the normal release policy rather than silently breaking consumers. Strengthen the interface check to cover the intended rule.

**Regression:** public-prelude smoke tests and workspace/feature-matrix builds; intentionally violating fixtures should fail the convention checker.

## Delivery and operational design risks — not newly introduced regressions

### R1 — Retry identities change across durable-stage handoffs

`libs/inbox/src/application/conversions.rs:48–64` assigns a fresh command UUID each time a durable inbox row is converted. `libs/commanding/src/execution/command_dispatcher.rs:54–77,101–106` assigns new event IDs on handler replay and hands events off individually.

If storing a command succeeds but completing its inbox row fails, the next attempt can store a distinct command. If event A is recorded but event B fails, retry can record A again under a new ID. Primary-key conflict handling cannot deduplicate these logical repeats. At-least-once execution permits duplicates; the problem is the additional operational requirement for a stable business key, not a violated exactly-once promise.

**Recommendation:** derive or persist a stable command ID for the one-command-per-inbox handoff. For command outputs, persist/replay produced envelopes or support stable application event identity. Do not blindly derive IDs from output position: retry output/order may change and cause distinct events to collide. Missing inbound producer IDs similarly need an explicit policy; payload hashing conflates legitimate identical messages.

The AMQP adapter also silently turns a supplied non-UUID `message_id` into `None` (`libs/inbox/src/amqp_consumption.rs:106–111`), after which conversion generates a new UUID. A stable opaque producer ID therefore does not deduplicate redelivery. Require UUID IDs explicitly and reject invalid supplied values, or introduce a deterministic, source-scoped mapping; keep absent-ID acceptance a deliberate policy.

**Tests:** fail completion after successful command storage; fail the second of multiple event handoffs; redeliver a message with a stable non-UUID producer ID; verify the chosen identity policy without assuming deterministic output that the interface does not guarantee.

### R2 — Legacy AMQP publisher has weaker delivery guarantees

`libs/outbox/src/amqp_publisher.rs:23–29,73–78` defaults to non-mandatory publication and accepts `Ack(Some(returned))` and `NotRequested` as success. Its message properties do not request persistent delivery. Unroutable messages, or messages lost on restart depending on broker/topology configuration, can therefore coexist with a completed outbox row.

The strict `ConfirmedAmqpPublisher` deliberately rejects returned/unconfirmed messages and requests persistence. This distinction is documented, so migration/deprecation is preferable to silently changing the legacy adapter. Validate mandatory unroutable publication, confirmed routing, and restart behavior against a real broker when selecting the supported production adapter.

### R3 — Inbox transport availability and topology require host policy

`libs/inbox/src/amqp_consumption.rs:232–251` unconditionally declares a queue using default declaration options. It cannot express a durable queue configuration; declaring an existing incompatible durable queue fails rather than adapting to it.

The worker's `spawn_blocking` storage join and `ack`/`nack` awaits at `:390–407` occur inside a selected branch, outside subsequent cancellation checks. A hung database operation or acknowledgement can prevent prompt shutdown. The code documents finishing in-flight recording, and catching consumer panics does not add deadlines to this transport loop.

**Recommendation/tests:** offer explicit topology ownership/declaration options; configure database and acknowledgement deadlines plus supervision. Test durable pre-existing queues and cancellation while store/ack futures remain pending. Do not claim cancellation can forcibly interrupt synchronous database work already running.

### R4 — Recovery, fan-out, and concurrency remain application responsibilities

- A composite subscriber can replay already-successful handlers when a later subscriber fails; the event dispatcher has one subscriber port, not per-subscriber durable checkpoints.
- Sweepers are application-owned; recoverable item errors are not a replacement for crash recovery. Select timeouts with long-running work and serial batch queueing in mind.
- Sweep queries load and lock all matching rows without a limit. Large recovery backlogs can cause long transactions and memory pressure; consider bounded, ownership-safe sweeps after measuring operational scale.
- HTTP batch limits bound individual requests, not concurrent request count. Blocking-pool admission control, authentication, routing exposure, and rate limiting belong to the host unless explicitly added to library contracts.
- Error IDs require an installed tracing subscriber. Panic hooks still run before unwind containment; omitting panic payloads from persisted/returned errors does not suppress host stderr/log output.

These are operational/design constraints, not evidence that the new failure-isolation boundaries fail their documented contract.

## Reassessment of historical findings

| Earlier concern                                               | Current assessment                                                                                                                                                                                   |
| ------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| One malformed reservation discards all healthy rows           | Addressed structurally in all four repositories; raw JSON and row finalization share a transaction. Real PostgreSQL validation remains outstanding here.                                             |
| Consumer item panic/completion error strands remaining batch  | Recoverable per-item unwinds and completion failures now attempt failure finalization and continue. Database/custom repository failures, aborting panics and hung work remain documented exceptions. |
| Direct outbox sequence zero creates unreadable metadata       | Direct record now validates sequence before acquiring a connection; unit coverage passes.                                                                                                            |
| Automatic failures become Dead                                | Intentionally changed: failures/sweeps retain Failed, retry eligibility enforces the cap. Explicit manual outbox dead remains.                                                                       |
| Raw internal storage errors leak through batch HTTP responses | Shared reporting now emits logged IDs; current tests cover this. Host logs still contain causes by design.                                                                                           |
| Doubled command/event conversion-error prefix                 | Current recording paths propagate conversion errors directly rather than wrapping the same category again.                                                                                           |
| Custom inbox backend cannot construct envelopes               | Still open: F2.                                                                                                                                                                                      |
| Retry IDs, weak legacy AMQP guarantees                        | Still relevant design constraints: R1/R2.                                                                                                                                                            |
| Sweeper max-attempt mismatch means rows never reach Dead      | The old diagnosis is obsolete: remaining Failed at the cap is now intentional. HTTP bridges still choose default attempt limits; configurability is a separate product decision.                     |
| Whole-system exactly-once expectations                        | Not provided: handler side effects, event handoff, publication and completion are not one distributed transaction.                                                                                   |

## Validation performed in this review

```sh
cargo test --offline --workspace --all-features --manifest-path libs/Cargo.toml -- \
  --skip unresponsive_broker_times_out_and_reconnects \
  --skip failed_connections_report_transport_errors_and_reconnect
cargo clippy --offline --workspace --all-features --all-targets --manifest-path libs/Cargo.toml
CARGO_NET_OFFLINE=true python3 scripts/check_http_features.py
cargo fmt --manifest-path libs/Cargo.toml --all -- --check
python3 scripts/check_prelude.py
```

- Library tests: **224 passed, 0 failed, 59 ignored, 2 filtered** across test/doc-test results. Ignored results include database tests and documentation examples; they are not successful runtime database validations.
- The two named socket-based publisher tests were excluded due to the restricted execution environment. No live broker test was run.
- Clippy completed successfully **with warnings**: module inception, existing excessive argument counts/type complexity, lazy Option evaluation, items after a test module, a single-pattern match, and unnecessary casts. This is not a warning-clean result and was not a `-D warnings` invocation.
- HTTP feature isolation and AMQP feature combinations passed. Formatting and the limited public-interface naming check passed.
- Neither `DATABASE_URL_TEST` nor `OUTBOX_TEST_DATABASE_URL` was configured. No database mutation, real PostgreSQL concurrency test, broker restart test, sanitizer run, Miri run, or load benchmark was performed.
- Kernel/HTTP harness results reported in the preceding implementation turn are not counted as newly executed checks in this review.

## Remediation acceptance sequence

1. Replace F1's unsafe fixture, rerun the AMQP unit suite, and add safety-oriented validation where supported.
2. Isolate F5's database fixtures, then execute reservation and sweep tests against disposable PostgreSQL, including F4's malformed legacy rows and forced transaction errors.
3. Fix F3 with one end-to-end deadline and contention/cancellation regression coverage.
4. Fix F2 with a genuine external, no-default-features backend construction test.
5. Resolve F6 through a compatibility-aware prelude migration and a checker that tests the actual convention.
6. Make explicit product decisions for R1/R2/R3: stable identities, recommended publisher, queue topology, and timeouts. Document migration and test against a real broker before upgrading claims about delivery guarantees.

Monitor failed outcomes by attempt count and failure stage, stale reserved age, finalization errors, publication queue wait, and duplicate business identities. Do not use a green unit suite alone as evidence of database atomicity or broker delivery.
