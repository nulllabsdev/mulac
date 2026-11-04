# Library and kernel review

Review date: 2026-11-04. Line references were checked against `f5e4020`, whose
library code matches `1dfeaae`.

This review was corrected after verification; see
[Corrections after verification](#corrections-after-verification).

## Scope and approach

Four reviewers examined all six libraries and the kernel, including persistence,
concurrency, delivery semantics, HTTP adapters, public APIs, feature boundaries,
and tests. This was a review of the implementations, not just a commit diff.

| Reviewer | Primary responsibility                                                 |
| -------- | ---------------------------------------------------------------------- |
| 1        | Kernel, `mulac_diesel`, cross-library verification, and test execution |
| 2        | `commanding`                                                           |
| 3        | `eventing` and `outbox`                                                |
| 4        | `inbox` and `mulac_http`                                               |

No implementation files were changed. Focused reproduction probes were kept
outside the repository. This document records findings, not completed fixes.

Priority definitions:

- **P1 — high:** address before relying on the affected delivery guarantees.
- **P2 — medium:** correctness, API usability, or test reliability issues that
  should be scheduled for correction.

## Corrections after verification

Claude Code checked these findings by reading the code at `f5e4020`, and all ten
held up. The probes were not kept, so they were not re-run. The reviewer count and
probe results below remain as originally reported.

These parts were changed:

- **Finding 1:** added inbox, which has the same pattern.
- **Finding 3:** corrected the inbox conversion line reference.
- **Finding 4:** raised from P2 to P1 and extended to inbox command IDs.
- **Finding 8:** extended from commanding to all four queues, and noted that the
  parity plan deferred this work.
- **Finding 10 and the limitations:** added results from a later test run.
- **Boundaries:** corrected the claim about `mulac_http`.
- **Recommended order:** updated to match finding 4's new priority.

This review does not list every known issue. [The Claude review](review_libs_claude.md)
has findings that are not repeated here, including `AmqpPublisher` marking unroutable
messages as published.

## High-priority findings

### 1. [P1] One malformed row can exhaust retries for an entire healthy batch

**Locations:**
[outbox repository:107–113](../libs/outbox/src/infra_diesel/repository.rs#L107-L113),
[commanding repository:94–103](../libs/commanding/src/infra_diesel/repository.rs#L94-L103),
[eventing repository:110–113](../libs/eventing/src/infra_diesel/repository.rs#L110-L113),
and [inbox repository:110–116](../libs/inbox/src/infra_diesel/repository.rs#L110-L116).

Reservation executes an autocommitted `UPDATE ... RETURNING` before decoding
all returned rows. One decoding failure discards every envelope, leaving healthy
entries reserved with their attempt counters incremented even though no handler
or publisher received them. Repeated sweep/reservation cycles can eventually
mark never-processed entries dead. Without a sweep, they remain reserved.

Outbox permits a concrete trigger without external database corruption: directly
recording a public `NewOutboxEntry` with `meta.sequence = Some(0)`. The repository
does not validate it; its metadata serializes successfully but cannot deserialize.

**Evidence:** a focused probe confirmed the serialization/deserialization
asymmetry. The reservation consequences were verified by source inspection,
not by executing PostgreSQL integration tests.

**Fix direction:** validate writes and isolate malformed rows without consuming
healthy entries' attempts. Transactionally rolling back failed decoding prevents
stranded reservations, but poison-row isolation is also needed for progress.

### 2. [P1] Inbox AMQP initialization cannot consume existing durable queues

**Location:**
[AMQP consumption:231–237](../libs/inbox/src/amqp_consumption.rs#L231-L237).

Every public transport/worker constructor reaches `queue_declare` with default
options and empty arguments. The pinned Lapin defaults set `durable = false`.

- An existing durable queue rejects the declaration with
  `406 PRECONDITION_FAILED`.
- A newly created transient queue can lose pending deliveries on broker restart,
  before those deliveries reach PostgreSQL.
- Callers cannot override or skip the declaration, despite supplying the channel.

**Evidence:** checked against the pinned Lapin implementation and RabbitMQ's
[queue declaration and durability rules](https://www.rabbitmq.com/docs/queues).
No live broker test was available.

**Fix direction:** let applications own topology declaration, use a passive
existence check, or accept explicit declaration options and arguments.

### 3. [P1] Valid non-UUID AMQP message IDs silently lose deduplication

**Locations:**
[AMQP consumption:106–110](../libs/inbox/src/amqp_consumption.rs#L106-L110)
and [inbox conversion:32](../libs/inbox/src/application/conversions.rs#L32).

An ordinary stable AMQP message ID such as `"order-123"` becomes `None` when UUID
parsing fails. Inbox conversion then generates a fresh UUID on every delivery.
If storage succeeds but acknowledgement is lost, broker redelivery creates
another inbox entry despite carrying the same transport ID. The primary-key
conflict check cannot deduplicate it.

**Evidence:** a focused probe used the actual AMQP conversion code, safe
`Delivery::mock` values, and the public inbox conversion. Two deliveries carrying
`"order-123"` produced different inbox UUIDs.

**Fix direction:** preserve opaque transport IDs, map them deterministically to
storage identities, or explicitly reject unsupported IDs instead of silently
treating them as absent.

This is separate from the documented non-atomic inbox-to-command handoff.

### 4. [P1] Retries generate new identities for already-delivered events and commands

**Locations:**
[command dispatcher:63–68](../libs/commanding/src/execution/command_dispatcher.rs#L63-L68),
[dispatch loop:100–104](../libs/commanding/src/execution/command_dispatcher.rs#L100-L104),
and [inbox conversion:57](../libs/inbox/src/application/conversions.rs#L57).

The typed handler wrapper assigns fresh event UUIDs on every execution. If the
first event is stored and the second event's dispatch fails, retrying the command
regenerates both identities. Event-store, outbox, and downstream message-ID
deduplication cannot recognize the already-delivered logical event.

The inbox path has the same instability. Its conversion assigns a fresh
`command_id` on every attempt, so if `dispatch` records the command and `completed`
then fails, the retried message records a second command that the store's conflict
check cannot recognize.

**Evidence:** a focused probe used the real wrapper and dispatcher with a handler
returning two events and a sink failing its second call. Retrying the identical
command stored three events for two logical events; the duplicate payload had a
different event ID. The inbox case was verified by source inspection, not by a probe.

**Fix direction:** preserve produced event identities across retries through
durable output recording or an explicitly stable identity strategy, and derive
inbox command IDs from the message ID. This finding concerns identity instability,
not the documented non-atomic handoff alone.

## Additional correctness and API findings

### 5. [P2] Publisher timeout excludes waiting for its shared connection lock

**Location:**
[confirmed publisher:69–75](../libs/outbox/src/confirmed_amqp_publisher.rs#L69-L75).

`connection.lock().await` executes before the publication timeout starts.
Concurrent batches sharing a publisher can therefore wait many times the
configured bound, retaining blocking threads and reservations long enough for
premature sweeping.

**Evidence:** a controlled probe of the unchanged implementation held the
connection mutex. A publication configured with a 10 ms timeout was still pending
when an outer timeout expired after approximately 101 ms. No network was needed.

**Fix direction:** apply one deadline across lock acquisition, connection
establishment, and publication while preserving connection cleanup on failure.

### 6. [P2] Independent inbox backends cannot construct reservation results

**Locations:**
[inbox model exports:1–11](../libs/inbox/src/application/models.rs#L1-L11),
[envelope:36–40](../libs/inbox/src/application/models.rs#L36-L40), and
[root prelude:24–42](../libs/inbox/src/lib.rs#L24-L42).

`InboxReservePort` publicly returns `InboxMessageEnvelope`, but its mandatory
`msg` field has an `InboxMessage` type that is not exported. Its required
`InboxStatus` is also not exported. There is no public constructor, deserialization
implementation, or equivalent conversion to construct a stored envelope.

An independent backend can return empty/error results or delegate to an existing
repository, but cannot construct real reservations using the public API.

**Evidence:** an external compile probe importing `InboxMessage` and `InboxStatus`
from the root prelude failed with E0432.

**Fix direction:** export the required types through `prelude`, or expose a
complete public envelope constructor.

### 7. [P2] Kernel prelude exports the wrong command envelope

**Location:** [kernel prelude:9–24](../kernel/src/lib.rs#L9-L24).

The prelude exports commanding's nongeneric `NewCommandEnvelope`, not the kernel's
typed `NewCommandEnvelope<C>`, and omits `ApplicationCommand`. Typed application
dispatch therefore cannot be used through the repository's mandated prelude
interface; callers must bypass it through crate-root exports.

**Evidence:** external compile probes produced E0432 for `ApplicationCommand` and
E0107 when supplying a type parameter to the prelude's `NewCommandEnvelope`.

**Fix direction:** export the typed application API through the prelude and
distinguish the raw gateway envelope with an explicit name.

### 8. [P2] Scheduling mixes application and database clocks

**Locations:**

- Commanding: [retry scheduling:178–187](../libs/commanding/src/infra_diesel/repository.rs#L178-L187)
  and [initial scheduling:22–30](../libs/commanding/src/infra_diesel/conversions.rs#L22-L30).
- Eventing: [retry scheduling:196](../libs/eventing/src/infra_diesel/repository.rs#L196)
  and [initial scheduling:15](../libs/eventing/src/infra_diesel/conversions.rs#L15).
- Inbox: [retry scheduling:192](../libs/inbox/src/infra_diesel/repository.rs#L192)
  and [initial scheduling:10](../libs/inbox/src/infra_diesel/conversions.rs#L10).
- Outbox: [retry scheduling:189](../libs/outbox/src/infra_diesel/repository.rs#L189)
  and [initial scheduling:54](../libs/outbox/src/application/record_events.rs#L54).

In all four queues, initial scheduling and explicit-failure backoff use application
`Utc::now()`, while reservation eligibility uses PostgreSQL `now()`. An application
clock ahead of PostgreSQL delays immediately submitted entries and retries.
A clock behind PostgreSQL by more than the backoff makes failed entries
immediately eligible, allowing attempts to exhaust rapidly.

The sweep path already uses database time, so the scheduling paths are
inconsistent. The [parity plan](queue-commanding-parity-implementation-plan.md)
kept explicit-failure paths on the application clock as out of scope, so this is
deferred work rather than an oversight.

**Evidence:** source inspection; no live clock-skew/database experiment was run.

**Fix direction:** calculate persisted scheduling timestamps using the same
database clock as reservation eligibility.

## Test reliability findings

### 9. [P2] An inbox AMQP test invokes undefined behavior

**Location:**
[AMQP test:158–176](../libs/inbox/src/amqp_consumption.rs#L158-L176).

The test zero-initializes `Delivery`, initializes fields other than `acker`, and
then calls `assume_init()`. The remaining `Acker` contains invalid zeroed
`Arc`-backed fields. Producing that value is undefined behavior; calling
`mem::forget` afterward does not repair it.

**Evidence:** inspected against the pinned Lapin implementation. Miri was not
available. Lapin's public `Delivery::mock` was successfully used in the separate
message-ID reproduction probe.

**Fix direction:** use `Delivery::mock`, assign the required properties, and remove
the unsafe initialization and deliberate leak.

### 10. [P2] Outbox database tests can destroy each other's fixtures

**Locations:**
[storage tests:81–88](../libs/outbox/tests/diesel_storage.rs#L81-L88) and
[lifecycle tests:71–78](../libs/outbox/tests/lifecycle.rs#L71-L78).

The mutex protects only table recreation and is released before the remainder
of each test. With the ignored database tests enabled under normal parallel test
execution, one test can drop/recreate `outbox_entries` while another inserts,
reserves, or inspects rows.

**Evidence:** source inspection of lock lifetimes and shared fixtures. The
database suite was not executed in this review's environment. A separate run on a
disposable PostgreSQL database reproduced the problem: six of these eight tests
failed in parallel with `relation "outbox_entries" does not exist`, and all eight
passed with `--test-threads=1`.

**Fix direction:** use per-test schemas and pools, as the newer timeout-sweep
fixtures do, or retain a shared guard throughout each test.

## Validation results and limitations

| Check                                                | Result                                                      |
| ---------------------------------------------------- | ----------------------------------------------------------- |
| Library tests                                        | 193 passed after excluding two sandbox-blocked socket tests |
| Kernel tests                                         | 10 passed                                                   |
| Combined HTTP adapter tests                          | 56 passed                                                   |
| Total passing tests                                  | **259**                                                     |
| HTTP feature-isolation and AMQP feature combinations | Passed                                                      |
| Library Clippy, all targets and features             | Completed with warnings                                     |
| Prelude naming script                                | Passed; does not detect all API findings above              |
| Kernel formatting check                              | Passed                                                      |

The initial all-feature library test run failed in two tests because socket
binding was denied by the sandbox, not because an application assertion failed:

- `unresponsive_broker_times_out_and_reconnects`
- `failed_connections_report_transport_errors_and_reconnect`

The remaining suite passed after excluding those tests. There were also
**34 ignored tests/doctests**: 20 database and broker tests and 14 doc examples.
This review's environment did not run the database and broker tests, so
source-based findings must not be mistaken for successful integration-test
reproductions.

A separate run, during the alignment work that became `1dfeaae`, executed those 20
tests against real PostgreSQL and RabbitMQ. All 20 passed, 6 of them only with
`--test-threads=1` (finding 10). That run shows the existing tests pass; it does
not reproduce the failure scenarios in the findings above.

Main commands used, from the repository root:

```bash
cargo test --locked --offline --workspace --all-features \
  --manifest-path libs/Cargo.toml -- \
  --skip unresponsive_broker_times_out_and_reconnects \
  --skip failed_connections_report_transport_errors_and_reconnect

cargo test --locked --offline --all-features \
  --manifest-path kernel/Cargo.toml

cargo test --locked --offline \
  --manifest-path test_apps/http_adapters/Cargo.toml

CARGO_NET_OFFLINE=true python3 scripts/check_http_features.py

cargo clippy --locked --offline --workspace --all-targets --all-features \
  --manifest-path libs/Cargo.toml

python3 scripts/check_prelude.py
cargo fmt --all --check --manifest-path kernel/Cargo.toml
```

The focused probes were temporary review artifacts, not committed regression
tests. Their scenarios should become permanent tests when the corresponding
issues are addressed.

## Boundaries and recommended order

No additional high-confidence defects were found in `mulac_diesel`. This is not a
claim that it is defect-free.

`mulac_http` has defects that this review missed. Its Poem `RunRequest` parsing
accepts the JSON array `[5]` as `limit = 5`, and reports
`expected struct RunRequestData` for non-object bodies. See findings 7 and 8 in
[the Claude review](review_libs_claude.md).

Documented choices were not treated as standalone defects: non-atomic handoffs,
at-least-once delivery, partial-success batch responses, and application-owned
sweep scheduling. Findings about identity and reservation handling describe
specific consequences beyond merely observing those choices.

Recommended order:

1. Correct reservation isolation, AMQP queue durability/transport identity, and
   event and command identity across retries.
2. Enforce publication deadlines.
3. Repair public prelude APIs and unify scheduling clocks.
4. Remove test undefined behavior, isolate database fixtures, and add regression
   coverage for the failure scenarios above.
