# Mulac architecture and engineering review

- **Date:** 2026-10-23
- **Reviewed commit:** `0b2f289`
- **Scope:** architecture, queue reliability, runtime lifecycle, HTTP adapters, and validation
- **Method:** parallel architecture, principal-engineering, and staff-engineering reviews, followed by consolidated findings and targeted verification
- **Changes:** no implementation files changed; this document records the review

## Overall assessment

**Mulac has solid component-level foundations, but I would not yet approve its default end-to-end setup for production.** The main concerns are silent no-op behavior, retry identity, and incomplete runtime ownership—not the overall architectural direction.

## Priority findings

### 1. P1 — Kernel setup can silently discard accepted work

Several public configuration paths install successful no-ops:

- `start()` exposes an inbox recorder backed by `NoopInboxStore`.
- Both startup paths replace `.outbox_subscriber(...)` registrations with `NoopEventSubscriber`.
- `start()` ignores factories registered through `.event_subscriber_with_command_gateway(...)`.

Consequently, recording or dispatch can succeed without persistence or subscriber execution. Persistent event processing can mark events completed without creating outbox entries.

**Recommendation:** wire real implementations or reject unsupported configurations at startup. Successful no-ops should be explicitly test-only.

References: [inbox setup](../kernel/src/lib.rs#L326), [outbox setup](../kernel/src/lib.rs#L360), [ignored factory](../kernel/src/lib.rs#L302).

**Verified:** a standalone probe observed zero factory/subscriber calls while event dispatch and inbox recording returned success.


































### 2. P1 — Inbox retries create different logical commands

Inbox-to-command conversion generates a fresh `command_id` every time. Command dispatch happens before inbox completion.

If command recording succeeds but inbox completion fails, retrying the same inbox entry creates another command with a different ID. Command-table deduplication therefore cannot prevent duplicate command records.

**Recommendation:** derive or persist a stable command identity for each inbox entry. Test the “dispatch succeeds, completion fails” recovery path.

References: [identity generation](../libs/inbox/src/inbox_consumer.rs#L202), [dispatch/completion boundary](../libs/inbox/src/inbox_consumer.rs#L263).

### 3. P1 — AMQP inbox cannot consume normally provisioned durable queues

Every AMQP inbox constructor redeclares its queue using default declaration options, including `durable=false`.

An existing durable queue fails declaration because its properties differ; a newly created queue is non-durable. Callers cannot configure or bypass this declaration.

**Recommendation:** support externally provisioned queues and explicit queue options.

Reference: [queue declaration](../libs/inbox/src/amqp_consumption.rs#L231).

### 4. P2 — Stale recovery strands exhausted entries

Inbox, commanding, and eventing sweepers always transition stale reservations to `Failed`, ignoring `max_attempts`.

When a worker crashes on its final attempt, sweeping produces an entry that:

- cannot be reserved again;
- never transitions to `Dead`;
- remains misleadingly classified as retryable failure.

**Recommendation:** apply the outbox implementation’s exhausted-attempt transition consistently across all queues.

References: [inbox](../libs/inbox/src/stale_reservation_sweep.rs#L172), [commanding](../libs/commanding/src/stale_command_sweep.rs#L128), [eventing](../libs/eventing/src/assembly/infra_diesel.rs#L467).

### 5. P2 — Kernel shutdown does not wait for workers

`PersistentKernelHandle::wait()` cancels a token and immediately returns. The direct handle’s `wait()` simply returns. Neither tracks or joins workers.

The example applications discard worker join handles and use this method as their shutdown barrier, despite describing graceful draining.

**Recommendation:** explicitly own and await worker tasks, or document cancellation-only semantics and require application-owned joins.

References: [persistent wait](../kernel/src/lib.rs#L533), [example worker startup](../test_apps/todo/src/assembly/bin/todoapp.rs#L62).

### 6. P2 — Poem’s OpenAPI schema contradicts failure responses

Failed batches serialize `"processed": null`, but the generated Poem schema declares an optional, **non-nullable** integer.

This affects all four Poem batch endpoints and can break schema validation or generated clients during failures.

**Recommendation:** declare nullability explicitly and validate actual response fixtures against generated schemas.

Reference: [response schema](../libs/mulac_poem/src/types.rs#L189).

**Verified:** the generated schema reports `nullable=false`; the actual response contains `processed:null`.

### 7. P2 — GitHub CI does not enforce the new quality gates

The workflow runs `make fmt`, `make check`, and `make test-libs`, not `make ci`.

Therefore:

- “Format check” rewrites files rather than rejecting formatting differences.
- Adapter dependency-boundary and isolated-feature checks are not enforced.
- The database/broker tests remain ignored.
- Kernel currently has no tests.

**Recommendation:** invoke `make ci` in GitHub Actions and add dedicated PostgreSQL/RabbitMQ jobs. Fix database-test isolation before enabling parallel execution.

Reference: [CI workflow](../.github/workflows/ci.yml#L23).

## Architectural decisions to address

### Domain changes and emitted events are not atomic

The dispatcher executes the handler, then dispatches returned events separately. Typed handlers receive no command metadata, and wrapped events receive fresh IDs on every execution.

A concrete failure scenario:

1. A handler commits a domain change.
2. The process crashes before recording its event.
3. The command retries.
4. An idempotent handler recognizes the existing change and returns no events.
5. The event is permanently missing.

The Twitter follow handler demonstrates that no-op retry pattern. This is an **architectural limitation**, not an accusation that Mulac promises exactly-once delivery.

**Recommendation:** introduce an execution-context and transactional event-recording seam, or provide a documented, tested durable-output replay pattern.

References: [dispatcher](../libs/commanding/src/dispatcher.rs#L108), [follow handler](../test_apps/twitter/src/user_follow.rs#L66).

### Queue acceptance and request completion need separate APIs

`PersistentKernelState::dispatch_command()` enqueues a command, then drains the **global** queues.

An unrelated command can cause the current request to fail. Conversely, a successful return need not mean the submitted command ran when backlog exceeds the configured drain rounds.

Separate enqueueing, queue processing, and command-specific completion semantics.

Reference: [dispatch implementation](../kernel/src/lib.rs#L488).

### Production safeguards need clearer defaults

- **Legacy publisher:** `AmqpPublisher` can treat returned/unroutable publication as success. This behavior is documented, but remains hazardous. Prefer `ConfirmedAmqpPublisher` and deprecate or prominently label the legacy implementation. [Source](../libs/outbox/src/amqp_publisher.rs#L73)
- **Admission control:** async services submit blocking work without bounding concurrent batches. Application-level concurrency limits are important. [Source](../libs/inbox_api/src/runtime.rs#L36)
- **Public API consistency:** kernel exposes competing root and `io` APIs, including two different `NewCommandEnvelope` types. Consolidate the facade before stabilizing it.
- **Documentation:** `contracts.md` describes retry counting, exponential backoff, and deduplication guarantees that differ from current implementation. Reliability contracts need one authoritative definition. [Source](contracts.md#L7)

## What is working well

- Clear separation between framework-free services and HTTP adapters.
- Useful storage and processing ports.
- Atomic `SKIP LOCKED` reservation and reservation-token fencing.
- Transactional failure updates.
- Stable outbound identity and a substantially stronger confirmed publisher.
- Shared adapter fixtures and feature-isolation checks.
- Explicit acknowledgment of at-least-once delivery.

**Outbox is currently more complete than the other queue lifecycle implementations.** Its stronger behavior should guide targeted improvements rather than a wholesale rewrite.

## Validation and recommended order

`make ci` passed after allowing socket-binding tests outside the sandbox:

- **198 tests passed**
- **9 infrastructure tests ignored**
- **8 documentation examples ignored**

Live PostgreSQL/RabbitMQ behavior was not exercised.

Recommended order:

1. Eliminate successful no-op kernel paths.
2. Preserve retry identity and settle domain/event transaction semantics.
3. Correct AMQP queue provisioning and exhausted-attempt recovery.
4. Implement explicit worker ownership and shutdown.
5. Enforce infrastructure tests and contract checks in CI.

**Bottom line:** keep the architecture, but prioritize reliable behavior and executable contracts over additional adapters or abstraction layers.
