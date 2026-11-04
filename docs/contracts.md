# Component Contracts

This document defines the interface contracts for each mulac component. Each contract specifies what a component accepts, what it produces, and what it guarantees to callers and collaborators.

For component responsibilities and lifecycle rules, see [components.md](components.md).

## Entry retry policy

This policy applies to every durable queue-backed entry type (`InboxEntry`,
`CommandEntry`, `EventEntry`, `OutboxEntry`). See the detailed
[item-based failure isolation contract](item-failure-isolation.md).

- Reservation increments attempts, in one transaction with per-row decoding and
  failure persistence. Malformed candidates become `Failed`; healthy entries
  are returned after commit. The limit bounds candidates, not returned entries.
- Each consumer item catches recoverable unwinding panics. Processing or
  completion failure attempts `failed`; failure-finalization errors are returned
  too, without preventing later items from being processed.
- `failed` and stale-reservation sweeps always select `Failed`, including at
  exhaustion. They clear reservation fields and do not increment attempts.
- Reservation requires `attempts < max_attempts` (default 6). Exhausted `Failed`
  entries await operator inspection/repair/requeue, not infinite automatic retries.
- Failure backoff is linear: `max(attempts, 1) × 30s`, capped at 120s except inbox.
  Outbox preserves exhausted schedules. Sweeps retain their existing timing.
- Sweeping is application-owned; no automatic kernel worker is installed.
  Database/repository failures, process termination, aborting panics, and hung
  work can still leave reservations needing recovery. Side effects are not
  rolled back; handlers and publishers must tolerate at-least-once delivery.
- `Dead` remains a legacy/manual status, not an automatic failure outcome.

---

## Inbox

**Accepts:**
An `InboundMessage` containing:
- A UTF-8 text payload; non-UTF-8 deliveries are rejected at the transport boundary and dead-lettered without retry
- Required transport metadata: stable message ID, source identifier, received-at timestamp

**Produces:**
An `InboxEntry` — a durable record containing the raw payload and transport metadata.

**Consumer-facing operations:**
- `reserve(count) → InboxEntry[]` — atomically selects up to `count` eligible `received`/`failed` candidates, fails malformed rows and returns healthy `reserved` entries; a `reserved` entry cannot be claimed by another consumer
- `completed(entry_id)` — signals successful processing; the Inbox transitions the entry to `completed`
- `failed(entry_id)` — signals processing failure; the Inbox transitions the entry to `failed` and schedules it for retry
- a call to `completed` or `failed` for an entry no longer owned by the caller (released by the sweep) is rejected

**Deduplication:**
Duplicate detection is active for a configurable retention window (default: 24 hours). A message whose ID was already seen from the same source within this window is silently discarded; messages outside the window are accepted as new.

**Stale reservation sweep:**
The application must schedule a sweeper. It releases reservations older than its configured timeout to `failed`, preserving attempts and clearing reservation fields. Entries at the attempt cap are not automatically retried.

**Guarantees:**
- Acceptance is signalled to the caller only after the `InboxEntry` is durably stored
- A message whose ID was already seen from the same source within the deduplication retention window will not produce a new `InboxEntry`; messages outside the window or from a different source are accepted as new
- A `reserved` entry will not be concurrently claimed by another consumer
- Delivery to consumers is at-least-once; the same entry may be surfaced more than once (after a failed report or a stale reservation release)

**Requires from collaborators:**
- Transport adapter must supply a stable, unique message ID with each `InboundMessage`
- Transport adapter must not acknowledge delivery to the broker until the Inbox signals acceptance
- Inbox consumer must tolerate at-least-once delivery (duplicate `InboxEntry` processing must be safe)
- Inbox consumer must call `completed` or `failed` for every reserved entry; unreported entries will be released by the sweep and retried

---

## Command Dispatcher

**Accepts:**
A `CommandEnvelope` containing:
- The command payload
- Required metadata: command type, correlation ID, causation ID, created-at timestamp
- Additional caller-supplied metadata (open, no prescribed shape)

The `command type` field is the routing key used to resolve the handler; it is preserved in the durable `CommandEntry` record.

**Produces:**
- Direct: `EventEnvelope` instances handed off inline to the Event Dispatcher
- TwoPhased: a durable `CommandEntry`; a queue consumer later invokes the handler and produces `EventEnvelope` instances handed off to the Event Dispatcher

**Consumer-facing operations (TwoPhased only):**
- `reserve(count) → CommandEntry[]` — atomically selects up to `count` eligible `received`/`failed` candidates, fails malformed rows and returns healthy `reserved` entries; a `reserved` entry cannot be claimed by another consumer
- `completed(entry_id)` — signals successful processing; the dispatcher transitions the entry to `completed`
- `failed(entry_id)` — signals processing failure; the dispatcher transitions the entry to `failed` and schedules it for retry
- a call to `completed` or `failed` for an entry no longer owned by the caller (released by the sweep) is rejected

**Stale reservation sweep (TwoPhased only):**
The application must schedule a sweeper. It releases reservations older than its configured timeout to `failed`, preserving attempts and clearing reservation fields. Entries at the attempt cap are not automatically retried.

**Guarantees:**
- Direct: the command is executed at most once; if the handler fails, or if the handler succeeds but inline handoff to the Event Dispatcher fails afterward, the error is propagated to the caller and retrying is the caller's responsibility
- Direct: if inline handoff to the Event Dispatcher fails after the handler has already executed, handler side-effects are not rolled back and the produced `EventEnvelope` instances are not retained for automatic redelivery
- TwoPhased: acceptance is signalled only after the `CommandEntry` is durably stored
- TwoPhased: execution is at-least-once; the same `CommandEntry` may be processed more than once (after a failed report or a stale reservation release)
- TwoPhased: a `reserved` `CommandEntry` will not be concurrently claimed by another consumer

**Requires from collaborators:**
- Caller must provide a well-formed `CommandEnvelope` with all required metadata fields
- Caller must ensure a handler is registered for the command type before dispatching; an unroutable command is a caller error
- Command handler must be idempotent regardless of variant
- Event Dispatcher must be available to accept handoff; if it is not, TwoPhased retries the `CommandEntry` which can re-invoke the handler — making handler idempotency essential

---

## Event Dispatcher

**Accepts:**
An `EventEnvelope` containing:
- The event payload
- Required metadata: event type, routing key, correlation ID, causation ID, created-at timestamp
- Additional caller-supplied metadata (open, no prescribed shape)

The `routing key` is supplied by the event producer and used by downstream subscribers (such as the Outbox) to resolve the outbound transport destination.

**Produces:**
- Direct: delivers the `EventEnvelope` inline to all resolved subscribers; if one subscriber fails the error propagates and already-invoked subscribers are not rolled back
- TwoPhased: a durable `EventEntry`; a queue consumer later delivers to all subscribers as a unit; retry can re-invoke already-successful subscribers

**Consumer-facing operations (TwoPhased only):**
- `reserve(count) → EventEntry[]` — atomically selects up to `count` eligible `received`/`failed` candidates, fails malformed rows and returns healthy `reserved` entries; a `reserved` entry cannot be claimed by another consumer
- `completed(entry_id)` — signals successful delivery to all subscribers; the dispatcher transitions the entry to `completed`
- `failed(entry_id)` — signals delivery failure; the dispatcher transitions the entry to `failed` and schedules it for retry
- a call to `completed` or `failed` for an entry no longer owned by the caller (released by the sweep) is rejected

**Stale reservation sweep (TwoPhased only):**
The application must schedule a sweeper. It releases reservations older than its configured timeout to `failed`, preserving attempts and clearing reservation fields. Entries at the attempt cap are not automatically retried.

**Guarantees:**
- Direct: each subscriber is invoked at most once per dispatch; if one subscriber fails the error propagates to the caller and retrying is the caller's responsibility; already-invoked subscribers are not rolled back
- TwoPhased: acceptance is signalled only after the `EventEntry` is durably stored
- TwoPhased: delivery is at-least-once; the same `EventEntry` may be delivered more than once (after a failed report or a stale reservation release)
- TwoPhased: if delivery to one subscriber fails after others have succeeded, a retry will re-invoke all subscribers — this is a known partial delivery hazard
- TwoPhased: a `reserved` `EventEntry` will not be concurrently claimed by another consumer
- Zero subscribers for an event type is a valid, expected outcome; the `EventEntry` is marked `completed` silently

**Requires from collaborators:**
- Caller must provide a well-formed `EventEnvelope` with all required metadata fields; zero subscribers for the given event type is not a caller error
- Subscribers must tolerate at-least-once delivery
- Subscribers must not assume anything about whether other subscribers have been invoked; each subscriber operates independently

---

## Outbox

**Accepts:**
An `EventEnvelope` — received as a subscriber of the Event Dispatcher; the Outbox owns the transformation to an outbound message for the external transport. The Outbox uses the `routing key` field from the `EventEnvelope` metadata to resolve the outbound transport destination; a missing or unresolvable routing key is a caller error and the `EventEnvelope` is rejected at acceptance time.

**Transformation contract:**
- Each accepted `EventEnvelope` maps to exactly one outbound message; cardinality is always 1:1.
- Routing key errors are caught at acceptance before an `OutboxEntry` is created; the `EventEnvelope` is rejected and no durable record is written.
- Post-acceptance conversion/serialisation errors use `failed(entry_id)`, like other item failures, preserving evidence for operator inspection.
- Transport failures (broker unavailable, broker rejection after message delivery) follow the standard [entry retry policy](#entry-retry-policy).

**Produces:**
- Stage 1: a durable `OutboxEntry` — the event is stored durably upon acceptance
- Stage 2: an outbound message accepted by the external transport — the outbox consumer publishes the message and waits for broker acceptance

**Consumer-facing operations:**
- `reserve(count) → OutboxEntry[]` — atomically selects up to `count` eligible `received`/`failed` candidates, fails malformed rows and returns healthy `reserved` entries; a `reserved` entry cannot be claimed by another consumer
- `completed(entry_id)` — signals broker acceptance; the Outbox transitions the entry to `completed`
- `failed(entry_id)` — signals delivery failure; the Outbox transitions the entry to `failed` and schedules it for retry
- `dead(entry_id)` — explicit administrative transition retained for compatibility; automatic consumers do not call it
- a call to `completed`, `failed`, or `dead` for an entry no longer owned by the caller (released by the sweep) is rejected

**Stale reservation sweep:**
The application must schedule a sweeper. It releases reservations older than its configured timeout to `failed`, preserving attempts and clearing reservation fields. Entries at the attempt cap are not automatically retried.

**Guarantees:**
- Acceptance is signalled only after the `OutboxEntry` is durably stored
- Delivery to the external transport is at-least-once; if broker acceptance succeeds but the `completed` transition fails, the entry is retried and the outbound message may be published again
- A `reserved` `OutboxEntry` will not be concurrently claimed by another consumer
- No ordering guarantee across entries or after retries; delivery order is not guaranteed

**Requires from collaborators:**
- Event Dispatcher must not consider the event delivered until the Outbox confirms durable storage of the `OutboxEntry`
- Outbox consumer must call `completed` only after the broker has accepted the message; calling `completed` before acceptance risks message loss
- Outbox consumer calls `failed(entry_id)` for all processing errors, including conversion and transport failures
- Behaviour beyond broker acceptance is outside the Outbox contract boundary

---

## Document Metadata

| Version | Author               | Reviewers            | Summary                                                                                 | Date       |
| ------- | -------------------- | -------------------- | --------------------------------------------------------------------------------------- | ---------- |
| 0.1.0   | Miro, Claude & Codex | Miro, Claude & Codex | Initial component contracts for Inbox, Command Dispatcher, Event Dispatcher, and Outbox | 2026-04-14 |
