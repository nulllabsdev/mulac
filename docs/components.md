# Component High-Level View

This document describes the core mulac components at the architecture level. It focuses on responsibilities, lifecycle models, and reliability boundaries (not implementation details).

See [item failure isolation](item-failure-isolation.md) for reservation atomicity, per-item panic containment, and recovery limits.

## Inbox

**Role:**
The inbox is the entry point for messages coming from outside the system boundary. It accepts inbound messages, stores them durably, and exposes them to internal command processing.

**Behaviour:**
1. An external transport (for example AMQP) delivers an inbound message to the inbox.
2. The inbox stores the message durably as an `InboxEntry`.
3. An inbox consumer reserves the `InboxEntry` for processing.
4. The inbox consumer transforms the entry into a `CommandEnvelope` and hands it off to the command dispatcher.
5. After successful handoff, the inbox marks the entry as `completed`.

**Model:**
The main model is `InboxEntry`, a durable representation of an external message inside the system boundary.

**States:**
1. `received` - the message was accepted and stored.
2. `reserved` - an inbox consumer claimed the entry for processing.
3. `completed` - the entry was successfully handed off to command processing.
4. `failed` - handoff failed and the entry is retained for inspection; eligible for scheduled retry only below the attempt cap.
5. `dead` - legacy/manual terminal state; automatic failures remain `failed`.

**Rules:**
1. A message should be acknowledged only after durable storage.
2. A reserved entry should not be processed by more than one consumer at the same time.
3. Failed entries are retried with backoff; inbox-triggered command handling must tolerate at-least-once execution.
4. At the configured attempt cap, the entry remains `failed` for inspection and is no longer eligible for automatic retry.
5. Completion means responsibility moved from inbox processing to command processing.

## Command Dispatcher

**Role:**
The command dispatcher accepts `CommandEnvelope` instances and routes commands to a command handler. It comes in two variants:

- **Direct:** calls the handler immediately.
- **TwoPhased:** persists a durable `CommandEntry` and relies on a command-queue consumer to execute it.

**Behaviour:**
1. A caller (application code or the inbox consumer) sends a `CommandEnvelope` to the dispatcher.
2. Direct: the dispatcher resolves the command handler and invokes it.
3. TwoPhased: the dispatcher stores a `CommandEntry` in the command queue.
4. TwoPhased: a command-queue consumer reserves the `CommandEntry`, invokes the handler, and produces events.
5. Produced events are wrapped into `EventEnvelope` instances and handed off to the event dispatcher.
6. TwoPhased: after successful handoff, the `CommandEntry` is marked as `completed`.

**Model:**
- Both variants: `CommandEnvelope` (in-flight command + metadata).
- TwoPhased only: `CommandEntry` (durable queued form used for retries).

**States:**
The Direct variant has no persistent state model; execution is synchronous and in-flight only.

Use `CommandEntry` lifecycle states when using the TwoPhased variant (`received/reserved/completed/failed/dead`).

**Rules:**
1. A `CommandEnvelope` should resolve to exactly one command handler.
2. TwoPhased dispatch is at-least-once; command handlers must tolerate duplicate execution (or implement deduplication).
3. TwoPhased: if execution succeeds but handoff to event dispatch fails, the command entry is retried (which can repeat command handling).
4. Direct: if the handler succeeds but inline handoff to the event dispatcher fails, the error propagates to the caller; no retry record exists and any handler side-effects remain in place.
5. Completion means responsibility moved from command processing to event dispatching.

## Event Dispatcher

**Role:**
The event dispatcher delivers `EventEnvelope` instances to subscribers. It comes in two variants:

- **Direct:** delivers to subscribers immediately.
- **TwoPhased:** persists a durable `EventEntry` and relies on an event-queue consumer to deliver it.

**Behaviour:**
1. The command dispatcher provides `EventEnvelope` instances to the event dispatcher.
2. Direct: the dispatcher resolves subscribers and delivers the event to each subscriber.
3. TwoPhased: the dispatcher stores an `EventEntry` in the event queue.
4. TwoPhased: an event-queue consumer reserves the `EventEntry`, resolves subscribers, and delivers the event.
5. TwoPhased: after successful delivery to all subscribers, the `EventEntry` is marked as `completed`.

**Model:**
- Both variants: `EventEnvelope` (in-flight event + metadata).
- TwoPhased only: `EventEntry` (durable queued form used for retries).

**States:**
The Direct variant has no persistent state model; execution is synchronous and in-flight only.

Use `EventEntry` lifecycle states when using the TwoPhased variant (`received/reserved/completed/failed/dead`).

**Rules:**
1. An event may resolve to zero or more subscribers.
2. Delivery is at-least-once; subscribers must tolerate duplicates.
3. Direct: partial delivery is possible if one subscriber fails after others have succeeded; the error propagates to the caller and retrying is the caller's responsibility — a caller-initiated retry may re-invoke already-successful subscribers.
4. TwoPhased: partial delivery is possible; the dispatcher's own retry loop can automatically re-invoke subscribers that already succeeded.
5. Completion means responsibility moved from event dispatching to subscriber processing.

## Outbox

**Role:**
The outbox is the exit point for messages that need to leave the system boundary. It acts as an event subscriber that persists outbound work durably and publishes it to an external transport.

**Behaviour:**
1. The outbox receives an event from the event dispatcher as a subscriber.
2. The outbox stores the event durably as an `OutboxEntry`.
3. An outbox consumer reserves the `OutboxEntry` for delivery.
4. The outbox consumer publishes the outbound message to an external transport (for example AMQP) and waits for broker acceptance.
5. After acceptance, the outbox marks the entry as `completed`.

**Model:**
The main model is `OutboxEntry`, a durable representation of outbound work.

**States:**
1. `received` - the event was accepted and stored.
2. `reserved` - an outbox consumer claimed the entry for delivery.
3. `completed` - the broker accepted the outbound message.
4. `failed` - delivery failed and the entry is retained for inspection; eligible for scheduled retry only below the attempt cap.
5. `dead` - explicit administrative terminal state; automatic failures remain `failed`.

**Rules:**
1. The outbox should consider an event accepted only after it has been stored durably.
2. A reserved entry should not be processed by more than one consumer at the same time.
3. If broker acceptance succeeds but marking `completed` fails, the entry can be retried and the outbound message may be published again; external consumers must tolerate duplicates.
4. Both transport and transformation failures use `failed`; the attempt cap bounds retries without removing failure evidence.
5. Completion means responsibility moved from the outbox to the external transport.

## Document Metadata

| Version | Author               | Reviewers            | Summary                                       | Date       |
| ------- | -------------------- | -------------------- | --------------------------------------------- | ---------- |
| 0.1.0   | Miro, Claude & Codex | Miro, Claude & Codex | High-level mulac components overview document | 2026-04-14 |
