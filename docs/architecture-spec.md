# mulac Architecture Spec

This document describes the end-to-end flows at a detailed, step-by-step level.

For component responsibilities, models, lifecycle states, and rules, see [components.md](components.md).

## Flows

> **Scope:** These flows describe the happy path only. Failure handling, retry scheduling, and stale reservation release are defined in [components.md](components.md) and [contracts.md](contracts.md).

### Regular flow (two-phased command dispatcher + two-phased event dispatcher)

1. A `CommandEnvelope` is sent to the command dispatcher.
2. The command dispatcher stores the `CommandEnvelope` as a `CommandEntry` in the command queue.
3. The command queue consumer reserves the `CommandEntry` and sends the command to the command handler.
4. The command handler executes the command and produces events.
5. The command queue consumer encapsulates the produced events into `EventEnvelope` instances and sends them to the event dispatcher. The `CommandEntry` is marked as `completed`.
6. The event dispatcher stores each `EventEnvelope` as an `EventEntry` in the event queue.
7. The event queue consumer reserves the `EventEntry` and delivers the event to all registered subscribers. The `EventEntry` is marked as `completed`.

### Externally triggered flow (Inbox + two-phased dispatchers)

1. An AMQP worker receives a message from an external system.
2. The AMQP worker sends the message to the Inbox.
3. The Inbox stores the message durably as an `InboxEntry`.
4. The inbox consumer reserves the `InboxEntry` and sends a `CommandEnvelope` to the command dispatcher. After successful handoff, the `InboxEntry` is marked as `completed`.
5. The command dispatcher stores the `CommandEnvelope` as a `CommandEntry` in the command queue.
6. The command queue consumer reserves the `CommandEntry` and sends the command to the command handler.
7. The command handler executes the command and produces events.
8. The command queue consumer encapsulates the produced events into `EventEnvelope` instances and sends them to the event dispatcher. The `CommandEntry` is marked as `completed`.
9. The event dispatcher stores each `EventEnvelope` as an `EventEntry` in the event queue.
10. The event queue consumer reserves the `EventEntry` and delivers the event to all registered subscribers. The `EventEntry` is marked as `completed`.
11. One of the subscribers can be the Outbox if outbound messages need to be sent to an external system.

### Full flow (Inbox + two-phased dispatchers + Outbox)

1. An AMQP worker picks up an incoming message and sends it to the Inbox. The AMQP worker will `ack` when the message is stored in the Inbox.
2. The Inbox converts the incoming message into an `InboxEntry` and stores it durably.
3. The inbox consumer reserves the `InboxEntry`, converts it to a `CommandEnvelope`, and sends it to the command dispatcher. After successful handoff, the `InboxEntry` is marked as `completed`.
4. The command dispatcher stores the `CommandEnvelope` as a `CommandEntry` in the command queue.
5. The command queue consumer reserves the `CommandEntry` and sends the command to the command handler.
6. The command handler executes the command and produces events.
7. The command queue consumer encapsulates the produced events into `EventEnvelope` instances, taking metadata from the `CommandEnvelope`, and sends them to the event dispatcher. The `CommandEntry` is marked as `completed`.
8. The event dispatcher stores each `EventEnvelope` as an `EventEntry` in the event queue.
9. The event queue consumer reserves the `EventEntry` and delivers the event to all registered subscribers; if one subscriber is the Outbox, the Outbox stores the event as an `OutboxEntry`. Once all subscribers have accepted the event, the `EventEntry` is marked as `completed`.
10. The outbox consumer reserves the `OutboxEntry` and sends the message to an AMQP producer. After broker acceptance, the `OutboxEntry` is marked as `completed`.

## Document Metadata

| Version | Author               | Reviewers            | Summary                              | Date       |
| ------- | -------------------- | -------------------- | ------------------------------------ | ---------- |
| 0.1.0   | Miro, Claude & Codex | Miro, Claude & Codex | Initial architecture flows for mulac | 2026-04-14 |
