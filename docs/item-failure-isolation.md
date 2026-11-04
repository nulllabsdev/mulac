# Item-based queue failure isolation

Applies to the supplied commanding, eventing, inbox, and outbox consumers and
Diesel/PostgreSQL repositories. Public port signatures and status codes are
unchanged; no database migration is required.

## Reservation checkpoint

1. Select at most the requested limit of eligible entries using
   `FOR UPDATE SKIP LOCKED`. Eligibility still requires a due `scheduled_at`,
   `Received` or `Failed` status, and `attempts < max_attempts` (default 6).
2. In **one transaction**, assign reservation IDs, increment each selected
   entry's attempt count once, and load JSONB metadata and diagnostics as text.
   Typed JSON decoding must not happen inside Diesel's batch load.
3. Decode each selected row independently. A malformed row becomes `Failed`,
   its reservation fields are cleared, and its ID, reservation ID, stage, and
   failure category are appended to its diagnostics on the same connection.
   Healthy rows remain reserved and are returned only after commit.
4. Failure to load raw rows, persist a malformed row's failure, or commit causes
   the reservation call to fail; transactional changes are rolled back where
   PostgreSQL can establish rollback. No healthy batch is returned on failure.

A bad row does not discard its healthy neighbours. There is no refill loop:
`limit` bounds **candidates**, not successful decodes. `reserve` may return fewer
entries, including zero after failing malformed candidates. Consequently a zero
consumer count does not prove the queue was empty. Reservation-stage failures
are recorded in the database, not in the returned consumer error list.

Original metadata remains untouched for inspection. Existing valid diagnostic
history and unknown fields are retained when appending an error. Invalid
`extra_info` shapes are retained under `quarantined_extra_info` alongside a
canonical `errors` array; JSON outside serde_json's numeric range is retained
as text rather than making the whole batch unreadable.

## Per-item execution checkpoint

1. Capture the entry and reservation IDs before conversion or dispatch.
2. Run conversion and handler/subscriber/command handoff/publication inside an
   item-local unwind boundary.
3. On success, call `completed` inside another unwind boundary.
4. On any processing or completion error, including a recoverable unwinding
   panic, attempt `failed` with item/reservation/stage context. Panic payloads
   are not copied into diagnostics or returned errors. The host's panic hook
   still runs and controls its own logging.
5. Contain a failure-finalization panic too. Return both the original error and
   any failure-finalization error; continue with the remaining items.

Completion and failure writes require the matching ID, reservation ID, and
`Reserved` status. A late failure must not overwrite a completed entry or a
new owner's reservation. A database acknowledgement failure can mean completion
already committed; failure finalization then correctly rejects the stale token.

Successful batches retain `Ok(count)`. Batches with execution/finalization
errors retain `Err(Vec<Error>)`; healthy items still finish. The existing HTTP
bridge reports those errors by logged ID. Direct gateway calls outside batch
consumers do not gain a new panic boundary.

## Failed, not Dead

All automatic failure paths, including malformed reservations, outbox
conversion failures, exhausted attempts, and stale-reservation sweeps, use
`Failed`. `Dead` remains a compatible status code for existing data and explicit
operator decisions; outbox's explicit `dead()` port remains available but is
not called by the consumer.

Failure recording and sweeps do not increment attempts; reservation does.
Failed entries retry only while under the reservation's configured attempt cap.
Exhausted entries stay `Failed` for inspection and operator-directed repair or
requeue; this is **not** an infinite retry policy. There is no new automatic
repair or operator API in this change. Keep worker attempt limits consistent.

Failure backoff uses PostgreSQL time:

| Queue                | `failed()` and malformed-row delay                               |
| -------------------- | ---------------------------------------------------------------- |
| Commanding, eventing | `min(max(attempts, 1) × 30s, 120s)`                              |
| Inbox                | `max(attempts, 1) × 30s`                                         |
| Outbox               | Same capped delay; exhausted rows retain their previous schedule |

Existing sweep timing is retained: commanding/eventing/inbox use uncapped
`attempts × 30s`; outbox uses the capped rule and preserves exhausted schedules.

Writes also validate metadata before accessing the database. In particular,
direct `OutboxStorePort::record` calls now reject sequences outside
`1..=i64::MAX`, just like the recorder and deserializer. Absent sequences remain
valid. Command/event metadata serialization errors are propagated, not silently
discarded.

## Recovery limits and operations

During normal execution, each returned item reaches `Completed` or `Failed`
provided its owner can persist finalization. This is **not** a guarantee that
only a database crash can leave `Reserved` rows. Connection/permission errors,
failed custom repository finalization, process termination, cancellation,
`panic=abort`, double panics during unwinding, or indefinitely blocked work can
prevent finalization. Unwinding does not undo side effects or repair poisoned
shared collaborator state. Handlers and publishers must remain safe to reuse.

Applications must still schedule sweepers to recover abandoned reservations;
the kernel does not start one automatically. Choose timeouts that do not reclaim
healthy long-running work. No work timeout is introduced here. Preserve
at-least-once/idempotent processing: a failed completion can cause delivery again.

Monitor `Failed` rows, their attempts and diagnostics, plus finalization errors
and stale `Reserved` rows. Investigate and repair malformed data before explicitly
requeueing it. Do not assume `Failed` means another retry will occur.

## Regression coverage

- Consumer tests: first item errors/panics during processing, completion, or
  failure finalization; later healthy items still complete; panic payloads stay
  out of returned diagnostics.
- Raw decode tests: malformed metadata/diagnostics, invalid sequences, optional
  legacy metadata, and JSON numbers outside serde_json's supported range.
- Write tests: direct outbox invalid sequences are rejected before acquiring a
  connection; valid boundary sequences round-trip.
- PostgreSQL tests: mixed batches, ownership protection, failure/update/load/
  commit rollback, bounded candidates, retry exhaustion, and concurrent claims.
  Each new integration test owns an isolated schema, not shared application tables.

Run the new database tests against a disposable PostgreSQL database with schema
creation privileges (they are ignored by default):

```sh
DATABASE_URL_TEST=postgres://... \
  cargo test --manifest-path libs/Cargo.toml --all-features \
  -p mulac-commanding -p mulac-eventing -p mulac-inbox \
  --test reservation_isolation -- --ignored
OUTBOX_TEST_DATABASE_URL=postgres://... \
  cargo test --manifest-path libs/Cargo.toml --all-features \
  -p mulac-outbox --test reservation_isolation -- --ignored
```
