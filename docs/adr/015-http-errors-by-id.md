# ADR-015: Report HTTP errors by logged ID

**Status:** Accepted; changes the error text in ADR-009, ADR-010 and ADR-014

## Context

HTTP responses revealed internal details in two ways. Parser errors named private
structs such as `RawRunOutboxRequest`, and the `errors` array of a batch response
carried raw error text, which could include database or broker details. Server
failures had the opposite problem: their fixed text was safe, but the library never
logged the cause, so operators had nothing to look up.

## Decision

Any error text we did not write as a message for clients becomes an error ID, and
that ID is logged with the real error.

| Case                                                       | Response                                                     | Logged at  |
| ---------------------------------------------------------- | ------------------------------------------------------------ | ---------- |
| Unparsable body, such as `{"limit":"x"}`, `{}` or `5`      | `400` with the ID                                            | `warn`     |
| Batch item failure                                         | `200` with one ID per failure in `errors`, in the same order | `error`    |
| Failed blocking task or inbox storage failure              | `500` with the ID                                            | `error`    |
| Limit out of range, or inbox field validation              | `400` with our message, unchanged                            | not logged |
| No outbox publisher configured                             | `503` with our message, unchanged                            | not logged |
| Framework rejection: JSON syntax, content type, body limit | Framework text, unchanged                                    | not logged |

Status codes and response shapes do not change. On Axum the body is exactly the ID.
Poem wraps parse failures itself, so a malformed body there reads
`parse request payload error: failed to parse "RunRequest": <id>`. That prefix names
only the public schema.

IDs are UUID v7 values created by `mulac_http`. `ReportedError` displays only the ID
and keeps the cause as `Error::source`, and `report_batch_error` does the same job for
batch items. Each log event has the fields `error_id`, `queue` and `error`, so searching
logs for an ID finds the cause.

## Consequences

**This is a breaking HTTP change.** Clients that read `errors` text or match
`queue processing task failed`, `inbox submission task failed` or
`inbox message could not be stored` will now see IDs. `offers` is not in this
repository, so check it before publishing.

**This is also a breaking Rust change.** `MalformedRequest`, `RunTaskFailed`,
`StorageFailed` and `SubmissionTaskFailed` now hold `ReportedError`. Patterns using
`(_)` still compile. `api` now depends on `mulac_http`, which pulls in no framework or
runtime when its features are off.

**Hosts must install a `tracing` subscriber,** or the IDs lead nowhere. Log filters also
matter: `EnvFilter::from_default_env()` shows only `error` when `RUST_LOG` is unset, so
malformed-body IDs need `RUST_LOG=warn` or lower.

**Logs now need the access control that responses no longer have to provide.** They hold
raw database text and fragments of client input. Unauthenticated clients can also create
`warn` lines by sending bad bodies, so hosts' authentication and rate limits matter more.

Some things are unchanged. Stored `extra_info.errors` still holds the reason text without
an ID, and kernel workers log failures without IDs because they have no client to report to.

This resolves findings 7 and 9 in
[the libraries review](../review_libraries_unified.md). JSON arrays being accepted as run
requests (finding 8) is a separate issue and remains open.

## References

- ADR-009: Shared Inbox HTTP API
- ADR-010: Commanding, Eventing and Outbox HTTP APIs
- ADR-014: Merge the other queues' execution bridges
