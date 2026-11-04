# test_app_twitter

A Twitter-like reference application that exercises the Mulac command/event framework with Diesel persistence.

## Overview

`test_app_twitter` mirrors `test_app_todo` in architecture but uses Diesel instead of SQLx for all database operations. It demonstrates:

- Flat feature modules, each with exactly one `pub mod prelude` facade
- App-owned `AppState { pool, mulac }` with two-phased command/event gateways
- App-owned `inbox_messages` and `outbox_messages` tables separate from kernel `*_entries` tables
- Idempotent outbox with PK reuse (`outbox_messages.id == source event_id`)
- Timeline fan-out via an event subscriber that dispatches internal commands

## Source layout

```
test_app_twitter/
  Cargo.toml
  Makefile
  AGENTS.md                          — detailed agent/developer reference
  docker-compose.yml
  migrations/
    2025-01-01-000001_infrastructure/ — kernel tables
    2025-01-01-000002_app_tables/     — domain tables
    2026-05-23-000001_app_messages/   — app inbox/outbox tables
  src/
    lib.rs                            — TwitterEvent, AppState, pub mod prelude
    schema.rs                         — Diesel schema (auto-generated)
    assembly/
      application.rs                  — errors, AppCommand, MulacState, start_mulac
      domain.rs                       — Clock, DTOs, InboundEntity
      infra_diesel.rs                 — DbPool, migrations, OutboxSubscriber
      bin/twitterapp.rs               — serve / migrate binary
    tweet_post.rs, tweet_delete.rs, tweet_retweet.rs
    user_follow.rs, user_unfollow.rs
    tweet_like.rs, tweet_unlike.rs
    direct_message_send.rs
    timeline_fan_out.rs
    inbox.rs, outbox.rs
  tests/
    utils.rs                          — reqwest harness, row structs, helpers
    tweet_post.rs, ..., outbox.rs     — one file per feature + inbox + outbox
```

## Running locally

```bash
make up        # start Postgres on port 5433
make migrate   # apply migrations
make serve     # start server on 127.0.0.1:33002
make test      # run integration tests
make reset     # wipe and re-migrate
```

Environment variables:
- `DATABASE_URL` — default: `postgres://twitter:twitter@localhost:5433/twitter`
- `BIND_ADDR` — default: `127.0.0.1:33002`

## REST API

| Method | Path                    | Response                  |
| ------ | ----------------------- | ------------------------- |
| GET    | /health                 | `ok`                      |
| GET    | /swagger                | Swagger UI                |
| POST   | /api/tweets             | `TweetDto`                |
| DELETE | /api/tweets/:id         | `204 No Content`          |
| POST   | /api/tweets/:id/retweet | `TweetDto`                |
| POST   | /api/users/follow       | `FollowDto`               |
| POST   | /api/users/unfollow     | `FollowDto` (synthesized) |
| POST   | /api/tweets/:id/like    | `LikeDto`                 |
| DELETE | /api/tweets/:id/like    | `204 No Content`          |
| POST   | /api/messages/direct    | `DirectMessageDto`        |
| POST   | /api/messages/commands  | `InboundResponse`         |
| GET    | /api/messages/outbox    | `OutboxList`              |

Errors: `{ "error": "..." }` with HTTP 400 / 404 / 409 / 500.

## Inbox and outbox

**`inbox_messages`** — records every command envelope received at `POST /api/messages/commands`.
- `status`: `received → processed` or `received → failed`
- Duplicate `id` returns `409 Conflict`

**`outbox_messages`** — app-level journal of published events.
- `id` equals the originating kernel `event_id` (PK-reuse idempotency)
- `status`: `pending` (AMQP publishing is not implemented; see deferred items)
- Payload shape: `{"type": "TweetPosted", "payload": { ... }}`

**Do not confuse with kernel tables:**
- `command_entries` / `event_entries` — kernel write-side journal
- `inbox_entries` / `outbox_entries` — kernel transport tables (not used for app-level messaging)

## Persistence

Diesel 2.x with PostgreSQL. All handlers and infra functions are synchronous — Diesel does not support async. All Diesel calls in async poem handlers are wrapped in `tokio::task::spawn_blocking` via the `run_blocking` helper in `assembly/application.rs`.

## Deferred features

- **Timeline read API** (`GET /api/timeline/:user_id`) — not yet implemented.
- **Mention notifications** — not yet implemented; identity model (handle text vs user-profile table) must be decided first.

See `AGENTS.md` for the full list of intentional divergences from `test_app_todo`.
