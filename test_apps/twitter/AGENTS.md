# test_app_twitter — Agent Reference

## Purpose

`test_app_twitter` is a Twitter-like event-sourced application used to test and demonstrate the Mulac command/event framework. It mirrors `test_app_todo` in architecture but uses Diesel (not SQLx) for persistence.

---

## Source map

```
src/
  lib.rs                    — module declarations, TwitterEvent enum, AppState
  schema.rs                 — Diesel-generated table definitions (do not hand-edit)
  db.rs                     — re-exports from assembly::infra_diesel (kept for external compat)
  assembly/
    mod.rs                  — pub mod prelude facade
    application.rs          — AppError, AppCommand, MulacState, MulacHandle, start_mulac, workers
    domain.rs               — Clock, shared DTOs (TweetDto, FollowDto, etc.), InboundEntity
    infra_diesel.rs         — DbPool, migrations, OutboxSubscriber, shared fetch helpers
    bin/
      twitterapp.rs         — binary entry point (serve / migrate subcommands)
  tweet_post.rs             — POST /api/tweets
  tweet_delete.rs           — DELETE /api/tweets/:id
  tweet_retweet.rs          — POST /api/tweets/:id/retweet
  user_follow.rs            — POST /api/users/follow
  user_unfollow.rs          — POST /api/users/unfollow
  tweet_like.rs             — POST /api/tweets/:id/like
  tweet_unlike.rs           — DELETE /api/tweets/:id/like
  direct_message_send.rs    — POST /api/messages/direct
  timeline_fan_out.rs       — internal FanOutTweet command + TweetPosted subscriber
  inbox.rs                  — POST /api/messages/commands (inbox HTTP API)
  outbox.rs                 — GET /api/messages/outbox (outbox HTTP API)
tests/
  utils.rs                  — start_test_app(), row structs, DB helpers
  tweet_post.rs             — PostTweet tests
  tweet_delete.rs           — DeleteTweet tests
  tweet_retweet.rs          — Retweet tests
  user_follow.rs            — FollowUser tests
  user_unfollow.rs          — UnfollowUser tests
  tweet_like.rs             — LikeTweet tests
  tweet_unlike.rs           — UnlikeTweet tests
  direct_message_send.rs    — SendDirectMessage tests
  timeline_fan_out.rs       — FanOutTweet tests
  inbox.rs                  — Inbox HTTP API tests
  outbox.rs                 — Outbox HTTP API tests
migrations/
  2025-01-01-000001_infrastructure/   — kernel transport tables (inbox/command/event/outbox entries)
  2025-01-01-000002_app_tables/       — domain tables (tweets, follows, likes, direct_messages, timelines)
  2026-05-23-000001_app_messages/     — app-level inbox_messages and outbox_messages tables
```

---

## Feature module convention

Every feature file (e.g. `tweet_post.rs`) follows:

```
pub const COMMAND_NAME: &str = "...";
pub const EVENT_NAME:   &str = "...";

mod models     { Command, Event + kernel trait impls }
mod handler    { CommandHandlerPort impl }
mod infra_diesel { raw Diesel queries }
mod http       { poem-openapi Api + request types }

pub mod prelude { pub use super::{COMMAND_NAME, EVENT_NAME}; pub use ...; }
```

Callers import through `feature::prelude::*`. Internal submodules are private.

---

## Domain model

**Core commands and events:**

| Command           | Event             | HTTP route                          |
| ----------------- | ----------------- | ----------------------------------- |
| PostTweet         | TweetPosted       | POST /api/tweets                    |
| DeleteTweet       | TweetDeleted      | DELETE /api/tweets/:id              |
| Retweet           | TweetRetweeted    | POST /api/tweets/:id/retweet        |
| FollowUser        | UserFollowed      | POST /api/users/follow              |
| UnfollowUser      | UserUnfollowed    | POST /api/users/unfollow            |
| LikeTweet         | TweetLiked        | POST /api/tweets/:id/like           |
| UnlikeTweet       | TweetUnliked      | DELETE /api/tweets/:id/like         |
| SendDirectMessage | DirectMessageSent | POST /api/messages/direct           |
| FanOutTweet       | —                 | internal (dispatched by subscriber) |

**Shared DTOs** (in `assembly/domain.rs`): `TweetDto`, `FollowDto`, `LikeDto`, `DirectMessageDto`, `InboundEntity`, `InboundResponse`.

**`InboundEntity`** — the response union from the inbox endpoint:
```rust
enum InboundEntity {
    Tweet(TweetDto),
    Follow(FollowDto),
    Like(LikeDto),
    DirectMessage(DirectMessageDto),
    NoEntity,  // for DeleteTweet, UnlikeTweet
}
```

---

## Command outcome matrix (§9.4)

| Command           | Case                             | HTTP | Event | Outbox |
| ----------------- | -------------------------------- | ---- | ----- | ------ |
| PostTweet         | new tweet_id                     | 200  | yes   | yes    |
| PostTweet         | duplicate tweet_id               | 409  | no    | no     |
| DeleteTweet       | active tweet, matching author    | 204  | yes   | yes    |
| DeleteTweet       | missing / deleted / wrong author | 404  | no    | no     |
| Retweet           | original exists, new retweet_id  | 200  | yes   | yes    |
| Retweet           | original missing                 | 404  | no    | no     |
| Retweet           | duplicate retweet_id             | 409  | no    | no     |
| FollowUser        | self-follow                      | 400  | no    | no     |
| FollowUser        | new relationship                 | 200  | yes   | yes    |
| FollowUser        | already following (no-op)        | 200  | no    | no     |
| UnfollowUser      | relationship exists              | 200  | yes   | yes    |
| UnfollowUser      | relationship absent (no-op)      | 200  | no    | no     |
| LikeTweet         | tweet exists, like absent        | 200  | yes   | yes    |
| LikeTweet         | tweet missing                    | 404  | no    | no     |
| LikeTweet         | already liked (no-op)            | 200  | no    | no     |
| UnlikeTweet       | like exists → deleted            | 204  | yes   | yes    |
| UnlikeTweet       | like absent (no-op)              | 204  | no    | no     |
| UnlikeTweet       | tweet missing                    | 404  | no    | no     |
| SendDirectMessage | new message_id                   | 200  | yes   | yes    |
| SendDirectMessage | duplicate message_id             | 409  | no    | no     |
| SendDirectMessage | blank/too-long content           | 400  | no    | no     |
| FanOutTweet       | any                              | —    | no    | no     |

---

## Database tables

**Infrastructure tables** (managed by `mulac`/`commanding`/`eventing` crates):
- `command_entries` — kernel command journal
- `event_entries` — kernel event journal
- `inbox_entries` — kernel inbox journal
- `outbox_entries` — kernel outbox journal (legacy; not used by app-level outbox)

**Application domain tables**:
- `tweets`, `follows`, `likes`, `direct_messages`, `timelines`

**Application message tables** (owned by this app):
- `inbox_messages` — records for `POST /api/messages/commands`
- `outbox_messages` — records for `GET /api/messages/outbox`; `id` == source `event_id`

---

## Command/event flow

```
HTTP handler (async poem-openapi)
  │
  └─ run_blocking(mulac.dispatch_command(envelope))
       │
       ├─ CommandGateway.dispatch → command_entries
       ├─ CommandConsumer.consume → handler executes → event_entries
       ├─ EventConsumer.consume  → OutboxSubscriber (→ outbox_messages)
       │                         → FanOutSubscriber (→ command_entries FanOutTweet)
       ├─ CommandConsumer.consume → FanOutTweet handler → timelines
       └─ EventConsumer.consume  → (nothing)
```

Background workers (`run_command_worker`, `run_event_worker`) handle retries and commands from sources other than HTTP.

---

## Running locally

```bash
docker compose up -d
DATABASE_URL=postgres://twitter:twitter@127.0.0.1:5433/twitter cargo run -- migrate
DATABASE_URL=postgres://twitter:twitter@127.0.0.1:5433/twitter BIND_ADDR=127.0.0.1:33002 cargo run -- serve
```

Or via Makefile:
```bash
make up        # start Postgres
make migrate   # apply migrations
make serve     # start HTTP server on 127.0.0.1:33002
make test      # run integration tests
make reset     # wipe database and re-migrate
```

Default `DATABASE_URL`: `postgres://twitter:twitter@127.0.0.1:5433/twitter`  
Default `BIND_ADDR`: `127.0.0.1:33002`

---

## REST API

All application routes under `/api`. Root routes: `/health`, `/swagger`.

| Method | Path                    | Description             |
| ------ | ----------------------- | ----------------------- |
| GET    | /health                 | Health check            |
| GET    | /swagger                | Swagger UI              |
| POST   | /api/tweets             | Post tweet              |
| DELETE | /api/tweets/:id         | Delete tweet (204)      |
| POST   | /api/tweets/:id/retweet | Retweet                 |
| POST   | /api/users/follow       | Follow user             |
| POST   | /api/users/unfollow     | Unfollow user           |
| POST   | /api/tweets/:id/like    | Like tweet              |
| DELETE | /api/tweets/:id/like    | Unlike tweet (204)      |
| POST   | /api/messages/direct    | Send direct message     |
| POST   | /api/messages/commands  | Inbox: dispatch command |
| GET    | /api/messages/outbox    | List outbox events      |

---

## Intentional divergences from test_app_todo

1. **Sync, single-pool `start_mulac`** — Twitter is Diesel-backed end-to-end. `start_mulac(pool: DbPool)` is synchronous and uses one pool for both app operations and kernel storage. Todo's `start_mulac` is async and uses separate SQLx and Diesel pools.

2. **Typed `InboundEntity` response** — Inbox returns `InboundEntity { Tweet | Follow | Like | DirectMessage | NoEntity }` instead of Todo's `TodoDto`. Twitter has multiple entity types and some commands (DeleteTweet, UnlikeTweet) produce no resulting entity.

3. **`Command::entity_id(&self) -> Option<Uuid>`** — Returns `None` for composite-key commands (`FollowUser`, `LikeTweet`, etc.) and `Some(id)` for single-key commands. Todo always returns `Uuid`.

4. **Outbox row id == source `event_id`** — PK reuse for idempotency. Todo generates a fresh outbox row id. Twitter uses the originating `event_id` as the outbox row `id` so that `ON CONFLICT (id) DO NOTHING` prevents duplicate rows under event-subscriber retry.

5. **Two drain rounds in `dispatch_command`** — After processing the primary command and its events, a second round is run to consume any commands dispatched by event subscribers (e.g., `FanOutTweet` queued by `timeline_fan_out`). This ensures HTTP responses reflect fully-materialized state.

---

## Deferred items

- **Timeline read API** (`GET /api/timeline/:user_id`) — not implemented. Deferred for a future PR.
- **Mention notifications** (`UserMentioned` event, `mention_notifications` table) — not implemented. Identity model (handle-text vs user-profile) needs to be decided first.
