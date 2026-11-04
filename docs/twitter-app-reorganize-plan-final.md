# test_app_twitter Reorganization Plan — Final

| Field                | Value                                                                                                                                                  |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Status               | Approved for implementation                                                                                                                            |
| Created              | 2026-05-23                                                                                                                                             |
| Target app           | `test_app_twitter` (currently `app-twitter`)                                                                                                           |
| Reference app        | `test_app_todo`                                                                                                                                        |
| Supersedes           | `twitter-app-reorganize-plan-v2.md` … `-v8.md`                                                                                                         |
| Implements proposals | `S001`–`S005` from `docs/suggestions/twitter-app-reorganize-plan-v5/` and `docs/suggestions/twitter-app-reorganize-plan-final/`                        |
| Persistence decision | Keep Diesel for Twitter app data and migrations                                                                                                        |
| Goal                 | Reorganize `test_app_twitter` so it looks, tests, and behaves like `test_app_todo` while preserving the Twitter domain and current Diesel persistence. |

This final plan merges v7 and v8. From v8 it keeps the command outcome matrix
(§9.4), the outbox idempotency model that reuses the source `event_id` as
the outbox row id (§3.1 decision 11, §10.5, §11.3, §11.5, §12.3), the
tagged-event decoding rule for event-specific subscribers (§3.1 decision 12,
§10.8), and the phase ordering that lands the `serve`/`migrate` binary and
`/api` mount before the Mulac wiring (§13). From v7 it keeps Phase 0 for the
`app-twitter` → `test_app_twitter` crate rename (§13 Phase 0), the typed
`InboundEntity` response (§3.1 decision 8, §12.2), the sync single-pool
`start_mulac` rationale (§3.1 decision 6, §10.3), the `spawn_blocking`
boundary in poem handlers (§3.1 decision 7, §10.7), the explicit
`interpret_dispatch_error` contract (§10.2), the required custom `Serialize`
impl for `AppCommand` (§10.1), the byte-vs-scalar content-length bug
call-out (§8.3), and the compatibility-alias decision to remove old routes
rather than alias them (§9.5).

---

## 1. Executive summary

`test_app_todo` is the canonical app-level reference in this repository. It
demonstrates:

- flat feature modules with exactly one public `prelude` facade per feature
- private feature internals (`models`, `handler`, `infra_*`, `http`)
- a private `assembly` module for app state, errors, command/event wiring,
  workers, migrations, and binary startup
- an application-owned `AppState { pool, mulac }`
- two-phased command/event flow backed by `command_entries` and
  `event_entries`
- app-owned inbox and outbox HTTP APIs backed by app tables
- split integration tests using a shared `tests/utils.rs` harness
- a binary under `src/assembly/bin/` with `serve` and `migrate` subcommands

`test_app_twitter` currently diverges from that pattern: it exposes internal
modules publicly, uses `kernel::boot()` directly, has no production app-owned
inbox/outbox HTTP APIs, has one integration test focused on `PostTweet`, ships
incomplete production wiring for timeline fan-out, and keeps a DTO-only stub
for mention notifications.

The work is intentionally split into two scopes:

1. **Core reorganization** — required to make Twitter look and work like Todo
   structurally, including the crate rename and the new app-owned
   inbox/outbox.
2. **Product-completion extension** — optional follow-up: timeline read API
   and mention notifications. Either implemented and documented, or
   explicitly deferred in docs.

The invariant is unchanged: **mirror Todo's architecture, not Todo's ORM.**

---

## 2. Scope

### 2.1 Core scope — required

Complete when Twitter has:

- Todo-style `assembly` structure
- flat feature files named `<entity>_<verb>.rs`
- exactly one public `prelude` facade per feature
- root-private `inbox` and `outbox` modules
- app-owned `inbox_messages` and `outbox_messages` tables
- two-phased command/event wiring owned by `test_app_twitter` (no
  `kernel::boot()`)
- command/event worker loops started by the binary
- split integration tests with a shared `tests/utils.rs`
- Diesel migrations kept additive
- crate renamed from `app-twitter` to `test_app_twitter`

### 2.2 Extension scope — optional but documented

- `GET /api/timeline/:user_id` + `timeline_list.rs`
- `mention_notification.rs`, `UserMentioned` event, `mention_notifications`
  table, mention-related outbox tests

If not implemented, both must be explicitly listed as deferred in
`docs/test_app_twitter.md` and `test_app_twitter/AGENTS.md`.

---

## 3. Decisions

### 3.1 Architecture invariants

1. **Keep Diesel for application data.** Keep `diesel`, `diesel_migrations`,
   `DbPool`, `schema.rs`. Use the same Diesel pool for Twitter app tables and
   for the `mulac_diesel` write-side stores. Do not migrate to SQLx in this
   reorganization.

2. **Mirror Todo's architecture.** `src/assembly/{application,domain,infra_diesel}.rs`
   and `src/assembly/bin/twitterapp.rs`. Feature modules have private
   `models`/`handler`/`infra_diesel`/`http` submodules and one `pub mod prelude`.

3. **Do not use `kernel::boot()` for final app wiring.** Twitter owns its
   command handler registry, event subscriber registry, two-phased gateways,
   consumers, and worker loops — identical to Todo's `start_mulac`.

4. **Use tagged event JSON.** `#[serde(tag = "type", content = "payload")]`.
   Untagged is ambiguous for same-shape variants (`UserFollowed` /
   `UserUnfollowed`, `TweetLiked` / `TweetUnliked`).

5. **Two-phased command and event persistence.** `CommandGateway::two_phased`
   and `EventGateway::two_phased` for every state-changing path.

6. **Single Diesel pool, sync `start_mulac` (intentional simplification over
   Todo).** Todo's `start_mulac` is `async` and takes both an `sqlx::PgPool`
   and a `database_url` it uses to build a separate `mulac_diesel` pool for
   kernel storage. Twitter is sync end-to-end and already Diesel-backed, so
   it can share one pool. Signature:
   `pub fn start_mulac(pool: DbPool) -> Result<MulacHandle, KernelError>`.
   Document this in `AGENTS.md` so future readers don't "fix" it back to
   Todo's shape.

7. **All Diesel-backed work from async HTTP must run inside a blocking
   boundary.** `MulacState::dispatch_command` is sync (matches Todo), but
   so is every other database touch in the Twitter HTTP layer because
   Diesel is sync end-to-end. The rule is layer-wide, not dispatch-only:
   anything in an async poem handler that talks to the database — `dispatch_command`,
   inbox `record_received` / `mark_processed` / `mark_failed`, app-row
   fetches that build `InboundEntity`, and outbox list queries — must run
   inside `tokio::task::spawn_blocking`. The canonical helper and the
   end-to-end inbox-handler example live in §10.7.

8. **Inbox response returns the resulting resource, not just an id.** Match
   Todo's ergonomics (`InboundResponse { message_id, todo: TodoDto }`).
   Twitter's response carries a per-variant DTO union so callers don't have
   to round-trip:

   ```rust
   pub enum InboundEntity {
       Tweet(TweetDto),
       Follow(FollowDto),
       Like(LikeDto),
       DirectMessage(DirectMessageDto),
       NoEntity, // for unfollow/unlike no-op success
   }
   pub struct InboundResponse {
       pub message_id: Uuid,
       pub entity: InboundEntity,
   }
   ```

9. **Outbox subscriber registered per event type, same `OutboxSubscriber`
   type.** Matches Todo: one `Arc::new(OutboxSubscriber::new(pool.clone()))`
   per `(event_type, subscriber_name)` tuple. Each registration name is
   unique (e.g. `tweet-posted-outbox`).

10. **Outbox subscriber registered _before_ any domain subscriber for the
    same event.** Todo's `EventSubscriberRegistry` stops on the first
    subscriber error. Outbox-first ensures durable journaling even when a
    downstream subscriber (e.g. `timeline_fan_out`) fails on first attempt.

11. **Outbox idempotency via PK reuse.** The outbox row id **is** the source
    `event_id` from `NewEventMetadata`. Inserts use `ON CONFLICT (id) DO
    NOTHING`. No separate `source_event_id` column is needed — primary-key
    reuse is what guarantees at-most-one outbox row per event delivery under
    retry.

12. **Event-specific subscribers decode through the tagged `TwitterEvent`.**
    Subscribers that branch on event shape must deserialize
    `envelope.payload` into `TwitterEvent` and then match the variant.
    Generic pass-through subscribers (e.g. `OutboxSubscriber`) may
    deserialize to `serde_json::Value`. Subscriber-local flat payload structs
    for core events are forbidden once the tagged event format lands.

13. **`Command` trait uses `entity_id(&self) -> Option<Uuid>`.** Intentional
    divergence from Todo's `todo_id(&self) -> Uuid`. Twitter has composite-
    key commands (`FollowUser` is `(follower_id, following_id)`, `LikeTweet`
    is `(user_id, tweet_id)`) where no single Uuid is the natural entity id.
    `None` signals "no scalar entity id"; the HTTP layer returns the DTO via
    `InboundEntity` instead.

### 3.2 Conventions

14. **`<entity>_<verb>` source naming.** `tweet_post.rs`, `tweet_delete.rs`,
    `tweet_retweet.rs`, `user_follow.rs`, `user_unfollow.rs`, `tweet_like.rs`,
    `tweet_unlike.rs`, `direct_message_send.rs`, `timeline_fan_out.rs`. Test
    files match source names (`tests/tweet_post.rs`). This is a small
    improvement over Todo's `src/task_*.rs` / `tests/todo_*.rs` mismatch.

15. **Migrations stay additive.** Existing `2025-01-01-*` migrations
    untouched. New migrations get the date prefix `2026-05-23-*`.

16. **Application routes under `/api`.** `/health` and `/swagger` at root.

17. **DELETE returns `204 No Content`.** Other state-changing routes return
    the resulting DTO as JSON. Errors serialize as `{ "error": "..." }`.

18. **Crate name change from `app-twitter` to `test_app_twitter`.** Lib name
    becomes `test_app_twitter`, bin name becomes `test_app_twitter`. See
    Phase 0.

19. **Incremental cutover.** Keep `PostTweet` green throughout. Convert one
    feature at a time. Do not delete old files until replacements compile
    and tests pass.

---

## 4. Gap inventory

| Element              | `test_app_todo`                                       | `test_app_twitter` current                                                   | Phase  |
| -------------------- | ----------------------------------------------------- | ---------------------------------------------------------------------------- | ------ |
| Crate name           | `test_app_todo`                                       | `app-twitter`                                                                | 0      |
| App state            | `AppState { pool, mulac }`                            | `pub use kernel::AppState;` (src/state.rs)                                   | 1      |
| App assembly         | `src/assembly/*`                                      | none                                                                         | 1      |
| Error handling       | centralized `AppError` / `ApiError` / `ErrorBody`     | ad hoc per feature                                                           | 2      |
| Event serialization  | tagged enum                                           | `#[serde(untagged)]` (src/events.rs:58)                                      | 2      |
| Content length check | (n/a, validates `title.trim().is_empty()`)            | `content.len() > 280` — bytes, not scalars                                   | 2 fix  |
| Binary + CLI         | `src/assembly/bin/todoapp.rs`, `serve`/`migrate`      | `src/main.rs`, always serves, `MIGRATE_ONLY` env                             | 3      |
| Route mount          | `/health`, `/swagger`, `/api` nest                    | flat at `/`                                                                  | 3      |
| Gateway              | two-phased command/event persistence                  | `kernel::boot()` (src/main.rs)                                               | 4      |
| Consumers + workers  | owned by `MulacState`, spawned at boot                | none in production                                                           | 4      |
| Outbox subscriber    | app-owned, writes `outbox_messages` keyed by event_id | kernel builder `outbox_subscriber()` only                                    | 4      |
| Feature layout       | flat files, private internals, one `pub mod prelude`  | nested `pub` aggregator modules                                              | 5      |
| Inbox HTTP API       | `POST /api/messages/commands`                         | none                                                                         | 6      |
| Outbox HTTP API      | `GET /api/messages/outbox`                            | none                                                                         | 6      |
| Tests                | split files + `tests/utils.rs`                        | one `tests/post_tweet.rs` with inline helpers                                | 7      |
| Timeline fan-out     | (n/a)                                                 | builds own `CommandGateway` (timeline/fan_out.rs:81-93); flat payload struct | 8      |
| Timeline read API    | (n/a)                                                 | docs may imply it; absent in code                                            | 10 opt |
| Mention notification | (n/a)                                                 | DTO-only stub (src/notifications/mention.rs)                                 | 11 opt |

---

## 5. Target file layout

### 5.1 Core target layout

```text
test_app_twitter/
  Cargo.toml                                       # name + lib + bin renamed
  Makefile
  docker-compose.yml
  migrations/
    2025-01-01-000001_infrastructure/              # existing, unchanged
    2025-01-01-000002_app_tables/                  # existing, unchanged
    2026-05-23-000001_app_messages/                # new
  src/
    lib.rs                                         # mod decls, TwitterEvent, AppState, inbox, outbox, pub mod prelude
    schema.rs                                      # regenerated after new migration
    assembly/
      mod.rs                                       # pub mod prelude facade
      application.rs                               # AppError, AppCommand, MulacState, MulacHandle, workers
      domain.rs                                    # Clock, shared DTOs/validation, InboundEntity
      infra_diesel.rs                              # DbPool, build_pool, MIGRATIONS, run_migrations, OutboxSubscriber
      bin/
        twitterapp.rs                              # serve/migrate
    tweet_post.rs
    tweet_delete.rs
    tweet_retweet.rs
    user_follow.rs
    user_unfollow.rs
    tweet_like.rs
    tweet_unlike.rs
    direct_message_send.rs
    timeline_fan_out.rs
  tests/
    utils.rs
    tweet_post.rs
    tweet_delete.rs
    tweet_retweet.rs
    user_follow.rs
    user_unfollow.rs
    tweet_like.rs
    tweet_unlike.rs
    direct_message_send.rs
    timeline_fan_out.rs
    inbox.rs
    outbox.rs                                      # list endpoint + event_id idempotency
```

### 5.2 Optional extension layout

Add only if extension scope is selected:

```text
src/timeline_list.rs
src/mention_notification.rs
tests/timeline_list.rs
tests/mention_notification.rs
migrations/2026-05-23-000002_mention_notifications/
```

---

## 6. File-by-file change map

| Current path                                                                          | Target path                      | Action             | Notes                                                                                          |
| ------------------------------------------------------------------------------------- | -------------------------------- | ------------------ | ---------------------------------------------------------------------------------------------- |
| `Cargo.toml`                                                                          | `Cargo.toml`                     | rewrite headers    | Rename pkg, lib, bin. Update `[[bin]]` path in Phase 3.                                        |
| `src/main.rs`                                                                         | `src/assembly/bin/twitterapp.rs` | move + rewrite     | `serve` / `migrate` subcommands; drop `KernelConfig::from_env()`.                              |
| `src/lib.rs`                                                                          | `src/lib.rs`                     | rewrite            | Private feature mods, `TwitterEvent`, `AppState`, root-private inbox/outbox.                   |
| `src/state.rs`                                                                        | —                                | delete             | `AppState` moves to `lib.rs`.                                                                  |
| `src/db.rs`                                                                           | `src/assembly/infra_diesel.rs`   | move + rewrite     | `DbPool`, `build_pool`, `MIGRATIONS`, `run_migrations`; add `OutboxSubscriber`.                |
| `src/schema.rs`                                                                       | `src/schema.rs`                  | regenerate         | After `2026-05-23-000001_app_messages`.                                                        |
| `src/commands.rs`                                                                     | `src/assembly/application.rs`    | replace            | Becomes `AppCommand`, `Command`, envelope helpers.                                             |
| `src/events.rs`                                                                       | `src/lib.rs` + feature `models`  | merge + delete     | Root `TwitterEvent` becomes tagged and references feature `Event` structs.                     |
| `src/tweets/post_tweet.rs`                                                            | `src/tweet_post.rs`              | move + restructure | Canonical feature layout.                                                                      |
| `src/tweets/delete_tweet.rs`                                                          | `src/tweet_delete.rs`            | move + restructure | Canonical feature layout.                                                                      |
| `src/tweets/retweet.rs`                                                               | `src/tweet_retweet.rs`           | move + restructure | Canonical feature layout.                                                                      |
| `src/users/follow_user.rs`                                                            | `src/user_follow.rs`             | move + restructure | Canonical feature layout.                                                                      |
| `src/users/unfollow_user.rs`                                                          | `src/user_unfollow.rs`           | move + restructure | Canonical feature layout.                                                                      |
| `src/likes/like_tweet.rs`                                                             | `src/tweet_like.rs`              | move + restructure | Canonical feature layout.                                                                      |
| `src/likes/unlike_tweet.rs`                                                           | `src/tweet_unlike.rs`            | move + restructure | Canonical feature layout.                                                                      |
| `src/messages/send_direct_message.rs`                                                 | `src/direct_message_send.rs`     | move + restructure | Canonical feature layout.                                                                      |
| `src/timeline/fan_out.rs`                                                             | `src/timeline_fan_out.rs`        | move + restructure | Drop its private `CommandGateway`; decode `TwitterEvent`; use `MulacState::command_gateway()`. |
| `src/notifications.rs`, `src/notifications/mention.rs`                                | —                                | delete in core     | Stub. Extension may create `src/mention_notification.rs`.                                      |
| `src/tweets.rs`, `src/users.rs`, `src/likes.rs`, `src/messages.rs`, `src/timeline.rs` | —                                | delete             | Replaced by flat feature modules.                                                              |
| `tests/post_tweet.rs`                                                                 | `tests/tweet_post.rs`            | rewrite + split    | Shared setup moves to `tests/utils.rs`.                                                        |
| —                                                                                     | `tests/utils.rs`                 | create             | Mirrors Todo's harness; mounts inbox/outbox APIs.                                              |
| —                                                                                     | per-feature `tests/*.rs`         | create             | One file per feature; inbox + outbox each get their own file.                                  |

---

## 7. Module contract

Every feature file follows the Todo convention and exposes exactly one
public module: `pub mod prelude`.

```rust
pub const COMMAND_NAME: &str = "...";
pub const EVENT_NAME: &str = "...";

mod models {
    // Command, Event, request, response structs.
    // ApplicationCommand / ApplicationEvent impls where applicable.
    // poem-openapi Object derives where needed.
}

mod handler {
    // CommandHandlerPort impl; calls infra_diesel.
}

mod infra_diesel {
    // Raw Diesel queries; sync; takes &DbPool.
}

mod http {
    // poem-openapi Api impl; request/response structs.
    // TryFrom<Request> for NewCommandEnvelope.
}

pub mod prelude {
    pub use super::{COMMAND_NAME, EVENT_NAME};
    pub use super::handler::Handler;
    pub use super::http::Api;
    pub use super::models::{Command, Event};
}
```

Rules:

- Callers import through `feature::prelude::*`. Internal submodules stay private.
- No `pub` aggregator modules (`tweets`, `users`, `likes`, `messages`,
  `timeline`, `notifications`, `db`, `state`, `commands`, `events`) remain
  at crate root.
- Root `src/lib.rs` exposes one app-level `pub mod prelude`.

Root shape (single source of truth — §10 and §12 reference this, do not
redefine):

```rust
mod assembly;
mod tweet_post;
mod tweet_delete;
mod tweet_retweet;
mod user_follow;
mod user_unfollow;
mod tweet_like;
mod tweet_unlike;
mod direct_message_send;
mod timeline_fan_out;
// optional: mod timeline_list; mod mention_notification;

#[derive(Debug, Clone, Serialize, Deserialize, Union)]
#[oai(discriminator_name = "type")]
#[serde(tag = "type", content = "payload")]
pub enum TwitterEvent {
    TweetPosted(tweet_post::prelude::Event),
    TweetDeleted(tweet_delete::prelude::Event),
    TweetRetweeted(tweet_retweet::prelude::Event),
    UserFollowed(user_follow::prelude::Event),
    UserUnfollowed(user_unfollow::prelude::Event),
    TweetLiked(tweet_like::prelude::Event),
    TweetUnliked(tweet_unlike::prelude::Event),
    DirectMessageSent(direct_message_send::prelude::Event),
    // optional: UserMentioned(mention_notification::prelude::Event),
}

#[derive(Clone)]
pub struct AppState {
    pub pool: assembly::prelude::DbPool,
    pub mulac: assembly::prelude::MulacState,
}

mod inbox;
mod outbox;

pub mod prelude {
    pub use super::{AppState, TwitterEvent};
    pub use super::assembly::prelude::*;
    pub use super::inbox::prelude::Api as InboxApi;
    pub use super::outbox::prelude::Api as OutboxApi;
    pub use super::tweet_post::prelude::Api as TweetPostApi;
    // ... one Api re-export per feature ...
}
```

---

## 8. Domain model and validation

### 8.1 Clock

`src/assembly/domain.rs` re-implements Todo's `Clock`:

```rust
pub struct Clock;
impl Clock {
    pub fn now() -> DateTime<Utc>;
    pub fn fix(at: DateTime<Utc>);
    pub fn reset();
}
```

Use it in app table writes so integration tests can assert deterministic
timestamps when needed.

### 8.2 DTOs

Prefer feature-owned DTOs in each feature's `models` module. Promote to
`assembly/domain.rs` only when truly shared. Likely shared:

- `TweetDto`
- `FollowDto`
- `LikeDto`
- `DirectMessageDto`
- `TimelineItemDto`
- `InboundEntity` (the inbox response union from §3.8)
- optional `MentionNotificationDto`

### 8.3 Validation rules

Centralize reusable helpers in `assembly/application.rs` or
`assembly/domain.rs`:

- tweet content must be non-blank after `trim()`
- tweet content must be at most **280 Unicode scalar values**
  (`content.chars().count() > 280`). **Behavior fix:** current
  `src/tweets/post_tweet.rs:102` uses `content.len() > 280` which is bytes,
  not scalars. Multi-byte content like `"é".repeat(200)` is currently
  rejected even though it is only 200 characters.
- direct message content must be non-blank and at most 280 scalars
- a user cannot follow themselves
- retweet requires an existing original tweet
- like/unlike require an existing tweet
- delete requires a matching active tweet by author
- delete/unlike/unfollow follow the explicit outcome matrix in §9.4 — they
  do not emit events for no-op cases
- timeline fan-out must be idempotent under retries
- optional mention detection must document its identity model before
  implementation

---

## 9. REST API

### 9.1 Core routes

Mount application routes under `/api`. Keep `/health` and `/swagger` at root.

| Method | Path                      | Command                        | Event               |
| ------ | ------------------------- | ------------------------------ | ------------------- |
| POST   | `/api/tweets`             | `PostTweet`                    | `TweetPosted`       |
| DELETE | `/api/tweets/:id`         | `DeleteTweet`                  | `TweetDeleted`      |
| POST   | `/api/tweets/:id/retweet` | `Retweet`                      | `TweetRetweeted`    |
| POST   | `/api/users/follow`       | `FollowUser`                   | `UserFollowed`      |
| POST   | `/api/users/unfollow`     | `UnfollowUser`                 | `UserUnfollowed`    |
| POST   | `/api/tweets/:id/like`    | `LikeTweet`                    | `TweetLiked`        |
| DELETE | `/api/tweets/:id/like`    | `UnlikeTweet`                  | `TweetUnliked`      |
| POST   | `/api/messages/direct`    | `SendDirectMessage`            | `DirectMessageSent` |
| POST   | `/api/messages/commands`  | any supported `TwitterCommand` | command-specific    |
| GET    | `/api/messages/outbox`    | —                              | —                   |
| GET    | `/health`                 | —                              | —                   |
| GET    | `/swagger`                | —                              | —                   |

### 9.2 Optional extension routes

| Method | Path                     | Purpose                          |
| ------ | ------------------------ | -------------------------------- |
| GET    | `/api/timeline/:user_id` | List timeline rows for one user. |

Mention notifications do not need a public read route unless separately
requested. The minimum extension behavior is persistence plus `UserMentioned`
event/outbox visibility.

### 9.3 Response policy

- State-changing success returns the resulting DTO as JSON, except `DELETE
  /api/tweets/:id` and `DELETE /api/tweets/:id/like` which return `204 No
  Content` (including idempotent no-op success for `UnlikeTweet`).
- Errors serialize as `{ "error": "..." }`.
- Error mapping: `NotFound` → 404, `Validation` → 400, `Conflict` → 409,
  `Storage` → 500.

### 9.4 Command outcome matrix

Lock these semantics now so implementation and tests do not invent behavior
ad hoc.

| Command             | Case                                               | HTTP result                | App row change                    | Event emitted | Outbox row |
| ------------------- | -------------------------------------------------- | -------------------------- | --------------------------------- | ------------- | ---------- |
| `PostTweet`         | new `tweet_id`                                     | `200` + `TweetDto`         | insert tweet                      | yes           | yes        |
| `PostTweet`         | duplicate `tweet_id`                               | `409`                      | no                                | no            | no         |
| `DeleteTweet`       | active tweet exists and author matches             | `204`                      | set `deleted_at`                  | yes           | yes        |
| `DeleteTweet`       | tweet missing, already deleted, or author mismatch | `404`                      | no                                | no            | no         |
| `Retweet`           | original exists and new `retweet_id`               | `200` + `TweetDto`         | insert retweet row                | yes           | yes        |
| `Retweet`           | original missing                                   | `404`                      | no                                | no            | no         |
| `Retweet`           | duplicate `retweet_id`                             | `409`                      | no                                | no            | no         |
| `FollowUser`        | follower equals following                          | `400`                      | no                                | no            | no         |
| `FollowUser`        | relationship absent                                | `200` + `FollowDto`        | insert follow row                 | yes           | yes        |
| `FollowUser`        | relationship already exists                        | `200` + `FollowDto`        | no                                | no            | no         |
| `UnfollowUser`      | relationship exists                                | `200` + `FollowDto`        | delete follow row                 | yes           | yes        |
| `UnfollowUser`      | relationship absent                                | `200` + `FollowDto`        | no                                | no            | no         |
| `LikeTweet`         | tweet exists and like absent                       | `200` + `LikeDto`          | insert like row                   | yes           | yes        |
| `LikeTweet`         | tweet missing                                      | `404`                      | no                                | no            | no         |
| `LikeTweet`         | like already exists                                | `200` + `LikeDto`          | no                                | no            | no         |
| `UnlikeTweet`       | tweet exists and like exists                       | `204`                      | delete like row                   | yes           | yes        |
| `UnlikeTweet`       | tweet exists and like absent                       | `204`                      | no                                | no            | no         |
| `UnlikeTweet`       | tweet missing                                      | `404`                      | no                                | no            | no         |
| `SendDirectMessage` | new `message_id`, valid content                    | `200` + `DirectMessageDto` | insert DM row                     | yes           | yes        |
| `SendDirectMessage` | duplicate `message_id`                             | `409`                      | no                                | no            | no         |
| `SendDirectMessage` | invalid input                                      | `400`                      | no                                | no            | no         |
| `FanOutTweet`       | followers present or absent                        | internal success           | insert missing timeline rows only | no            | no         |

Notes:

- `DeleteTweet` intentionally treats wrong-author and already-deleted cases
  as `404`, not silent no-ops.
- `FollowUser`, `UnfollowUser`, `LikeTweet`, and `UnlikeTweet` are
  idempotent on the relationship state, but only real state changes emit
  events and create outbox rows.
- `FanOutTweet` is an internal command only; its idempotency comes from the
  unique `(user_id, tweet_id)` timeline constraint.
- For the inbox endpoint (`POST /api/messages/commands`), `200` returns the
  same DTO via `InboundEntity`. `204` cases (delete, unlike-no-op) map to
  `InboundEntity::NoEntity` so the JSON body is still `InboundResponse`.

### 9.5 Compatibility aliases (decided)

Current routes such as `POST /tweets/delete`, `POST /tweets/retweet`, `POST
/likes/like`, and `POST /likes/unlike` are **removed**, not aliased. Twitter
is pre-production and the cleanup is part of the reorganization. If a caller
breaks, fix the caller in the same commit.

If at deploy time a temporary alias is genuinely needed, mount it in Phase
3 alongside the new `/api/...` path and put the removal commit on the
schedule in the same PR description. No alias may outlive the
reorganization branch.

---

## 10. Assembly plan

Implement `src/assembly/application.rs` as the Twitter equivalent of Todo's
assembly layer, adapted for Diesel.

### 10.1 Core types

```rust
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("not found")]
    NotFound,
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("{0}")]
    Conflict(String),
    #[error("storage error: {0}")]
    Storage(#[from] anyhow::Error),
}

pub type ApiError = poem::Error;

#[derive(Debug, Clone, Serialize, Deserialize, Object)]
pub struct ErrorBody { pub error: String }

impl From<AppError> for poem::Error { /* maps to status + ErrorBody body */ }

pub trait Command: kernel::ApplicationCommand {
    fn entity_id(&self) -> Option<Uuid>;
}

#[derive(Debug, Clone)]
pub enum AppCommand {
    PostTweet(tweet_post::prelude::Command),
    DeleteTweet(tweet_delete::prelude::Command),
    Retweet(tweet_retweet::prelude::Command),
    FollowUser(user_follow::prelude::Command),
    UnfollowUser(user_unfollow::prelude::Command),
    LikeTweet(tweet_like::prelude::Command),
    UnlikeTweet(tweet_unlike::prelude::Command),
    SendDirectMessage(direct_message_send::prelude::Command),
    FanOutTweet(timeline_fan_out::prelude::Command),
}

// REQUIRED: matches Todo. Without this, kernel storage serializes the outer
// enum wrapper instead of the inner command shape and replay breaks.
impl serde::Serialize for AppCommand {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::PostTweet(c)          => c.serialize(s),
            Self::DeleteTweet(c)        => c.serialize(s),
            Self::Retweet(c)            => c.serialize(s),
            Self::FollowUser(c)         => c.serialize(s),
            Self::UnfollowUser(c)       => c.serialize(s),
            Self::LikeTweet(c)          => c.serialize(s),
            Self::UnlikeTweet(c)        => c.serialize(s),
            Self::SendDirectMessage(c)  => c.serialize(s),
            Self::FanOutTweet(c)        => c.serialize(s),
        }
    }
}

impl kernel::ApplicationCommand for AppCommand { fn command_type(&self) -> &'static str { ... } }
impl Command for AppCommand { fn entity_id(&self) -> Option<Uuid> { ... } }

pub struct NewCommandEnvelope {
    pub command: AppCommand,
    pub metadata: kernel::NewCommandMetadata,
}
```

### 10.2 `interpret_dispatch_error` contract

The §9.4 command outcome matrix is the single source of truth for
business-case behavior. `interpret_dispatch_error` is **only** invoked when
a handler returns `CommandError::HandlerExecution(_)` — i.e., for true
failures. Idempotent no-op cases approved in §9.4 (duplicate follow,
missing unfollow, duplicate like, missing unlike, missing-delete-target on
the inbox path that maps to `NoEntity`) must **not** be surfaced as
`HandlerExecution` errors. Handlers convert those cases to successful `Ok`
returns with an empty event list; the HTTP / inbox layer then builds the
appropriate `InboundEntity` per §12.4.

Handler-side error messages and the interpreter must agree for the
remaining real-failure cases:

| Handler `CommandError::HandlerExecution` message  | Mapped `AppError`           | Status |
| ------------------------------------------------- | --------------------------- | ------ |
| starts with `"tweet not found"`                   | `NotFound`                  | 404    |
| starts with `"user not found"`                    | `NotFound`                  | 404    |
| starts with `"validation failed: "` (Todo style)  | `Validation(remainder)`     | 400    |
| `"cannot follow self"`                            | `Validation(message)`       | 400    |
| `"duplicate tweet_id"` / `"duplicate retweet_id"` | `Conflict(message)`         | 409    |
| `"duplicate message_id"`                          | `Conflict(message)`         | 409    |
| anything else                                     | `Storage(anyhow!(message))` | 500    |

Notes:

- `"relationship not found"` is intentionally **not** in this table.
  Missing-follow on `UnfollowUser` and missing-like on `UnlikeTweet` are
  idempotent successes per §9.4, not handler errors.
- `"already following"` and `"already liked"` are likewise not errors;
  duplicate follow/like are idempotent successes per §9.4.
- `DeleteTweet` against a missing/already-deleted/wrong-author row is the
  one "missing target" case that **is** a handler error (`"tweet not
  found"` → 404), because §9.4 routes it to 404, not no-op.

Test coverage must include both the 4xx mappings here and the no-op cases
from §9.4 — handlers should be checked to produce neither a row change nor
an event for the no-op paths.

### 10.3 `start_mulac` signature

```rust
pub fn start_mulac(pool: DbPool) -> Result<MulacHandle, KernelError>
```

Sync, single Diesel pool. Diesel is sync, the kernel storage is Diesel-
backed, and there is no need for a SQLx-style `block_on_blocking` helper.
See decision 3.6 for the divergence rationale.

### 10.4 `MulacState` and `MulacHandle`

```rust
#[derive(Clone)]
pub struct MulacState {
    command_gateway: Arc<CommandGateway>,
    command_consumer: Arc<CommandConsumer>,
    event_consumer: Arc<EventConsumer>,
}

impl MulacState {
    pub fn dispatch_command(&self, envelope: NewCommandEnvelope) -> Result<(), KernelError>;
    pub fn command_gateway(&self) -> Arc<CommandGateway>;
}

pub struct MulacHandle {
    state: MulacState,
    token: CancellationToken,
}

impl MulacHandle {
    pub fn state(&self) -> MulacState;
    pub fn child_token(&self) -> CancellationToken;
    pub fn command_consumer(&self) -> Arc<CommandConsumer>;
    pub fn event_consumer(&self) -> Arc<EventConsumer>;
    pub fn shutdown(&self);
    pub async fn wait(self) -> Result<(), KernelError>;
}
```

### 10.5 What `start_mulac` wires

- **Command handler registry**, one wrapped handler per command in 10.1.
- **Event subscriber registry**, one `(event_type, subscriber_name,
  OutboxSubscriber instance)` entry per externally relevant event:
  `TweetPosted`, `TweetDeleted`, `TweetRetweeted`, `UserFollowed`,
  `UserUnfollowed`, `TweetLiked`, `TweetUnliked`, `DirectMessageSent`. Each
  registration uses its own `Arc::new(OutboxSubscriber::new(pool.clone()))`
  and a unique name like `tweet-posted-outbox` — matches Todo.
- **Downstream subscriber**: `TweetPosted` → `timeline_fan_out`, registered
  **after** the outbox subscriber for `TweetPosted`.
- **Optional downstream subscriber**: `TweetPosted` → mention detection if
  extension scope is selected.
- **Gateways** using `CommandGateway::two_phased(...)` and
  `EventGateway::two_phased(...)`, both backed by the single Diesel pool.
- **Consumers** using the Diesel-backed `Command*Storage` and `Event*Storage`.

Subscriber ordering and failure semantics: Todo's `EventSubscriberRegistry`
stops on the first subscriber error for a given event delivery. Twitter
preserves that, with the following consequences:

1. `OutboxSubscriber` registers first for every relevant event type.
2. `OutboxSubscriber` is idempotent on the source `event_id` (§11.5).
3. Later subscribers (`timeline_fan_out`, optional mention detection) may
   fail without producing duplicate outbox rows on retry, because retry
   inserts conflict on the existing outbox PK.

The `timeline_fan_out` subscriber must use `MulacState::command_gateway()`
— not build its own `CommandGateway::two_phased(pool)` like
`src/timeline/fan_out.rs:81-93` currently does.

### 10.6 Workers

```rust
pub async fn run_command_worker(consumer: Arc<CommandConsumer>, token: CancellationToken);
pub async fn run_event_worker(consumer: Arc<EventConsumer>, token: CancellationToken);
```

Each loops on a short tick, runs the sync consumer inside
`tokio::task::spawn_blocking`, logs errors, and exits on `token.cancelled()`.
Direct copy of Todo's shape.

### 10.7 Blocking boundary for the HTTP layer

`MulacState::dispatch_command` mirrors Todo. Sync internals:

1. dispatch the envelope to the two-phased command gateway
2. synchronously drain the command consumer
3. synchronously drain the event consumer
4. return after the command and resulting events have reached terminal
   local state

This lets HTTP response bodies reflect the command's terminal state.
Background workers remain useful for retry progress and for commands
recorded by other means, including the inbox endpoint.

#### 10.7.1 Why a layer-wide rule

Diesel is synchronous. Every Twitter handler that touches the database
from inside an async poem handler must move that work onto a blocking
thread, or it stalls a Tokio worker. The rule is not "wrap
`dispatch_command`" — it is "wrap any Diesel-touching closure invoked
from `async fn`." That includes:

- `dispatch_command` (the two-phased command + drain path, §10.7 main
  body)
- inbox `record_received` / `mark_processed` / `mark_failed` (§12.2)
- the `read_after_write` / `synthesize_from_command` resolution that
  builds `InboundEntity` (§12.4) — `synthesize_from_command` is
  computation-only and does not need the wrapper, but the surrounding
  `read_after_write` cases do
- the outbox list query for `GET /api/messages/outbox` (§12.3)
- any feature-level HTTP route that performs Diesel reads or writes
  outside of dispatch (none in the core scope, but applies to the
  optional `GET /api/timeline/:user_id` in Phase 10)

#### 10.7.2 Canonical helper

Define one helper in `assembly::application` and use it from every async
handler that calls Diesel:

```rust
/// Run a sync, Diesel-touching closure off the async runtime, mapping
/// both the join error and the closure result through AppError.
pub async fn run_blocking<F, T>(f: F) -> Result<T, AppError>
where
    F: FnOnce() -> Result<T, AppError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|join_err| AppError::Storage(anyhow::anyhow!(
            "blocking task join failed: {join_err}"
        )))?
}
```

Notes:

- The double `?` (one for the `JoinError`, one for the closure's
  `Result`) is collapsed inside `run_blocking` so call sites stay flat.
- Returning `AppError::Storage` for a `JoinError` is correct: panics in
  Diesel work are infrastructure failures, not domain errors.
- For `KernelError` returned by `MulacState::dispatch_command`, run it
  through `interpret_dispatch_error` (§10.2) inside the closure so the
  helper's return type is uniformly `Result<_, AppError>`.

#### 10.7.3 End-to-end inbox handler example

Combines the helper with the inbox processing sequence (§12.2) and
materialization (§12.4):

```rust
async fn process_command(
    state: Data<&AppState>,
    Json(envelope): Json<CommandEnvelope>,
) -> Result<Json<InboundResponse>, ApiError> {
    let pool = state.pool.clone();
    let mulac = state.mulac.clone();
    let envelope = Arc::new(envelope);

    // 1. Record received (Diesel write).
    {
        let pool = pool.clone();
        let envelope = envelope.clone();
        run_blocking(move || record_received(&pool, &envelope)).await?;
    }

    let message_id = envelope.id;

    // 2 + 3. Build metadata and dispatch under spawn_blocking; map kernel
    //        errors through interpret_dispatch_error.
    let dispatch_result: Result<(), AppError> = {
        let mulac = mulac.clone();
        let app_command = to_app_command(envelope.command.clone());
        let metadata = NewCommandMetadata {
            command_id: envelope.id,
            correlation_id: Some(envelope.id),
            causation_id: Some(envelope.id),
            source: Some("test_app_twitter.inbox".to_string()),
        };
        run_blocking(move || {
            mulac
                .dispatch_command(NewCommandEnvelope {
                    command: app_command,
                    metadata,
                })
                .map_err(interpret_dispatch_error)
        }).await
    };

    // 4 / 5. Mark processed/failed; both are Diesel writes.
    match dispatch_result {
        Ok(()) => {
            let pool = pool.clone();
            run_blocking(move || mark_processed(&pool, message_id)).await?;
        }
        Err(err) => {
            let err_text = err.to_string();
            let pool = pool.clone();
            run_blocking(move || mark_failed(&pool, message_id, &err_text)).await?;
            return Err(err.into());
        }
    }

    // 6. Materialize InboundEntity per §12.4.
    let entity = {
        let pool = pool.clone();
        let envelope = envelope.clone();
        run_blocking(move || materialize_entity(&pool, &envelope.command)).await?
    };

    Ok(Json(InboundResponse { message_id, entity }))
}
```

The pattern for feature HTTP routes (`POST /api/tweets`, etc.) is the
same: build the envelope, dispatch through `run_blocking`, then if the
route returns a DTO, fetch the row through a second `run_blocking` call.
DELETE routes that return `204` skip the second call.

The pattern for the outbox list endpoint is one `run_blocking` call
wrapping the `SELECT ... FROM outbox_messages ORDER BY created_at ASC`
query.

### 10.8 Subscriber payload decoding rule

Use one decoding rule consistently across the app:

- **event-type-specific subscribers** must deserialize `envelope.payload`
  through the tagged `TwitterEvent` enum and then match on the variant they
  handle.
- **generic pass-through subscribers** such as `OutboxSubscriber` may
  deserialize to `serde_json::Value` because they do not inspect
  event-specific fields.

For example, `timeline_fan_out::prelude::Subscriber` decodes `TwitterEvent`, then
matches:

```rust
match serde_json::from_str::<TwitterEvent>(&envelope.payload)? {
    TwitterEvent::TweetPosted(payload) => { /* dispatch FanOutTweet */ }
    other => return Err(EventError::SubscriberExecution(format!(
        "unexpected event payload for {}: {:?}",
        envelope.event_type, other,
    ))),
}
```

Do not introduce subscriber-local flat payload structs for core events
after the tagged event format is adopted. The current
`src/timeline/fan_out.rs:102-108` flat `Payload` struct is the pattern to
delete.

---

## 11. Persistence plan

### 11.1 Pool and migrations

`src/assembly/infra_diesel.rs` owns:

```rust
pub type DbPool = Pool<ConnectionManager<PgConnection>>;

pub fn build_pool(database_url: &str) -> anyhow::Result<DbPool>;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub fn run_migrations(pool: &DbPool) -> anyhow::Result<()>;
```

`embed_migrations!` requires the `migrations/` directory at compile time;
no runtime `diesel-cli` is needed for `migrate`.

### 11.2 Existing tables stay unchanged

| Migration                           | Tables                                                                |
| ----------------------------------- | --------------------------------------------------------------------- |
| `2025-01-01-000001_infrastructure/` | `inbox_entries`, `command_entries`, `event_entries`, `outbox_entries` |
| `2025-01-01-000002_app_tables/`     | `tweets`, `follows`, `likes`, `direct_messages`, `timelines`          |

The kernel `*_entries` tables are the kernel's transport-level journal
(reservations, retries, dispatcher bookkeeping). The new app-level
`*_messages` tables (§11.3) are the application's inbox/outbox surface
exposed over HTTP. **Do not consolidate or rename them.**

### 11.3 New core migration: app messages

`migrations/2026-05-23-000001_app_messages/up.sql`:

```sql
CREATE TABLE inbox_messages (
    id UUID PRIMARY KEY,
    message_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('received', 'processed', 'failed')),
    received_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ,
    error TEXT
);
CREATE INDEX idx_inbox_messages_status ON inbox_messages (status, received_at);

CREATE TABLE outbox_messages (
    id UUID PRIMARY KEY,                            -- == source NewEventMetadata.event_id
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'published', 'failed')),
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    attempts INT NOT NULL DEFAULT 0
);
CREATE INDEX idx_outbox_messages_status ON outbox_messages (status, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS uq_timelines_user_tweet ON timelines (user_id, tweet_id);
```

`down.sql`:

```sql
DROP INDEX IF EXISTS uq_timelines_user_tweet;
DROP TABLE IF EXISTS outbox_messages;
DROP TABLE IF EXISTS inbox_messages;
```

For outbox rows, `id` is the originating `event_id` from
`NewEventMetadata`. The schema does not need a separate `source_event_id`
column because primary-key reuse already makes the outbox journal
idempotent under subscriber retries.

After applying the migration, regenerate the schema:

```bash
diesel print-schema --database-url "$DATABASE_URL" > test_app_twitter/src/schema.rs
```

`diesel-cli` is a dev-time prerequisite, not a crate dependency.

### 11.4 Optional migration: mention notifications

Only if extension scope is selected:

```sql
CREATE TABLE mention_notifications (
    id UUID PRIMARY KEY,
    tweet_id UUID NOT NULL REFERENCES tweets(id),
    mentioned_handle TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE UNIQUE INDEX uq_mention_notifications_handle_tweet
    ON mention_notifications (mentioned_handle, tweet_id);
```

If a real user profile/handle table is added instead, replace
`mentioned_handle` with the chosen user identity model and document that
choice.

### 11.5 Generic outbox subscriber

`OutboxSubscriber` lives in `assembly/infra_diesel.rs`, holds a `DbPool`,
and writes a row to `outbox_messages` for every event handed to it. It is
**sync** — Twitter does not need Todo's `block_on_blocking` wrapper because
Diesel is already blocking.

```rust
pub struct OutboxSubscriber {
    pool: DbPool,
}

impl OutboxSubscriber {
    pub fn new(pool: DbPool) -> Self { Self { pool } }
}

impl EventSubscriberPort for OutboxSubscriber {
    fn handle(&self, envelope: &NewEventEnvelope) -> Result<(), EventError> {
        let event_id = envelope
            .metadata
            .as_ref()
            .map(|meta| meta.event_id)
            .ok_or_else(|| EventError::SubscriberExecution(
                "event_id is required for outbox idempotency".to_string()
            ))?;
        let payload: serde_json::Value = serde_json::from_str(&envelope.payload)
            .map_err(|e| EventError::SubscriberExecution(e.to_string()))?;
        record_event_payload(&self.pool, event_id, &envelope.event_type, payload)
            .map(|_| ())
            .map_err(|e| EventError::SubscriberExecution(e.to_string()))
    }
}

pub fn record_event_payload(
    pool: &DbPool,
    event_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
) -> anyhow::Result<Uuid> {
    // INSERT INTO outbox_messages (id, event_type, payload, status, created_at)
    // VALUES ($1, $2, $3, 'pending', $4)
    // ON CONFLICT (id) DO NOTHING
    // Use `event_id` as `id`.
}
```

Two correctness rules to call out:

- Do not generate a fresh outbox row id in the subscriber. Reusing the
  source `event_id` is what prevents duplicate outbox rows when the same
  event is retried.
- Registration follows Todo's pattern: separate `Arc::new(...)` per event
  type with a unique subscriber name. The `OutboxSubscriber` type is
  shared; the instances are not.

---

## 12. Inbox and outbox plan

Implement root-private `mod inbox` and `mod outbox` in `src/lib.rs`,
mirroring Todo.

### 12.1 `TwitterCommand` union

Defined inside `mod inbox::models`. `Union` with discriminator `type`,
serde tagged.

Supported core input:

| `type`              | Required fields                                      |
| ------------------- | ---------------------------------------------------- |
| `PostTweet`         | `tweet_id`, `author_id`, `content`                   |
| `DeleteTweet`       | `tweet_id`, `author_id`                              |
| `Retweet`           | `retweet_id`, `original_tweet_id`, `author_id`       |
| `FollowUser`        | `follower_id`, `following_id`                        |
| `UnfollowUser`      | `follower_id`, `following_id`                        |
| `LikeTweet`         | `user_id`, `tweet_id`                                |
| `UnlikeTweet`       | `user_id`, `tweet_id`                                |
| `SendDirectMessage` | `message_id`, `sender_id`, `recipient_id`, `content` |

`FanOutTweet` is internal and is **not** exposed in the inbox union.

Required methods on `TwitterCommand`:

```rust
impl TwitterCommand {
    pub fn message_type(&self) -> &'static str;     // same string as command_type
    pub fn entity_id(&self) -> Option<Uuid>;        // delegates per variant
}
impl kernel::ApplicationCommand for TwitterCommand {
    fn command_type(&self) -> &'static str { self.message_type() }
}
```

### 12.2 Inbox processing sequence

`POST /api/messages/commands` (every Diesel step runs through `run_blocking`
per §10.7.2):

1. **`run_blocking(record_received)`** — insert into `inbox_messages` with
   `status = 'received'`, `ON CONFLICT (id) DO NOTHING`. Zero rows
   affected → `409 Conflict`.
2. Build `NewCommandMetadata { command_id: envelope.id, correlation_id:
   Some(envelope.id), causation_id: Some(envelope.id), source:
   Some("test_app_twitter.inbox".to_string()) }`.
3. **`run_blocking(dispatch_command + interpret_dispatch_error)`** —
   per §10.7.
4. On success: **`run_blocking(mark_processed)`**, then
   **`run_blocking(materialize_entity)`** per the strategy table in §12.4,
   return the `InboundResponse`.
5. On failure: **`run_blocking(mark_failed)`** with the error text, return
   the mapped `AppError`.

The full handler shape with explicit join/error mapping is in §10.7.3.

`InboundResponse`:

```rust
pub struct InboundResponse {
    pub message_id: Uuid,
    pub entity: InboundEntity,
}
```

`InboundEntity` is the union from §3.1 decision 8 (`Tweet | Follow | Like |
DirectMessage | NoEntity`).

### 12.3 Outbox API

`GET /api/messages/outbox` lists `outbox_messages` in `created_at` order.

DTO fields:

- `id` (same value as the originating `event_id`)
- `event_type`
- `payload`
- `status`
- `created_at`
- `published_at`
- `attempts`

The list API therefore exposes at most one outbox row per source `event_id`.

Stored payload shape (tagged):

```json
{
  "type": "TweetPosted",
  "payload": {
    "tweet_id": "...",
    "author_id": "...",
    "content": "..."
  }
}
```

The endpoint runs its Diesel `SELECT` through `run_blocking` (§10.7.2), like
every other async handler in the app.

Known non-goal: like Todo, this app-level outbox is initially a
journal/API, not an AMQP publisher. If AMQP publishing is desired later,
add it as a separate feature.

### 12.4 `InboundEntity` materialization strategy

The `200`/`204` outcomes in §9.4 do not all correspond to a row that exists
after the command runs. Lock the resolution rule per command so the inbox
endpoint produces consistent `InboundEntity` values without inventing
behavior at implementation time.

| Command             | §9.4 case                               | HTTP route status | Inbox `InboundEntity`                                                                                         |
| ------------------- | --------------------------------------- | ----------------- | ------------------------------------------------------------------------------------------------------------- |
| `PostTweet`         | new `tweet_id`                          | `200`             | `Tweet(read_after_write(tweet_id))`                                                                           |
| `DeleteTweet`       | success (soft delete)                   | `204`             | `NoEntity` — body is `InboundResponse { entity: NoEntity }`                                                   |
| `Retweet`           | new `retweet_id`                        | `200`             | `Tweet(read_after_write(retweet_id))`                                                                         |
| `FollowUser`        | relationship absent → inserted          | `200`             | `Follow(read_after_write(follower_id, following_id))`                                                         |
| `FollowUser`        | relationship already exists (duplicate) | `200` (no-op)     | `Follow(read_after_write(follower_id, following_id))` — pre-existing row                                      |
| `UnfollowUser`      | relationship existed → deleted          | `200`             | `Follow(synthesize_from_command(follower_id, following_id))` — row is gone, build the DTO from command inputs |
| `UnfollowUser`      | relationship absent (no-op)             | `200` (no-op)     | `Follow(synthesize_from_command(follower_id, following_id))` — same synthesis path                            |
| `LikeTweet`         | like absent → inserted                  | `200`             | `Like(read_after_write(user_id, tweet_id))`                                                                   |
| `LikeTweet`         | like already exists (duplicate)         | `200` (no-op)     | `Like(read_after_write(user_id, tweet_id))` — pre-existing row                                                |
| `UnlikeTweet`       | like existed → deleted                  | `204`             | `NoEntity`                                                                                                    |
| `UnlikeTweet`       | like absent (no-op)                     | `204`             | `NoEntity`                                                                                                    |
| `SendDirectMessage` | new `message_id`                        | `200`             | `DirectMessage(read_after_write(message_id))`                                                                 |

Strategy notes:

- **`read_after_write`** performs a `SELECT` against the relevant app table
  inside `spawn_blocking` (§10.7). Returning `AppError::Storage` here is
  acceptable but should not happen on the happy path; if the row is
  unexpectedly missing after a successful dispatch, treat it as `Storage`
  (500), not `NotFound`.
- **`synthesize_from_command`** constructs the DTO directly from command
  fields. This is the only safe strategy for `UnfollowUser`: the row is
  either just-deleted or never existed, but the inbox contract still
  promises a `FollowDto`. Synthesize with `created_at = Clock::now()` and
  document that the timestamp is the response time, not a row timestamp.
- **`NoEntity`** is reserved for HTTP-`204` commands only (`DeleteTweet`,
  `UnlikeTweet`). The JSON body remains `InboundResponse { message_id,
  entity: NoEntity }` so the inbox endpoint always returns a consistent
  shape.
- **Snapshot-before-delete is intentionally not used.** It would require
  reading inside the handler before dispatch, which couples the inbox
  layer to handler internals. Synthesis from command inputs keeps the
  contract simple.
- 404 cases (`DeleteTweet` missing/wrong-author, `Retweet` missing
  original, `LikeTweet` / `UnlikeTweet` missing tweet) never reach the
  materialization step — dispatch fails and the inbox marks the message
  `failed` per §12.2 step 5. The same is true for `409` cases (duplicate
  `tweet_id`, `retweet_id`, `message_id`) and `400` cases.

---

## 13. Implementation phases

Run after every meaningful phase:

```bash
cargo fmt --manifest-path test_app_twitter/Cargo.toml
cargo check --manifest-path test_app_twitter/Cargo.toml
cargo test --manifest-path test_app_twitter/Cargo.toml --no-run
make test
```

Project convention is `make test` from repo root.

**Runtime cutover note:** `serve` / `migrate` and the `/api` route prefix
become canonical in **Phase 3**. Any later acceptance criteria or route
tables that mention `cargo run -- migrate` or `/api/...` assume Phase 3 has
landed.

### Phase 0 — Crate rename

1. Update `test_app_twitter/Cargo.toml`: `name = "test_app_twitter"`,
   `[lib] name = "test_app_twitter"`, `[[bin]] name = "test_app_twitter"`.
   Bin path stays `src/main.rs` until Phase 3.
2. Update the workspace `Cargo.toml` member list and any dev-dependency
   aliases.
3. Replace every `use app_twitter::*` with `use test_app_twitter::*`
   (production + tests).
4. Update `Makefile`, `docker-compose.yml`, env vars, and any CI script
   referencing `app-twitter` or `app_twitter`.

Acceptance: `cargo check` and `make test` pass with no code-shape change.

### Phase 1 — Assembly skeleton, still Diesel

1. Create `src/assembly/{mod,application,domain,infra_diesel}.rs`.
2. Move `DbPool`, `build_pool`, `MIGRATIONS`, `run_migrations` from
   `src/db.rs` into `infra_diesel.rs`. Add `Clock` in `domain.rs`.
3. Create `src/assembly/application.rs` with temporary or skeletal app
   types.
4. Add root `AppState { pool, mulac }` in `src/lib.rs`, replacing
   `pub use kernel::AppState;`.
5. Temporarily alias/shim `MulacState` if needed to keep compile green
   while `kernel::boot()` is still in use.
6. Delete `src/state.rs`. Update imports away from `crate::state::AppState`
   and `crate::db::*`.

Acceptance: `cargo check` passes; no route behavior changes yet.

### Phase 2 — Error and command boundary

1. Add `AppError`, `ApiError`, `ErrorBody`, `From<AppError> for poem::Error`.
2. Add `interpret_dispatch_error` per the contract in §10.2.
3. Add shared validation helpers.
4. Add `Command` trait with `entity_id(&self) -> Option<Uuid>`.
5. Add `AppCommand` enum + custom `Serialize` impl (per §10.1, required for
   correct kernel-storage serialization).
6. Replace `src/commands.rs` with assembly-owned command types.
7. Convert HTTP dispatch code to build `NewCommandEnvelope<AppCommand>`.
8. Switch `TwitterEvent` from `#[serde(untagged)]` to tagged.
9. Fix tweet content validation from `len()` to `chars().count()` (§8.3).

Acceptance: `cargo check` passes; current `PostTweet` test still passes;
invalid content returns 400 via `AppError::Validation`.

### Phase 3 — Binary move, CLI split, and `/api` route mounting

1. Move `src/main.rs` to `src/assembly/bin/twitterapp.rs`.
2. Add `serve` and `migrate` subcommands.
3. Drop `KernelConfig::from_env()` (no longer used).
4. Build the pool and run migrations on `serve`.
5. Mount `/health`, `/swagger`, and `/api` routes.
6. Update `[[bin]]` path in `Cargo.toml`:

   ```toml
   [[bin]]
   name = "test_app_twitter"
   path = "src/assembly/bin/twitterapp.rs"
   ```

7. If a route compatibility alias is genuinely needed, mount it under its
   legacy path alongside the new `/api/...` path and put the removal commit
   in the same PR description (per §9.5).

Acceptance:

- `cargo run --manifest-path test_app_twitter/Cargo.toml -- migrate`
  applies migrations
- `DATABASE_URL=... cargo run --manifest-path test_app_twitter/Cargo.toml -- serve`
  boots
- `GET /health` returns `200`
- new or converted APIs are mounted under `/api`

### Phase 4 — Two-phased Mulac and app outbox

1. Implement real `MulacState` with two-phased command and event gateways.
2. Implement `MulacHandle`.
3. Implement `start_mulac(pool: DbPool)` (sync, single Diesel pool).
4. Implement command handler registry; register all command handlers.
5. Implement event subscriber registry; register `OutboxSubscriber` per
   event type **before** any domain subscriber.
6. Register `TweetPosted → timeline_fan_out` (after `tweet-posted-outbox`).
7. Implement `OutboxSubscriber::handle` and `record_event_payload` using
   the source `event_id` as the outbox row id (§11.5).
8. Implement `run_command_worker` and `run_event_worker`.
9. Add migration `2026-05-23-000001_app_messages` and regenerate
   `schema.rs`.
10. Spawn the two workers from the binary; build `AppState { pool, mulac:
    handle.state() }`.

Acceptance:

- HTTP `POST /api/tweets` persists a tweet row
- `command_entries` has completed `PostTweet`
- `event_entries` has completed `TweetPosted`
- `outbox_messages` has a pending `TweetPosted` row whose `id` equals the
  source `event_id`
- replaying the same event delivery does not insert a second outbox row

### Phase 5 — Flatten feature modules

Convert one feature at a time:

| Order | Current file                          | New file                     | Route                          |
| ----- | ------------------------------------- | ---------------------------- | ------------------------------ |
| 1     | `src/tweets/post_tweet.rs`            | `src/tweet_post.rs`          | `POST /api/tweets`             |
| 2     | `src/tweets/delete_tweet.rs`          | `src/tweet_delete.rs`        | `DELETE /api/tweets/:id`       |
| 3     | `src/tweets/retweet.rs`               | `src/tweet_retweet.rs`       | `POST /api/tweets/:id/retweet` |
| 4     | `src/users/follow_user.rs`            | `src/user_follow.rs`         | `POST /api/users/follow`       |
| 5     | `src/users/unfollow_user.rs`          | `src/user_unfollow.rs`       | `POST /api/users/unfollow`     |
| 6     | `src/likes/like_tweet.rs`             | `src/tweet_like.rs`          | `POST /api/tweets/:id/like`    |
| 7     | `src/likes/unlike_tweet.rs`           | `src/tweet_unlike.rs`        | `DELETE /api/tweets/:id/like`  |
| 8     | `src/messages/send_direct_message.rs` | `src/direct_message_send.rs` | `POST /api/messages/direct`    |
| 9     | `src/timeline/fan_out.rs`             | `src/timeline_fan_out.rs`    | internal only                  |

For each feature:

1. Move to the flat source file.
2. Restructure into private `models`, `handler`, `infra_diesel`, `http`.
3. Expose only `pub mod prelude`.
4. Add command/event name constants.
5. Extract raw Diesel queries into `infra_diesel`.
6. Wire the feature into `start_mulac()` and the OpenAPI service.
7. Add or update one integration test file.

For `timeline_fan_out` specifically: drop the inline `Payload` struct in
the subscriber, decode `TwitterEvent` per §10.8, and use
`MulacState::command_gateway()` instead of building a new gateway from a
raw pool.

Acceptance:

- no imports bypass `feature::prelude::*`
- no old public feature aggregator modules remain
- full tests pass after each converted feature or after a clearly bounded
  batch

### Phase 6 — Inbox and outbox HTTP APIs

1. Implement root-private `mod inbox` with `TwitterCommand`, the
   processing sequence (§12.2), and `POST /api/messages/commands` returning
   `InboundResponse { message_id, entity: InboundEntity }`.
2. Implement root-private `mod outbox` with `GET /api/messages/outbox`.
3. Mount both APIs in the binary **and** in
   `tests/utils.rs::start_test_app`.
4. Add `tests/inbox.rs`.
5. Add `tests/outbox.rs` covering list shape, `created_at` ordering, tagged
   payload shape, and the no-duplicate-row-per-event-id invariant.

Acceptance:

- successful inbox command marks `processed` and returns the right
  `InboundEntity`
- duplicate inbox id returns `409`
- failed command marks `failed` and returns the mapped `AppError`
- outbox endpoint returns persisted events in `created_at` order
- retried event delivery produces no duplicate outbox row

### Phase 7 — Tests split and utils

1. Create `tests/utils.rs` modeled on Todo's, with Diesel row structs
   (`OutboxRow`, `InboxRow`, `CommandEntryRow`, `EventEntryRow`, plus
   `TweetRow`, `FollowRow`, `LikeRow`, `DirectMessageRow`, `TimelineRow`).
2. Move shared setup out of `tests/post_tweet.rs`. Split per feature.
3. Use `reqwest` against a real spawned Poem server (not
   `poem::test::TestClient`).
4. Use a static `tokio::sync::Mutex` when sharing one database.
5. Reset all app + infrastructure tables between tests.
6. Each feature test asserts: HTTP response, app row(s), `command_entries`,
   `event_entries`, and `outbox_messages` per the outcome matrix in §9.4.

Acceptance:

- no noisy `println!` in integration tests
- each test asserts HTTP response, app rows, command journal, event
  journal, and outbox state where applicable

### Phase 8 — Timeline fan-out hardening

1. Ensure `TweetPosted` records `FanOutTweet` via
   `MulacState::command_gateway()`.
2. Keep `FanOutTweet` internal (not in `TwitterCommand`).
3. `ON CONFLICT DO NOTHING` against the new `uq_timelines_user_tweet`
   unique index.
4. Add `tests/timeline_fan_out.rs` covering happy path, retry idempotency,
   and `command_entries` progression.

Acceptance:

- author with followers posts a tweet → timeline rows are created for
  followers
- retry does not duplicate timeline rows
- command journal shows `FanOutTweet` progressing to completed

### Phase 9 — Cleanup and documentation

1. Delete obsolete files:
   - `src/db.rs`, `src/state.rs`, `src/commands.rs`, `src/events.rs`
   - `src/tweets.rs`, `src/tweets/*`
   - `src/users.rs`, `src/users/*`
   - `src/likes.rs`, `src/likes/*`
   - `src/messages.rs`, `src/messages/*`
   - `src/timeline.rs`, `src/timeline/*`
   - `src/notifications.rs`, `src/notifications/*`
   - `src/main.rs` (already moved in Phase 3)
2. Update `docs/test_app_twitter.md` from aspirational to descriptive.
3. Create or update `test_app_twitter/AGENTS.md`. Call out the deliberate
   divergences from Todo (sync single-pool `start_mulac`, typed
   `InboundEntity`, optional `entity_id`).
4. Update Makefile + run docs.
5. Verify: `rg "pub mod (tweets|users|likes|messages|timeline|notifications|commands|events|db|state)" test_app_twitter/src`
   returns no hits.

---

## 14. Optional product-completion phases

Outside the core Todo-style reorganization. Do them only if the desired
final app behavior includes the broader Twitter product surface.

### Phase 10 — Timeline read API

1. Add `src/timeline_list.rs`.
2. Implement `GET /api/timeline/:user_id`.
3. Return timeline DTOs in deterministic order.
4. Add `tests/timeline_list.rs`.
5. Document the endpoint in `docs/test_app_twitter.md` and
   `test_app_twitter/AGENTS.md`.

Acceptance:

- `GET /api/timeline/:user_id` returns rows produced by fan-out
- empty timelines return an empty array
- deleted tweets are handled per documented policy

### Phase 11 — Mention notifications

1. Choose mention identity model:
   - simple: persist `mentioned_handle TEXT`
   - richer: add user profile/handle table and resolve to user ids
2. Add `src/mention_notification.rs`.
3. Add migration/table if persistence is selected.
4. Parse mentions from `TweetPosted` content (decoded per §10.8 via
   `TwitterEvent`).
5. Persist notification rows.
6. Emit or record `UserMentioned` consistently.
7. Register outbox subscriber for `UserMentioned`.
8. Add `tests/mention_notification.rs`.

Acceptance:

- content with `@handle` creates notification rows
- `UserMentioned` appears in `outbox_messages`
- content without mentions creates no notification
- retry does not duplicate notifications

---

## 15. Test strategy

Use Todo's `tests/utils.rs` model, adapted to Diesel.

`tests/utils.rs` should provide:

- `start_test_app() -> (base_url, DbPool, OwnedMutexGuard<()>)`
- database migration and cleanup
- root health polling
- shared app route assembly (mounts inbox + outbox + all feature APIs)
- row structs for:
  - `command_entries`, `event_entries`
  - `outbox_messages`, `inbox_messages`
  - `tweets`, `follows`, `likes`, `direct_messages`, `timelines`
  - optional `mention_notifications`
- helper functions:
  - `fetch_command_entries()`, `fetch_event_entries()`
  - `fetch_outbox()`, `fetch_inbox()`
  - `fetch_tweet_by_id()`, `fetch_timeline_for_user()`
  - optional `fetch_mentions()`
- `STATUS_COMPLETED: i32 = 5`

Use `reqwest` against a spawned real Poem server, not
`poem::test::TestClient`, so the harness matches Todo.

Minimum core test matrix:

| Test file                | Required assertions                                                                                                    |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| `tweet_post.rs`          | HTTP success, tweet row, command/event/outbox entries; blank/too-long (scalar count) → 400; duplicate `tweet_id` → 409 |
| `tweet_delete.rs`        | delete happy path, missing / already-deleted / mismatched author → 404, no event/outbox on 404                         |
| `tweet_retweet.rs`       | retweet row links original, missing original → 404, duplicate retweet_id → 409, event + outbox                         |
| `user_follow.rs`         | follow row, self-follow → 400, duplicate follow is 200 no-op with no event/outbox                                      |
| `user_unfollow.rs`       | existing relation removed with event/outbox; missing relation is 200 no-op with no event/outbox                        |
| `tweet_like.rs`          | like row, missing tweet → 404, duplicate like is 200 no-op with no event/outbox                                        |
| `tweet_unlike.rs`        | existing like removed with 204 + event/outbox; missing tweet → 404; missing like → 204 no-op                           |
| `direct_message_send.rs` | DM row, validation, event + outbox; duplicate `message_id` → 409 with no row/event/outbox                              |
| `timeline_fan_out.rs`    | followers receive timeline rows after `TweetPosted`; retry produces no duplicate rows                                  |
| `inbox.rs`               | command envelope success returns `InboundEntity`; duplicate id → 409; failed command → failed; malformed payload → 400 |
| `outbox.rs`              | list endpoint shape, `created_at` ordering, tagged payload shape, **no duplicate row for retried event_id**            |

Optional extension tests:

| Test file                 | Required assertions                                             |
| ------------------------- | --------------------------------------------------------------- |
| `timeline_list.rs`        | timeline read endpoint returns rows in documented order         |
| `mention_notification.rs` | `@handle` creates notification and `UserMentioned` outbox event |

---

## 16. Makefile and Docker Compose

Use `docker compose`, not direct `docker` or legacy `docker-compose`
commands. Decide ports up front and update `docker-compose.yml`, `Makefile`,
docs, and `tests/utils.rs` in the **same commit**.

Recommended non-conflicting defaults:

- HTTP bind: `127.0.0.1:33002`
- Postgres host port: `5433`
- RabbitMQ AMQP host port: `56730` (forward-compat only)
- RabbitMQ management port: `15680`

```make
DATABASE_URL ?= postgres://twitter:twitter@localhost:5433/twitter
BIND_ADDR ?= 127.0.0.1:33002
AMQP_URL ?= amqp://guest:guest@localhost:56730

.PHONY: up down migrate test reset serve check

up:      ; docker compose up -d
down:    ; docker compose down
migrate: ; DATABASE_URL=$(DATABASE_URL) cargo run -- migrate
reset:   ; docker compose down -v && docker compose up -d && DATABASE_URL=$(DATABASE_URL) cargo run -- migrate
test:    ; DATABASE_URL=$(DATABASE_URL) cargo test
check:   ; cargo fmt --check && cargo check
serve:   ; DATABASE_URL=$(DATABASE_URL) BIND_ADDR=$(BIND_ADDR) cargo run -- serve
```

If the current compose files use different ports, treat the change as a
port migration and call it out in `AGENTS.md`.

---

## 17. Documentation updates

### 17.1 `test_app_twitter/AGENTS.md`

Create or update a codebase reference matching `test_app_todo/AGENTS.md`:

- source map and feature module convention
- domain model (including `InboundEntity`)
- REST API table under `/api` and inbox command shapes
- command/event flow and the §9.4 outcome matrix
- database table map (`*_entries` vs `*_messages` distinction)
- test map and running-locally instructions (`serve` / `migrate`, `BIND_ADDR`)
- explicit Diesel persistence note
- **explicit list of intentional divergences from Todo:**
  - sync, single-Diesel-pool `start_mulac` (vs Todo's async dual-pool)
  - typed `InboundEntity` response union (vs Todo's `TodoDto`)
  - `Command::entity_id(&self) -> Option<Uuid>` (vs Todo's `todo_id -> Uuid`)
  - outbox row id == source `event_id` (Twitter only — Todo generates a
    fresh outbox id)

### 17.2 `docs/test_app_twitter.md`

Update from aspirational to implemented:

- replace nested layout with the actual flat layout
- document Diesel persistence explicitly
- document app-level `inbox_messages` / `outbox_messages` vs
  infrastructure `inbox_entries` / `outbox_entries`
- document root `/health`, root `/swagger`, and `/api` route prefix
- document `BIND_ADDR=127.0.0.1:33002` and `serve` / `migrate` subcommands
- document which flows are synchronously drained and which are
  worker/retry driven
- document known non-goals, including AMQP publishing if still
  unimplemented
- either document timeline-list and mention-notification behavior as
  implemented, or explicitly defer them

---

## 18. Remaining open questions

The intentionally still-open items (everything else is decided):

1. **Mention identity model.** Only relevant if Phase 11 is selected
   (handle text vs user-profile table).
2. **Outbox AMQP publishing.** Out of scope for the reorganization; track
   separately if desired.
3. **Optional Phase 3 compatibility aliases.** Decide at PR time based on
   real consumer breakage. Default per §9.5 is no aliases.

---

## 19. Risk controls

- Keep changes incremental; never bundle phases.
- Treat `tweet_post` as the canary feature through Phase 5.
- Do not delete old modules until replacement tests pass.
- Add app-level message tables before wiring app outbox/inbox.
- Keep Diesel; do not combine ORM migration with architecture reorg.
- Add `uq_timelines_user_tweet` before enabling retryable fan-out.
- Replace untagged event serialization before expanding event/outbox tests.
- Make optional product-completion work visibly separate from core
  reorganization.
- After every phase: `cargo fmt && cargo check && make test`.

---

## 20. Verification

After core phases land:

```bash
cargo check --manifest-path test_app_twitter/Cargo.toml
cargo test --manifest-path test_app_twitter/Cargo.toml
cargo run --manifest-path test_app_twitter/Cargo.toml -- migrate
DATABASE_URL=... cargo run --manifest-path test_app_twitter/Cargo.toml -- serve
```

Manual probes against a running server with `BIND_ADDR=127.0.0.1:33002`:

| Probe                         | Expected                                                                 |
| ----------------------------- | ------------------------------------------------------------------------ |
| `GET /health`                 | `200 ok`                                                                 |
| `GET /swagger`                | Swagger UI lists core endpoints + inbox + outbox                         |
| `POST /api/tweets`            | tweet row + completed command/event entries + pending outbox             |
| inspect `tweets`              | row exists                                                               |
| inspect `command_entries`     | `PostTweet`, completed status                                            |
| inspect `event_entries`       | `TweetPosted`, completed status                                          |
| inspect `outbox_messages`     | `TweetPosted`, `pending`, **row `id == event_id`**                       |
| `POST /api/messages/commands` | inbox row recorded, command dispatched, response carries `InboundEntity` |
| repost same inbox id          | `409 Conflict`                                                           |
| `GET /api/messages/outbox`    | returns tagged events in `created_at` order                              |
| follower posts → fan-out      | timeline rows for followers; retry produces no duplicates                |
| retry same event delivery     | **no duplicate `outbox_messages` row for that `event_id`**               |
| `DELETE /api/tweets/:id`      | `204 No Content`                                                         |

Optional extension probes:

| Probe                        | Expected                                                          |
| ---------------------------- | ----------------------------------------------------------------- |
| `GET /api/timeline/:user_id` | returns timeline rows in documented order                         |
| tweet content with `@handle` | mention notification row and `UserMentioned` outbox event created |

---

## 21. Completion checklist

Core reorganization complete when:

- [ ] Crate renamed to `test_app_twitter` (Phase 0).
- [ ] `src/` has Todo-like `assembly` structure and `serve`/`migrate` bin.
- [ ] Diesel kept for app data and migrations; `2025-01-01-*` untouched.
- [ ] Root `src/lib.rs` exposes only `pub mod prelude` plus root types.
- [ ] Every feature file exposes only `pub mod prelude`.
- [ ] No caller imports from internal feature submodules.
- [ ] `AppState { pool, mulac }` used everywhere; no `pub use kernel::AppState`.
- [ ] No `kernel::boot()` in production wiring.
- [ ] Two-phased gateways used for all state-changing routes.
- [ ] `command_entries` and `event_entries` written for state-changing
      commands.
- [ ] App-owned `inbox_messages` and `outbox_messages` APIs work.
- [ ] **Outbox rows are idempotent on source `event_id` (PK reuse).**
- [ ] Outbox subscriber registered before any domain subscriber per event.
- [ ] **Event-specific subscribers decode through the tagged `TwitterEvent`
      contract** (no subscriber-local flat payload structs).
- [ ] `run_command_worker` / `run_event_worker` spawned at boot.
- [ ] `TwitterEvent` is tagged.
- [ ] Tweet content validated as Unicode scalars, not bytes.
- [ ] `timeline_fan_out` uses `MulacState::command_gateway()`.
- [ ] Behavior matches the §9.4 outcome matrix.
- [ ] Split integration tests cover all core workflows including outbox
      event-id idempotency.
- [ ] `make test` passes.
- [ ] `cargo run -- migrate` applies all migrations on a fresh DB.
- [ ] Obsolete files removed.
- [ ] `docs/test_app_twitter.md` and `AGENTS.md` match the live code,
      including the documented divergences from Todo.

Extension scope is complete when selected and implemented:

- [ ] `GET /api/timeline/:user_id` exists and is tested.
- [ ] Mention identity model is documented.
- [ ] Mention notifications are persisted and idempotent.
- [ ] `UserMentioned` appears in `outbox_messages`.
- [ ] Optional extension docs and tests are green.

If extension scope is not selected, completion instead requires:

- [ ] `docs/test_app_twitter.md` explicitly says timeline read API is
      deferred.
- [ ] `docs/test_app_twitter.md` explicitly says mention notifications are
      deferred.
- [ ] `test_app_twitter/AGENTS.md` reflects those deferrals.
