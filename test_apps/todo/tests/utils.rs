use std::sync::{Arc, OnceLock};

use diesel::prelude::*;
use poem::{EndpointExt, Route, get, handler, middleware::AddData};
use poem_openapi::OpenApiService;
use reqwest::Client;
use test_app_todo::io::{
    AppState, CompleteApi, CreateApi, DbPool, DeleteApi, DueDatesApi, GetApi, InboxApi, ListApi,
    OutboxApi, ReopenApi, UpdateApi, build_pool, run_migrations, start_mulac,
};
use tokio::sync::{Mutex, OwnedMutexGuard};
use uuid::Uuid;

pub const STATUS_COMPLETED: i32 = 5;

#[derive(diesel::QueryableByName)]
pub struct OutboxRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    pub id: Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub event_type: String,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    pub payload: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub status: String,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    pub created_at: chrono::DateTime<chrono::Utc>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    pub published_at: Option<chrono::DateTime<chrono::Utc>>,
    #[diesel(sql_type = diesel::sql_types::Int4)]
    pub attempts: i32,
}

#[derive(diesel::QueryableByName)]
pub struct InboxRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    pub id: Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub message_type: String,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    pub payload: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub status: String,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    pub received_at: chrono::DateTime<chrono::Utc>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    pub processed_at: Option<chrono::DateTime<chrono::Utc>>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub error: Option<String>,
}

#[derive(diesel::QueryableByName)]
pub struct CommandEntryRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    pub id: Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub command_type: String,
    #[diesel(sql_type = diesel::sql_types::Int4)]
    pub status: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub payload: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    pub meta: Option<serde_json::Value>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    pub extra_info: Option<serde_json::Value>,
    #[diesel(sql_type = diesel::sql_types::Int4)]
    pub attempts: i32,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
    pub reservation_id: Option<Uuid>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    pub processed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(diesel::QueryableByName)]
pub struct EventEntryRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    pub id: Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub event_type: String,
    #[diesel(sql_type = diesel::sql_types::Int4)]
    pub status: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub payload: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    pub meta: Option<serde_json::Value>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    pub extra_info: Option<serde_json::Value>,
    #[diesel(sql_type = diesel::sql_types::Int4)]
    pub attempts: i32,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
    pub reservation_id: Option<Uuid>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    pub processed_at: Option<chrono::DateTime<chrono::Utc>>,
}

pub async fn fetch_outbox(pool: &DbPool) -> Vec<OutboxRow> {
    let mut conn = pool.get().unwrap();
    diesel::sql_query(
        "SELECT id, event_type, payload, status, created_at, published_at, attempts FROM outbox_messages",
    )
    .load::<OutboxRow>(&mut conn)
    .unwrap()
}

pub async fn fetch_command_entries(pool: &DbPool) -> Vec<CommandEntryRow> {
    let mut conn = pool.get().unwrap();
    diesel::sql_query(
        "SELECT id, command_type, status, payload, meta, extra_info, attempts, reservation_id, processed_at FROM command_entries ORDER BY received_at ASC",
    )
    .load::<CommandEntryRow>(&mut conn)
    .unwrap()
}

pub async fn fetch_event_entries(pool: &DbPool) -> Vec<EventEntryRow> {
    let mut conn = pool.get().unwrap();
    diesel::sql_query(
        "SELECT id, event_type, status, payload, meta, extra_info, attempts, reservation_id, processed_at FROM event_entries ORDER BY received_at ASC",
    )
    .load::<EventEntryRow>(&mut conn)
    .unwrap()
}

pub async fn fetch_inbox(pool: &DbPool) -> Vec<InboxRow> {
    let mut conn = pool.get().unwrap();
    diesel::sql_query(
        "SELECT id, message_type, payload, status, received_at, processed_at, error FROM inbox_messages",
    )
    .load::<InboxRow>(&mut conn)
    .unwrap()
}

#[derive(diesel::QueryableByName)]
pub struct TodoRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    pub id: Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub title: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub description: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub status: String,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    pub created_at: chrono::DateTime<chrono::Utc>,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    pub updated_at: chrono::DateTime<chrono::Utc>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    pub due_at: Option<chrono::DateTime<chrono::Utc>>,
}

pub async fn fetch_todo_row(pool: &DbPool, id: Uuid) -> TodoRow {
    let mut conn = pool.get().unwrap();
    diesel::sql_query(
        "SELECT id, title, description, status, created_at, updated_at, due_at FROM todos WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .load::<TodoRow>(&mut conn)
    .unwrap()
    .into_iter()
    .next()
    .expect("todo row not found")
}

pub async fn count_todos(pool: &DbPool, id: Uuid) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }

    let mut conn = pool.get().unwrap();
    diesel::sql_query("SELECT count(*) AS count FROM todos WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .load::<Count>(&mut conn)
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .count
}

#[handler]
fn health() -> &'static str {
    "ok"
}

fn test_lock() -> Arc<Mutex<()>> {
    static LOCK: OnceLock<Arc<Mutex<()>>> = OnceLock::new();
    LOCK.get_or_init(|| Arc::new(Mutex::new(()))).clone()
}

pub async fn start_test_app() -> (String, DbPool, OwnedMutexGuard<()>) {
    let guard = test_lock().lock_owned().await;
    dotenvy::dotenv().ok();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    run_migrations(&database_url).unwrap();
    let pool = build_pool(&database_url).unwrap();

    {
        let mut conn = pool.get().unwrap();
        diesel::sql_query(
            "TRUNCATE TABLE event_entries, command_entries, outbox_messages, inbox_messages, todos RESTART IDENTITY CASCADE",
        )
        .execute(&mut conn)
        .unwrap();
    }

    let kernel = start_mulac(pool.clone()).await.unwrap();
    let state = AppState::new(pool.clone(), kernel.state());

    let api = OpenApiService::new(
        (
            CreateApi,
            ListApi,
            GetApi,
            UpdateApi,
            CompleteApi,
            ReopenApi,
            DeleteApi,
            DueDatesApi,
            InboxApi,
            OutboxApi,
        ),
        "test_app_todo",
        "0.1.0",
    );

    let app = Route::new()
        .at("/health", get(health))
        .nest("/api", api)
        .with(AddData::new(state));

    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = std_listener.local_addr().unwrap().port();
    drop(std_listener);

    let base_url = format!("http://127.0.0.1:{port}");
    tokio::spawn(
        poem::Server::new(poem::listener::TcpListener::bind(format!(
            "127.0.0.1:{port}"
        )))
        .run(app),
    );

    let client = Client::new();
    for _ in 0..20 {
        if client
            .get(format!("{base_url}/health"))
            .send()
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    (base_url, pool, guard)
}

pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap()
}

pub async fn assert_outbox_pending(pool: &DbPool, event_type: &str) {
    let outbox = fetch_outbox(pool).await;
    let matching: Vec<_> = outbox
        .iter()
        .filter(|r| r.event_type == event_type)
        .collect();
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].status, "pending");
}

pub async fn assert_command_completed(pool: &DbPool, command_type: &str) {
    let cmds = fetch_command_entries(pool).await;
    let matching: Vec<_> = cmds
        .iter()
        .filter(|c| c.command_type == command_type)
        .collect();
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].status, STATUS_COMPLETED);
}

pub async fn assert_event_completed(pool: &DbPool, event_type: &str) {
    let events = fetch_event_entries(pool).await;
    let matching: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == event_type)
        .collect();
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].status, STATUS_COMPLETED);
}

macro_rules! assert_ok_response {
    ($resp:expr) => {
        assert_eq!($resp.status(), 200)
    };
}
pub(crate) use assert_ok_response;

macro_rules! assert_bad_request_response {
    ($resp:expr) => {
        assert_eq!($resp.status(), 400)
    };
}
pub(crate) use assert_bad_request_response;

macro_rules! assert_not_found_response {
    ($resp:expr) => {
        assert_eq!($resp.status(), 404)
    };
}
pub(crate) use assert_not_found_response;

macro_rules! assert_no_content_response {
    ($resp:expr) => {
        assert_eq!($resp.status(), 204)
    };
}
pub(crate) use assert_no_content_response;

macro_rules! assert_conflict_response {
    ($resp:expr) => {
        assert_eq!($resp.status(), 409)
    };
}
pub(crate) use assert_conflict_response;
