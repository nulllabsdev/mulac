#![cfg(feature = "diesel")]

use std::env;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;

use chrono::{Duration, Utc};
use diesel::connection::SimpleConnection;
use diesel::deserialize::{FromSql, Result as DeserializeResult};
use diesel::pg::{Pg, PgValue};
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Int4, Jsonb, Nullable, Text, Timestamptz, Uuid as SqlUuid};
use mulac_outbox::prelude::*;
use serde_json::from_slice;
use uuid::Uuid;

static DB_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(QueryableByName)]
struct EntryRow {
    #[diesel(sql_type = SqlUuid)]
    id: Uuid,
    #[diesel(sql_type = Int4)]
    status: i32,
    #[diesel(sql_type = Int4)]
    attempts: i32,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    reservation_id: Option<Uuid>,
    #[diesel(sql_type = Timestamptz)]
    scheduled_at: chrono::DateTime<Utc>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    reserved_at: Option<chrono::DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    processed_at: Option<chrono::DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Text>)]
    last_error: Option<String>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    extra_info: Option<ExtraInfoJsonb>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct ExtraInfoJsonb {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    errors: Vec<String>,
}

impl From<ExtraInfoJsonb> for ExtraInfo {
    fn from(value: ExtraInfoJsonb) -> Self {
        ExtraInfo::with_errors(value.errors)
    }
}

impl FromSql<Jsonb, Pg> for ExtraInfoJsonb {
    fn from_sql(bytes: PgValue<'_>) -> DeserializeResult<Self> {
        let bytes = bytes.as_bytes();
        if bytes.is_empty() {
            return Err("empty jsonb value".into());
        }
        if bytes[0] != 1 {
            return Err(format!("unsupported jsonb version: {}", bytes[0]).into());
        }
        Ok(from_slice(&bytes[1..])?)
    }
}

fn database_url() -> String {
    env::var("OUTBOX_TEST_DATABASE_URL")
        .expect("OUTBOX_TEST_DATABASE_URL must be set to run Diesel integration tests")
}

fn test_pool() -> DbPool {
    build_pool(&database_url()).expect("pool builds")
}

fn reset_schema(pool: &DbPool) {
    let _guard = DB_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
    let mut conn = pool.get().expect("pool connection");
    conn.batch_execute("DROP TABLE IF EXISTS outbox_entries;")
        .expect("drop table");
    conn.batch_execute(include_str!("../../../docs/outbox_entries.sql"))
        .expect("create schema");
}

fn metadata(event_id: Uuid, routing_key: &str) -> OutboxEntryMetadata {
    OutboxEntryMetadata {
        sequence: None,
        event_id,
        message_id: event_id,
        correlation_id: None,
        causation_id: None,
        event_type: "UserRegistered".into(),
        routing_key: routing_key.into(),
        source: Some("identity-service".into()),
        content_type: Some("application/json".into()),
    }
}

fn new_entry(event_id: Uuid) -> NewOutboxEntry {
    let now = Utc::now();
    NewOutboxEntry {
        id: event_id,
        payload: "{}".into(),
        meta: metadata(event_id, "users.registered"),
        scheduled_at: now,
        received_at: now,
    }
}

fn fetch_count(pool: &DbPool) -> i64 {
    let mut conn = pool.get().expect("pool connection");
    diesel::sql_query("SELECT COUNT(*) AS count FROM outbox_entries")
        .get_result::<CountRow>(&mut conn)
        .expect("count query")
        .count
}

fn fetch_entry(pool: &DbPool, id: Uuid) -> EntryRow {
    let mut conn = pool.get().expect("pool connection");
    diesel::sql_query(
        r#"
        SELECT id, status, attempts, reservation_id, scheduled_at, reserved_at, processed_at, last_error, extra_info
        FROM outbox_entries
        WHERE id = $1
        "#,
    )
    .bind::<SqlUuid, _>(id)
    .get_result::<EntryRow>(&mut conn)
    .expect("entry query")
}

#[ignore = "requires OUTBOX_TEST_DATABASE_URL"]
#[test]
fn idempotent_recording_keeps_one_row() {
    let pool = test_pool();
    reset_schema(&pool);
    let store = DieselPgOutboxRepository::new(pool.clone());
    let event_id = Uuid::now_v7();
    let entry = new_entry(event_id);

    store.record(&entry).expect("first record succeeds");
    store.record(&entry).expect("duplicate record succeeds");

    assert_eq!(fetch_count(&pool), 1);
}

#[ignore = "requires OUTBOX_TEST_DATABASE_URL"]
#[test]
fn concurrent_reservations_do_not_claim_the_same_row() {
    let pool = test_pool();
    reset_schema(&pool);
    let store = DieselPgOutboxRepository::new(pool.clone());
    let storage = Arc::new(DieselPgOutboxRepository::new(pool.clone()));

    let first_id = Uuid::now_v7();
    let second_id = Uuid::now_v7();
    store
        .record(&new_entry(first_id))
        .expect("store first entry");
    store
        .record(&new_entry(second_id))
        .expect("store second entry");

    let left = {
        let storage = storage.clone();
        thread::spawn(move || {
            storage
                .reserve(&ReservableOutboxSpec::new(1))
                .expect("reserve left")
        })
    };
    let right = {
        let storage = storage.clone();
        thread::spawn(move || {
            storage
                .reserve(&ReservableOutboxSpec::new(1))
                .expect("reserve right")
        })
    };

    let mut ids = left
        .join()
        .expect("left thread")
        .into_iter()
        .chain(right.join().expect("right thread"))
        .map(|entry| entry.message.id)
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();

    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&first_id));
    assert!(ids.contains(&second_id));
}

#[ignore = "requires OUTBOX_TEST_DATABASE_URL"]
#[test]
fn lifecycle_transitions_update_status_and_metadata() {
    let pool = test_pool();
    reset_schema(&pool);
    let store = DieselPgOutboxRepository::new(pool.clone());
    let storage = DieselPgOutboxRepository::new(pool.clone());

    let completed_id = Uuid::now_v7();
    store
        .record(&new_entry(completed_id))
        .expect("store completed entry");
    let completed = storage
        .reserve(&ReservableOutboxSpec::new(1))
        .expect("reserve completed entry")
        .pop()
        .expect("reserved row");
    let completed_reservation_id = completed.message.reservation_id.expect("reservation id");
    storage
        .completed(completed_id, completed_reservation_id)
        .expect("complete row");
    let completed_row = fetch_entry(&pool, completed_id);
    assert_eq!(completed_row.id, completed_id);
    assert_eq!(completed_row.status, i32::from(OutboxStatus::Completed));
    assert!(completed_row.processed_at.is_some());
    assert!(completed_row.reservation_id.is_none());
    assert!(completed_row.extra_info.is_none());

    let failed_id = Uuid::now_v7();
    store
        .record(&new_entry(failed_id))
        .expect("store failed entry");
    let failed = storage
        .reserve(&ReservableOutboxSpec::new(1))
        .expect("reserve failed entry")
        .pop()
        .expect("reserved row");
    let failed_reservation_id = failed.message.reservation_id.expect("reservation id");
    storage
        .failed(
            failed_id,
            failed_reservation_id,
            6,
            Some("broker unavailable".into()),
        )
        .expect("fail row");
    let failed_row = fetch_entry(&pool, failed_id);
    assert_eq!(failed_row.id, failed_id);
    assert_eq!(failed_row.status, i32::from(OutboxStatus::Failed));
    assert!(failed_row.scheduled_at > Utc::now() - Duration::seconds(1));
    assert_eq!(failed_row.last_error.as_deref(), Some("broker unavailable"));
    assert_eq!(
        failed_row.extra_info.map(Into::into),
        Some(ExtraInfo::with_error("broker unavailable"))
    );

    let dead_id = Uuid::now_v7();
    store.record(&new_entry(dead_id)).expect("store dead entry");
    let dead = storage
        .reserve(&ReservableOutboxSpec::new(1))
        .expect("reserve dead entry")
        .pop()
        .expect("reserved row");
    let dead_reservation_id = dead.message.reservation_id.expect("reservation id");
    storage
        .dead(dead_id, dead_reservation_id, Some("invalid payload".into()))
        .expect("dead row");
    let dead_row = fetch_entry(&pool, dead_id);
    assert_eq!(dead_row.id, dead_id);
    assert_eq!(dead_row.status, i32::from(OutboxStatus::Dead));
    assert!(dead_row.reservation_id.is_none());
    assert_eq!(dead_row.last_error.as_deref(), Some("invalid payload"));
    assert_eq!(
        dead_row.extra_info.map(Into::into),
        Some(ExtraInfo::with_error("invalid payload"))
    );
}

#[ignore = "requires OUTBOX_TEST_DATABASE_URL"]
#[test]
fn stale_sweep_releases_old_reservations_without_incrementing_attempts() {
    let pool = test_pool();
    reset_schema(&pool);
    let mut conn = pool.get().expect("pool connection");
    let event_id = Uuid::now_v7();
    let reservation_id = Uuid::now_v7();
    let now = Utc::now();

    diesel::sql_query(
        r#"
        INSERT INTO outbox_entries (
            id, status, payload, meta, scheduled_at, attempts, reservation_id, reserved_at, received_at, updated_at
        ) VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind::<SqlUuid, _>(event_id)
    .bind::<Int4, _>(i32::from(OutboxStatus::Reserved))
    .bind::<Text, _>("{}")
    .bind::<Text, _>(serde_json::to_string(&metadata(event_id, "users.registered")).unwrap())
    .bind::<Timestamptz, _>(now - Duration::minutes(10))
    .bind::<Int4, _>(1)
    .bind::<SqlUuid, _>(reservation_id)
    .bind::<Timestamptz, _>(now - Duration::minutes(10))
    .bind::<Timestamptz, _>(now - Duration::minutes(10))
    .bind::<Timestamptz, _>(now - Duration::minutes(10))
    .execute(&mut conn)
    .expect("seed reserved row");

    let storage = DieselPgOutboxRepository::new(pool.clone());
    let swept = storage
        .sweep(&StaleReservationSpec::new(Duration::minutes(5)))
        .expect("sweep succeeds");

    assert_eq!(swept, 1);
    let row = fetch_entry(&pool, event_id);
    assert_eq!(row.id, event_id);
    assert_eq!(row.status, i32::from(OutboxStatus::Failed));
    assert_eq!(row.attempts, 1);
    assert!(row.reservation_id.is_none());
    assert!(row.reserved_at.is_none());
    assert!(row.scheduled_at > now);
    assert_eq!(
        row.extra_info.map(Into::into),
        Some(ExtraInfo::with_error("publication reservation timed out"))
    );

    // Sweeping intentionally schedules a future retry. Make it due explicitly
    // before testing the read-side preservation of timeout diagnostics.
    diesel::sql_query(
        "UPDATE outbox_entries SET scheduled_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind::<SqlUuid, _>(event_id)
    .execute(&mut conn)
    .unwrap();
    let swept = storage
        .reserve(&ReservableOutboxSpec::new(1))
        .expect("re-reserve swept row")
        .pop()
        .expect("reserved swept row");
    assert_eq!(
        swept.message.extra_info,
        Some(ExtraInfo::with_error("publication reservation timed out"))
    );
}
