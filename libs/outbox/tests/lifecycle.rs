#![cfg(feature = "diesel")]

use std::env;
use std::sync::{Arc, Mutex, OnceLock};

use chrono::{Duration, Utc};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_types::{Int4, Nullable, Timestamptz, Uuid as SqlUuid};
use mulac_outbox::prelude::*;
use uuid::Uuid;

static DB_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(QueryableByName)]
struct EntryRow {
    #[diesel(sql_type = SqlUuid)]
    id: Uuid,
    #[diesel(sql_type = Int4)]
    status: i32,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    reservation_id: Option<Uuid>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    processed_at: Option<chrono::DateTime<Utc>>,
}

struct FakePublisher {
    published_message_ids: Mutex<Vec<Uuid>>,
    error: Option<String>,
}

impl FakePublisher {
    fn success() -> Self {
        Self {
            published_message_ids: Mutex::new(vec![]),
            error: None,
        }
    }

    fn failing(message: &str) -> Self {
        Self {
            published_message_ids: Mutex::new(vec![]),
            error: Some(message.into()),
        }
    }
}

impl OutboxPublisherPort for FakePublisher {
    fn publish(&self, envelope: OutboundMessageEnvelope) -> Result<(), OutboxError> {
        self.published_message_ids
            .lock()
            .unwrap()
            .push(envelope.metadata.message_id);

        match &self.error {
            Some(message) => Err(OutboxError::Transport(message.clone())),
            None => Ok(()),
        }
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

fn metadata(event_id: Uuid, routing_key: &str) -> NewOutboxMetadata {
    NewOutboxMetadata {
        sequence: None,
        event_id,
        message_id: None,
        correlation_id: None,
        causation_id: None,
        event_type: "UserRegistered".into(),
        routing_key: routing_key.into(),
        source: Some("identity-service".into()),
        content_type: Some("application/json".into()),
    }
}

fn envelope(event_id: Uuid) -> NewOutboxEnvelope {
    NewOutboxEnvelope {
        payload: "{}".into(),
        metadata: metadata(event_id, "users.registered"),
    }
}

fn fetch_entry(pool: &DbPool, id: Uuid) -> EntryRow {
    let mut conn = pool.get().expect("pool connection");
    diesel::sql_query(
        r#"
        SELECT id, status, reservation_id, processed_at
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
fn record_reserve_publish_complete_updates_row() {
    let pool = test_pool();
    reset_schema(&pool);
    let recorder = OutboxRecorder::new(Arc::new(DieselPgOutboxRepository::new(pool.clone())));
    let publisher = Arc::new(FakePublisher::success());
    let consumer = OutboxConsumer::new(
        Arc::new(DieselPgOutboxRepository::new(pool.clone())),
        publisher.clone(),
    );
    let event_id = Uuid::now_v7();

    recorder
        .record(&envelope(event_id))
        .expect("record succeeds");
    consumer
        .publish_batch(&ReservableOutboxSpec::new(10))
        .expect("publish batch succeeds");

    let row = fetch_entry(&pool, event_id);
    assert_eq!(row.id, event_id);
    assert_eq!(row.status, i32::from(OutboxStatus::Completed));
    assert!(row.processed_at.is_some());
    assert!(row.reservation_id.is_none());
    assert_eq!(
        publisher.published_message_ids.lock().unwrap().as_slice(),
        &[event_id]
    );
}

#[ignore = "requires OUTBOX_TEST_DATABASE_URL"]
#[test]
fn transport_failure_moves_entry_to_failed() {
    let pool = test_pool();
    reset_schema(&pool);
    let recorder = OutboxRecorder::new(Arc::new(DieselPgOutboxRepository::new(pool.clone())));
    let publisher = Arc::new(FakePublisher::failing("broker unavailable"));
    let consumer = OutboxConsumer::new(
        Arc::new(DieselPgOutboxRepository::new(pool.clone())),
        publisher.clone(),
    );
    let event_id = Uuid::now_v7();

    recorder
        .record(&envelope(event_id))
        .expect("record succeeds");
    assert!(
        consumer
            .publish_batch(&ReservableOutboxSpec::new(10))
            .is_err()
    );

    let row = fetch_entry(&pool, event_id);
    assert_eq!(row.status, i32::from(OutboxStatus::Failed));
    assert!(row.reservation_id.is_none());
}

#[ignore = "requires OUTBOX_TEST_DATABASE_URL"]
#[test]
fn failed_entries_become_reservable_after_scheduled_at() {
    let pool = test_pool();
    reset_schema(&pool);
    let recorder = OutboxRecorder::new(Arc::new(DieselPgOutboxRepository::new(pool.clone())));
    let publisher = Arc::new(FakePublisher::failing("broker unavailable"));
    let consumer = OutboxConsumer::new(
        Arc::new(DieselPgOutboxRepository::new(pool.clone())),
        publisher,
    );
    let storage = DieselPgOutboxRepository::new(pool.clone());
    let event_id = Uuid::now_v7();

    recorder
        .record(&envelope(event_id))
        .expect("record succeeds");
    assert!(
        consumer
            .publish_batch(&ReservableOutboxSpec::new(10))
            .is_err()
    );

    let mut conn = pool.get().expect("pool connection");
    diesel::sql_query("UPDATE outbox_entries SET scheduled_at = $2 WHERE id = $1")
        .bind::<SqlUuid, _>(event_id)
        .bind::<Timestamptz, _>(Utc::now() - Duration::seconds(1))
        .execute(&mut conn)
        .expect("force entry eligible");

    let reserved = storage
        .reserve(&ReservableOutboxSpec::new(10))
        .expect("reserve failed entry");
    assert_eq!(reserved.len(), 1);
    assert_eq!(reserved[0].message.id, event_id);
}

#[ignore = "requires OUTBOX_TEST_DATABASE_URL"]
#[test]
fn conversion_failure_moves_entry_to_failed() {
    let pool = test_pool();
    reset_schema(&pool);
    let recorder = OutboxRecorder::new(Arc::new(DieselPgOutboxRepository::new(pool.clone())));
    let publisher = Arc::new(FakePublisher::success());
    let consumer = OutboxConsumer::new(
        Arc::new(DieselPgOutboxRepository::new(pool.clone())),
        publisher,
    );
    let event_id = Uuid::now_v7();

    recorder
        .record(&envelope(event_id))
        .expect("record succeeds");

    let mut conn = pool.get().expect("pool connection");
    diesel::sql_query(
        r#"
        UPDATE outbox_entries
        SET meta = jsonb_set(meta, '{routing_key}', '"   "')
        WHERE id = $1
        "#,
    )
    .bind::<SqlUuid, _>(event_id)
    .execute(&mut conn)
    .expect("blank routing key");

    assert!(
        consumer
            .publish_batch(&ReservableOutboxSpec::new(10))
            .is_err()
    );

    let row = fetch_entry(&pool, event_id);
    assert_eq!(row.status, i32::from(OutboxStatus::Failed));
    assert!(row.reservation_id.is_none());
}

#[test]
fn duplicate_publish_after_completion_failure_keeps_message_id_stable() {
    struct RepeatReserve {
        entry: OutboxEntryEnvelope,
    }

    impl OutboxReservePort for RepeatReserve {
        fn reserve(
            &self,
            _spec: &ReservableOutboxSpec,
        ) -> Result<Vec<OutboxEntryEnvelope>, OutboxError> {
            Ok(vec![self.entry.clone()])
        }
    }

    struct FlakyProcess {
        completed_calls: Mutex<usize>,
    }

    impl OutboxProcessPort for FlakyProcess {
        fn completed(&self, _id: Uuid, _reservation_id: Uuid) -> Result<(), OutboxError> {
            let mut calls = self.completed_calls.lock().unwrap();
            *calls += 1;
            if *calls == 1 {
                Err(OutboxError::Reservation("completion failed".into()))
            } else {
                Ok(())
            }
        }

        fn failed(
            &self,
            _id: Uuid,
            _reservation_id: Uuid,
            _max_attempts: i32,
            _reason: Option<String>,
        ) -> Result<(), OutboxError> {
            Ok(())
        }

        fn dead(
            &self,
            _id: Uuid,
            _reservation_id: Uuid,
            _reason: Option<String>,
        ) -> Result<(), OutboxError> {
            Ok(())
        }
    }

    let event_id = Uuid::now_v7();
    let reservation_id = Uuid::now_v7();
    let now = Utc::now();
    let metadata = OutboxEntryMetadata {
        sequence: None,
        event_id,
        message_id: event_id,
        correlation_id: None,
        causation_id: None,
        event_type: "UserRegistered".into(),
        routing_key: "users.registered".into(),
        source: Some("identity-service".into()),
        content_type: Some("application/json".into()),
    };
    let entry = OutboxEntryEnvelope {
        message: OutboxEntry {
            id: event_id,
            status: OutboxStatus::Reserved,
            payload: "{}".into(),
            meta: metadata.clone(),
            scheduled_at: now,
            attempts: 1,
            reservation_id: Some(reservation_id),
            reserved_at: Some(now),
            received_at: now,
            updated_at: now,
            processed_at: None,
            last_error: None,
            extra_info: None,
        },
        metadata,
    };
    let publisher = Arc::new(FakePublisher::success());
    let repository = Arc::new(TestRepository {
        reserve: Arc::new(RepeatReserve { entry }),
        process: Arc::new(FlakyProcess {
            completed_calls: Mutex::new(0),
        }),
    });
    let consumer = OutboxConsumer::new(repository, publisher.clone());

    assert!(
        consumer
            .publish_batch(&ReservableOutboxSpec::new(1))
            .is_err()
    );
    consumer
        .publish_batch(&ReservableOutboxSpec::new(1))
        .expect("second completion succeeds");

    assert_eq!(
        publisher.published_message_ids.lock().unwrap().as_slice(),
        &[event_id, event_id]
    );
}

struct TestRepository {
    reserve: Arc<dyn OutboxReservePort>,
    process: Arc<dyn OutboxProcessPort>,
}
impl OutboxReservePort for TestRepository {
    fn reserve(&self, s: &ReservableOutboxSpec) -> Result<Vec<OutboxEntryEnvelope>, OutboxError> {
        self.reserve.reserve(s)
    }
}
impl OutboxProcessPort for TestRepository {
    fn completed(&self, id: Uuid, r: Uuid) -> Result<(), OutboxError> {
        self.process.completed(id, r)
    }
    fn failed(&self, id: Uuid, r: Uuid, m: i32, e: Option<String>) -> Result<(), OutboxError> {
        self.process.failed(id, r, m, e)
    }
    fn dead(&self, id: Uuid, r: Uuid, e: Option<String>) -> Result<(), OutboxError> {
        self.process.dead(id, r, e)
    }
}
