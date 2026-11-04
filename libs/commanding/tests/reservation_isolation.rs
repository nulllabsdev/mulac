#![cfg(feature = "diesel")]
//! Real PostgreSQL regressions. Each test owns a schema; no application tables are touched.
use chrono::{DateTime, Utc};
use diesel::sql_types::{Int4, Jsonb, Nullable, Timestamptz, Uuid as SqlUuid};
use diesel::{
    connection::SimpleConnection,
    prelude::*,
    r2d2::{ConnectionManager, CustomizeConnection, Pool},
};
use mulac_commanding::prelude::*;
use serde_json::{Value, json};
use std::sync::{Arc, Barrier};
use uuid::Uuid;
type TestPool = Pool<ConnectionManager<PgConnection>>;
const TABLE: &str = "command_entries";
#[derive(Debug)]
struct SearchPath(String);
impl CustomizeConnection<PgConnection, diesel::r2d2::Error> for SearchPath {
    fn on_acquire(&self, conn: &mut PgConnection) -> Result<(), diesel::r2d2::Error> {
        conn.batch_execute(&format!(
            "SET search_path TO {}; SET statement_timeout TO '10s'",
            self.0
        ))
        .map_err(diesel::r2d2::Error::QueryError)
    }
}
struct Fixture {
    schema: String,
    pool: TestPool,
}
impl Fixture {
    fn new() -> Self {
        let url = std::env::var("DATABASE_URL_TEST")
            .expect("set DATABASE_URL_TEST to a disposable PostgreSQL database");
        let schema = format!("reservation_{}", Uuid::new_v4().simple());
        let mut conn = PgConnection::establish(&url).unwrap();
        conn.batch_execute(&format!("CREATE SCHEMA {schema}"))
            .unwrap();
        let pool = Pool::builder()
            .max_size(4)
            .connection_timeout(std::time::Duration::from_secs(3))
            .connection_customizer(Box::new(SearchPath(schema.clone())))
            .build(ConnectionManager::<PgConnection>::new(url))
            .unwrap();
        pool.get().unwrap().batch_execute(&format!("CREATE TABLE {TABLE} (
            id uuid PRIMARY KEY, status int4 NOT NULL DEFAULT 0, payload text NOT NULL DEFAULT '{{}}',
            command_type text NOT NULL DEFAULT 'test', event_type text NOT NULL DEFAULT 'test',
            meta jsonb, attempts int4 NOT NULL DEFAULT 0,
            reservation_id uuid, reserved_at timestamptz, received_at timestamptz NOT NULL DEFAULT now(),
            scheduled_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
            processed_at timestamptz, extra_info jsonb, last_error text
        )")).unwrap();
        Self { schema, pool }
    }
    fn repository(&self) -> DieselPgCommandRepository {
        DieselPgCommandRepository::new(self.pool.clone())
    }
    fn seed(&self, n: u128, meta: Value, extra: Option<Value>) {
        diesel::sql_query(format!("INSERT INTO {TABLE} (id,meta,extra_info,scheduled_at) VALUES ($1,$2,$3,to_timestamp($4))"))
            .bind::<SqlUuid,_>(Uuid::from_u128(n)).bind::<Jsonb,_>(meta).bind::<Nullable<Jsonb>,_>(extra)
            .bind::<diesel::sql_types::BigInt,_>(n as i64).execute(&mut self.pool.get().unwrap()).unwrap();
    }
    fn rows(&self) -> Vec<Row> {
        diesel::sql_query(format!("SELECT id,status,attempts,reservation_id,reserved_at,scheduled_at,updated_at,meta,extra_info FROM {TABLE} ORDER BY id"))
            .load(&mut self.pool.get().unwrap()).unwrap()
    }
    fn sql(&self, sql: &str) {
        self.pool.get().unwrap().batch_execute(sql).unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Ok(mut conn) = self.pool.get() {
            let _ = conn.batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema));
        }
    }
}
#[derive(QueryableByName, Debug, PartialEq)]
struct Row {
    #[diesel(sql_type=SqlUuid)]
    id: Uuid,
    #[diesel(sql_type=Int4)]
    status: i32,
    #[diesel(sql_type=Int4)]
    attempts: i32,
    #[diesel(sql_type=Nullable<SqlUuid>)]
    reservation_id: Option<Uuid>,
    #[diesel(sql_type=Nullable<Timestamptz>)]
    reserved_at: Option<DateTime<Utc>>,
    #[diesel(sql_type=Timestamptz)]
    scheduled_at: DateTime<Utc>,
    #[diesel(sql_type=Timestamptz)]
    updated_at: DateTime<Utc>,
    #[diesel(sql_type=Jsonb)]
    meta: Value,
    #[diesel(sql_type=Nullable<Jsonb>)]
    extra_info: Option<Value>,
}
fn metadata(n: u128) -> Value {
    let id = Uuid::from_u128(n);
    json!({"command_id": id})
}
fn identity(entry: &CommandEnvelope) -> (Uuid, Uuid) {
    (entry.command.id, entry.command.reservation_id)
}

#[test]
#[ignore = "requires DATABASE_URL_TEST"]
fn malformed_rows_do_not_discard_healthy_reservations() {
    let mut malformed = vec![
        (json!("not metadata"), None),
        (metadata(2), Some(json!({"errors":[42]}))),
    ];
    malformed.push((json!({"message_id":42,"command_id":42}), None));
    for (bad_meta, bad_extra) in malformed {
        let fixture = Fixture::new();
        fixture.seed(1, metadata(1), None);
        fixture.seed(
            2,
            bad_meta.clone(),
            bad_extra
                .clone()
                .or(Some(json!({"errors":["old"],"other":42}))),
        );
        fixture.seed(3, metadata(3), None);
        let repository = fixture.repository();
        let spec = ReservableCommandSpec::new(3).with_max_attempts(1);
        let entries = repository.reserve(&spec).unwrap();
        let mut ids: Vec<_> = entries.iter().map(|e| identity(e).0).collect();
        ids.sort();
        assert_eq!(ids, vec![Uuid::from_u128(1), Uuid::from_u128(3)]);
        let rows = fixture.rows();
        assert_eq!(rows[1].status, 4);
        assert_eq!(rows[1].attempts, 1);
        assert!(rows[1].reservation_id.is_none());
        assert!(rows[1].reserved_at.is_none());
        assert_eq!(rows[1].meta, bad_meta);
        let info = rows[1].extra_info.as_ref().unwrap();
        assert!(
            info["errors"]
                .as_array()
                .unwrap()
                .last()
                .unwrap()
                .as_str()
                .unwrap()
                .contains("stage reservation")
        );
        if let Some(original) = bad_extra {
            assert_eq!(info["quarantined_extra_info"], original);
        } else {
            assert_eq!(info["other"], 42);
            assert_eq!(info["errors"][0], "old");
        }
        for entry in entries {
            let (id, reservation) = identity(&entry);
            repository.completed(id, reservation).unwrap();
        }
        assert!(repository.reserve(&spec).unwrap().is_empty());
        assert_eq!(
            fixture.rows().iter().map(|r| r.status).collect::<Vec<_>>(),
            vec![5, 4, 5]
        );
        assert!(
            fixture
                .rows()
                .iter()
                .all(|r| r.attempts == 1 && r.reservation_id.is_none())
        );
    }
}

#[test]
#[ignore = "requires DATABASE_URL_TEST"]
fn candidate_limit_and_failed_retry_exhaustion_are_preserved() {
    let f = Fixture::new();
    f.seed(1, json!([]), None);
    f.seed(2, metadata(2), None);
    let repo = f.repository();
    let spec = ReservableCommandSpec::new(1).with_max_attempts(2);
    assert!(repo.reserve(&spec).unwrap().is_empty());
    let rows = f.rows();
    assert_eq!(rows[0].status, 4);
    assert_eq!(rows[1].attempts, 0);
    assert!(rows[0].scheduled_at > rows[0].updated_at);
    let healthy = repo.reserve(&spec).unwrap();
    assert_eq!(healthy.len(), 1);
    let (id, reservation) = identity(&healthy[0]);
    repo.failed(id, reservation, 1, Some("exhausted but inspectable".into()))
        .unwrap();
    assert_eq!(f.rows()[1].status, 4);
    f.sql(&format!(
        "UPDATE {TABLE} SET scheduled_at=to_timestamp(1) WHERE id='{}'",
        Uuid::from_u128(1)
    ));
    assert!(repo.reserve(&spec).unwrap().is_empty());
    assert_eq!(f.rows()[0].attempts, 2);
    assert_eq!(f.rows()[0].status, 4);
    f.sql(&format!("UPDATE {TABLE} SET scheduled_at=to_timestamp(1)"));
    assert!(
        repo.reserve(&ReservableCommandSpec::new(2).with_max_attempts(1))
            .unwrap()
            .is_empty()
    );
    assert!(
        f.rows()
            .iter()
            .all(|r| r.status == 4 && r.reservation_id.is_none())
    );
}

#[test]
#[ignore = "requires DATABASE_URL_TEST"]
fn reservation_update_failure_update_and_commit_errors_roll_back() {
    for (status, deferred) in [(2, false), (4, false), (2, true)] {
        let f = Fixture::new();
        f.seed(1, metadata(1), None);
        f.seed(2, json!([]), None);
        let before = f.rows();
        f.sql(&format!("CREATE FUNCTION reject_transition() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            IF NEW.status={status} THEN RAISE EXCEPTION 'forced transition failure'; END IF; RETURN NEW; END $$;"));
        if deferred {
            f.sql(&format!(
                "CREATE CONSTRAINT TRIGGER reject_transition AFTER UPDATE ON {TABLE}
                DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_transition()"
            ));
        } else {
            f.sql(&format!(
                "CREATE TRIGGER reject_transition BEFORE UPDATE ON {TABLE}
                FOR EACH ROW EXECUTE FUNCTION reject_transition()"
            ));
        }
        assert!(
            f.repository()
                .reserve(&ReservableCommandSpec::new(2))
                .is_err()
        );
        assert_eq!(f.rows(), before, "status={status}, deferred={deferred}");
    }
}

#[test]
#[ignore = "requires DATABASE_URL_TEST"]
fn concurrent_batches_own_distinct_rows_including_malformed_candidates() {
    let f = Fixture::new();
    for n in 1..=12 {
        f.seed(
            n,
            if n == 3 || n == 9 {
                json!([])
            } else {
                metadata(n)
            },
            None,
        );
    }
    let barrier = Arc::new(Barrier::new(2));
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let repo = f.repository();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                repo.reserve(&ReservableCommandSpec::new(6))
                    .unwrap()
                    .iter()
                    .map(identity)
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let reservations: Vec<_> = threads
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    let ids: std::collections::HashSet<_> = reservations.iter().map(|(id, _)| *id).collect();
    assert_eq!(reservations.len(), 10);
    assert_eq!(ids.len(), 10);
    for (id, reservation) in reservations {
        f.repository().completed(id, reservation).unwrap();
    }
    let rows = f.rows();
    assert!(
        rows.iter()
            .all(|r| r.attempts == 1 && r.reservation_id.is_none())
    );
    assert_eq!(rows.iter().filter(|r| r.status == 4).count(), 2);
}

#[test]
#[ignore = "requires DATABASE_URL_TEST"]
fn raw_row_loading_errors_roll_back_reservations_and_attempts() {
    let f = Fixture::new();
    f.seed(1, metadata(1), None);
    f.seed(2, metadata(2), None);
    // Simulate an incompatible schema to force a Diesel load error after UPDATE.
    f.sql(&format!(
        "ALTER TABLE {TABLE} ALTER COLUMN payload DROP NOT NULL;
        UPDATE {TABLE} SET payload=NULL WHERE id='{}'",
        Uuid::from_u128(2)
    ));
    let before = f.rows();
    assert!(
        f.repository()
            .reserve(&ReservableCommandSpec::new(2))
            .is_err()
    );
    assert_eq!(f.rows(), before);
}

#[test]
#[ignore = "requires DATABASE_URL_TEST"]
fn failure_updates_are_atomic_and_never_overwrite_another_owner_or_completion() {
    let f = Fixture::new();
    f.seed(1, metadata(1), Some(json!({"errors":["old"],"other":42})));
    let repo = f.repository();
    let entries = repo.reserve(&ReservableCommandSpec::new(1)).unwrap();
    let (id, reservation) = identity(&entries[0]);
    let before = f.rows();
    assert!(
        repo.failed(id, Uuid::now_v7(), 1, Some("stale owner".into()))
            .is_err()
    );
    assert_eq!(f.rows(), before);
    f.sql(&format!(
        "CREATE FUNCTION reject_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        IF NEW.status=4 THEN RAISE EXCEPTION 'forced failure'; END IF; RETURN NEW; END $$;
        CREATE TRIGGER reject_failure BEFORE UPDATE ON {TABLE}
        FOR EACH ROW EXECUTE FUNCTION reject_failure()"
    ));
    assert!(
        repo.failed(id, reservation, 1, Some("not committed".into()))
            .is_err()
    );
    assert_eq!(f.rows(), before);
    f.sql(&format!("DROP TRIGGER reject_failure ON {TABLE}"));
    repo.completed(id, reservation).unwrap();
    let completed = f.rows();
    assert!(
        repo.failed(id, reservation, 1, Some("late failure".into()))
            .is_err()
    );
    assert_eq!(f.rows(), completed);
}
