#![cfg(feature = "diesel")]
use chrono::{DateTime, Duration, Utc};
use diesel::Connection;
use diesel::{
    connection::SimpleConnection,
    prelude::*,
    r2d2::{ConnectionManager, CustomizeConnection, Pool},
};
use mulac_inbox::prelude::*;
use std::{
    sync::Arc,
    thread,
    time::{Duration as StdDuration, Instant},
};
use uuid::Uuid;
type PoolType = Pool<ConnectionManager<PgConnection>>;
const TABLE: &str = "inbox_entries";
fn url() -> String {
    std::env::var("DATABASE_URL_TEST").expect("set DATABASE_URL_TEST to a disposable test database")
}
fn ddl(temporary: bool) -> String {
    format!(
        "CREATE {} TABLE {} (id uuid PRIMARY KEY,status int4 NOT NULL,payload text NOT NULL DEFAULT '',meta jsonb,scheduled_at timestamptz NOT NULL,attempts int4 NOT NULL,reservation_id uuid,reserved_at timestamptz,received_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL,processed_at timestamptz,extra_info jsonb,last_error text)",
        if temporary { "TEMP" } else { "" },
        TABLE
    )
}
fn seed(conn: &mut PgConnection) {
    conn.batch_execute(&format!("INSERT INTO {TABLE} (id,status,scheduled_at,attempts,reservation_id,reserved_at,updated_at,extra_info) SELECT md5(n::text)::uuid,CASE WHEN n=21 THEN 5 ELSE 2 END,now()-interval '1 hour',n,md5(n::text)::uuid,CASE WHEN n=22 THEN NULL WHEN n=23 THEN now()-interval '5 minutes' WHEN n=20 THEN now() ELSE now()-interval '1 hour' END,now()-interval '1 hour',CASE WHEN n=1 THEN '{{\"errors\":[\"existing\"],\"other\":1}}'::jsonb ELSE NULL END FROM unnest(ARRAY[1,3,5,6,10,20,21,22,23]) n")).unwrap();
}
#[derive(QueryableByName, Debug, PartialEq)]
struct Row {
    #[diesel(sql_type=diesel::sql_types::Int4)]
    attempts: i32,
    #[diesel(sql_type=diesel::sql_types::Int4)]
    status: i32,
    #[diesel(sql_type=diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
    reservation_id: Option<Uuid>,
    #[diesel(sql_type=diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    reserved_at: Option<DateTime<Utc>>,
    #[diesel(sql_type=diesel::sql_types::Timestamptz)]
    scheduled_at: DateTime<Utc>,
    #[diesel(sql_type=diesel::sql_types::Timestamptz)]
    updated_at: DateTime<Utc>,
    #[diesel(sql_type=diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    extra_info: Option<serde_json::Value>,
}
fn rows(conn: &mut PgConnection) -> Vec<Row> {
    diesel::sql_query(format!("SELECT attempts,status,reservation_id,reserved_at,scheduled_at,updated_at,extra_info FROM {TABLE} ORDER BY attempts")).load(conn).unwrap()
}
#[test]
#[ignore = "requires DATABASE_URL_TEST"]
fn timeout_sweep_policy_and_rollback() {
    for limit in [6, 3] {
        let pool = Pool::builder()
            .max_size(1)
            .build(ConnectionManager::<PgConnection>::new(url()))
            .unwrap();
        {
            let mut c = pool.get().unwrap();
            c.begin_test_transaction().unwrap();
            c.batch_execute(&ddl(true)).unwrap();
            seed(&mut c);
        }
        let database_now = diesel::select(diesel::dsl::sql::<diesel::sql_types::Timestamptz>(
            "transaction_timestamp()",
        ))
        .get_result::<DateTime<Utc>>(&mut pool.get().unwrap())
        .unwrap();
        let before = rows(&mut pool.get().unwrap());
        let repository = DieselPgInboxRepository::new(pool.clone());
        let spec = StaleReservationSpec::new(Duration::minutes(5)).with_max_attempts(limit);
        assert_eq!(repository.sweep(&spec).unwrap(), 5);
        let after = rows(&mut pool.get().unwrap());
        for (old, new) in before.iter().zip(&after) {
            assert_eq!(new.attempts, old.attempts);
            if old.attempts <= 10 {
                assert_eq!(new.status, 4);
                assert!(new.reservation_id.is_none());
                assert!(new.reserved_at.is_none());
                let expected = new.updated_at + Duration::seconds(i64::from(old.attempts) * 30);
                assert_eq!(new.scheduled_at, expected);
                assert_eq!(new.updated_at, database_now);
                let info = new.extra_info.as_ref().unwrap();
                let errors = info["errors"].as_array().unwrap();
                assert_eq!(errors.last().unwrap(), "inbox reservation timed out");
                assert_eq!(errors.len(), if old.attempts == 1 { 2 } else { 1 });
                if old.attempts == 1 {
                    assert_eq!(info["other"], 1);
                    assert_eq!(errors[0], "existing");
                }
            } else {
                assert_eq!(new, old);
            }
        }
        assert_eq!(repository.sweep(&spec).unwrap(), 0);
        assert_eq!(rows(&mut pool.get().unwrap()), after);
        // Restore the fixture and reject the set-based update. No selected row may change.
        {
            let mut c = pool.get().unwrap();
            c.batch_execute(&format!("TRUNCATE {TABLE}")).unwrap();
            seed(&mut c);
            c.batch_execute(&format!("CREATE FUNCTION pg_temp.reject_timeout() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced timeout failure'; END $$; CREATE TRIGGER reject_timeout BEFORE UPDATE ON {TABLE} FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_timeout()")).unwrap();
        }
        let before = rows(&mut pool.get().unwrap());
        assert!(repository.sweep(&spec).is_err());
        assert_eq!(rows(&mut pool.get().unwrap()), before);
    }
}
#[derive(Debug)]
struct Isolate {
    schema: String,
}
impl CustomizeConnection<PgConnection, diesel::r2d2::Error> for Isolate {
    fn on_acquire(&self, c: &mut PgConnection) -> Result<(), diesel::r2d2::Error> {
        c.batch_execute(&format!("SET search_path TO {}; SET application_name TO '{}'; SET statement_timeout TO '8s'; SET lock_timeout TO '7s'",self.schema,self.schema)).map_err(diesel::r2d2::Error::QueryError)
    }
}
struct Fixture {
    schema: String,
    pool: PoolType,
}
impl Fixture {
    fn new() -> Self {
        let schema = format!("parity_{}", Uuid::new_v4().simple());
        let mut c = PgConnection::establish(&url()).unwrap();
        c.batch_execute(&format!("CREATE SCHEMA {schema}")).unwrap();
        let pool = Pool::builder()
            .max_size(4)
            .connection_customizer(Box::new(Isolate {
                schema: schema.clone(),
            }))
            .build(ConnectionManager::<PgConnection>::new(url()))
            .unwrap();
        {
            let mut c = pool.get().unwrap();
            c.batch_execute(&ddl(false)).unwrap();
            seed(&mut c);
            c.batch_execute(&format!("DELETE FROM {TABLE} WHERE attempts<>1"))
                .unwrap();
        }
        Self { schema, pool }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Ok(mut c) = PgConnection::establish(&url()) {
            let _ = c.batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema));
        }
    }
}
#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type=diesel::sql_types::BigInt)]
    count: i64,
}
fn wait_for_locks(f: &Fixture, n: i64) {
    let start = Instant::now();
    loop {
        let count=diesel::sql_query("SELECT count(*) AS count FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock'").bind::<diesel::sql_types::Text,_>(&f.schema).get_result::<Count>(&mut f.pool.get().unwrap()).unwrap().count;
        if count >= n {
            return;
        }
        assert!(
            start.elapsed() < StdDuration::from_secs(5),
            "expected {n} blocked database operations, saw {count}"
        );
        thread::sleep(StdDuration::from_millis(10));
    }
}
#[test]
#[ignore = "requires DATABASE_URL_TEST"]
fn concurrent_timeout_sweeps_do_not_double_process() {
    let f = Fixture::new();
    let repo = Arc::new(DieselPgInboxRepository::new(f.pool.clone()));
    let mut holder = f.pool.get().unwrap();
    holder
        .batch_execute(&format!("BEGIN; SELECT id FROM {TABLE} FOR UPDATE"))
        .unwrap();
    let a = repo.clone();
    let t1 = thread::spawn(move || a.sweep(&StaleReservationSpec::new(Duration::minutes(5))));
    let b = repo.clone();
    let t2 = thread::spawn(move || b.sweep(&StaleReservationSpec::new(Duration::minutes(5))));
    wait_for_locks(&f, 2);
    holder.batch_execute("COMMIT").unwrap();
    assert_eq!(t1.join().unwrap().unwrap() + t2.join().unwrap().unwrap(), 1);
    let after = rows(&mut f.pool.get().unwrap());
    assert_eq!(
        after[0].extra_info.as_ref().unwrap()["errors"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}
#[test]
#[ignore = "requires DATABASE_URL_TEST"]
fn completion_and_timeout_obey_reservation_ownership() {
    for sweep_first in [false, true] {
        let f = Fixture::new();
        let repo = Arc::new(DieselPgInboxRepository::new(f.pool.clone()));
        let row = rows(&mut f.pool.get().unwrap()).remove(0);
        let reservation = row.reservation_id.unwrap();
        let id = Uuid::parse_str("c4ca4238-a0b9-2382-0dcc-509a6f75849b").unwrap();
        // The first actual operation holds the row while its trigger waits on our gate.
        let key = (Uuid::new_v4().as_u128() & i64::MAX as u128) as i64;
        let mut gate = f.pool.get().unwrap();
        gate.batch_execute(&format!("SELECT pg_advisory_lock({key}); CREATE FUNCTION gate_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({key}); RETURN NEW; END $$; CREATE TRIGGER gate_update BEFORE UPDATE ON {TABLE} FOR EACH ROW EXECUTE FUNCTION gate_update()")).unwrap();
        let a = repo.clone();
        let first = thread::spawn(move || {
            if sweep_first {
                a.sweep(&StaleReservationSpec::new(Duration::minutes(5)))
                    .map(|n| n as usize)
            } else {
                a.completed(id, reservation).map(|_| 0)
            }
        });
        wait_for_locks(&f, 1);
        let b = repo.clone();
        let second = thread::spawn(move || {
            if sweep_first {
                b.completed(id, reservation).map(|_| 0)
            } else {
                b.sweep(&StaleReservationSpec::new(Duration::minutes(5)))
                    .map(|n| n as usize)
            }
        });
        wait_for_locks(&f, 2);
        gate.batch_execute(&format!("SELECT pg_advisory_unlock({key})"))
            .unwrap();
        let first = first.join().unwrap();
        let second = second.join().unwrap();
        if sweep_first {
            assert_eq!(first.unwrap(), 1);
            let error = second.unwrap_err();
            assert!(matches!(error, InboxError::ReservationNotOwned { .. }));
        } else {
            first.unwrap();
            assert_eq!(second.unwrap(), 0);
        }
        assert_eq!(
            rows(&mut f.pool.get().unwrap())[0].status,
            if sweep_first { 4 } else { 5 }
        );
    }
}

#[test]
#[ignore = "requires DATABASE_URL_TEST"]
fn inbox_duplicate_identity_preserves_the_original_record() {
    let pool = Pool::builder()
        .max_size(1)
        .build(ConnectionManager::<PgConnection>::new(url()))
        .unwrap();
    {
        let mut conn = pool.get().unwrap();
        conn.begin_test_transaction().unwrap();
        conn.batch_execute(&ddl(true)).unwrap();
    }
    let repo = DieselPgInboxRepository::new(pool.clone());
    let id = Uuid::now_v7();
    for payload in ["original", "replacement"] {
        repo.store(NewInboxMessageEnvelope::from(InboundMessageEnvelope {
            payload: payload.into(),
            message_id: Some(id),
            routing_key: Some("Example".into()),
            ..Default::default()
        }))
        .unwrap();
    }
    #[derive(QueryableByName)]
    struct Stored {
        #[diesel(sql_type=diesel::sql_types::Text)]
        payload: String,
    }
    let stored = diesel::sql_query("SELECT payload FROM inbox_entries")
        .load::<Stored>(&mut pool.get().unwrap())
        .unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].payload, "original");
}
