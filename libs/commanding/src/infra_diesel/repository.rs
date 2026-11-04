use super::entity::NewCommandEntry;
use super::schema::command_entries;
use crate::application::prelude::Criterion;
use crate::application::prelude::StaleCommandSpec;
use crate::application::prelude::{CommandEnvelope, CommandError, NewCommandEnvelope};
use crate::application::prelude::{
    CommandProcessPort, CommandReservePort, CommandStorePort, CommandSweepPort,
};
use crate::domain::prelude::{CommandStatus, ReservationTimeoutPolicy};
use crate::execution::prelude::ReservableCommandSpec;
#[cfg(test)]
use chrono::Duration;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::sql_types::{Array, BigInt, Int4, Timestamptz, Uuid as SqlUuid};
use mulac_diesel::DbPool;
use uuid::Uuid;

pub struct DieselPgCommandRepository {
    pub(crate) pool: DbPool,
}

impl DieselPgCommandRepository {
    pub(crate) const RETRY_BACKOFF_SECONDS: i64 = 30;
    pub(crate) const MAX_RETRY_BACKOFF_SECONDS: i64 = 120;

    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}

impl CommandStorePort for DieselPgCommandRepository {
    fn record(&self, envelope: &NewCommandEnvelope) -> Result<(), CommandError> {
        let entry = NewCommandEntry::try_from(envelope)?;
        let mut conn = self
            .pool
            .get()
            .map_err(|e| CommandError::Storage(e.to_string()))?;

        diesel::insert_into(command_entries::table)
            .values(&entry)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .map_err(|e| CommandError::Storage(e.to_string()))?;

        Ok(())
    }
}

impl CommandReservePort for DieselPgCommandRepository {
    fn reserve(&self, spec: &ReservableCommandSpec) -> Result<Vec<CommandEnvelope>, CommandError> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| CommandError::Reservation(e.to_string()))?;

        let criteria = ReservationCriteria::from(spec);
        let reservation_id = Uuid::now_v7();

        let sql_query = r#"
            WITH candidates AS (
                SELECT id
                FROM command_entries
                WHERE status = ANY($1)
                  AND scheduled_at <= now()
                  AND attempts < $2
                ORDER BY scheduled_at ASC
                LIMIT $3
                FOR UPDATE SKIP LOCKED
            )
            UPDATE command_entries AS entries
            SET status = $4,
                reservation_id = $5,
                reserved_at = now(),
                attempts = entries.attempts + 1,
                updated_at = now()
            FROM candidates
            WHERE entries.id = candidates.id
            RETURNING
                entries.id,
                entries.command_type,
                entries.status,
                entries.payload,
                entries.meta::text AS meta,
                entries.scheduled_at,
                entries.attempts,
                entries.reservation_id,
                entries.reserved_at,
                entries.received_at,
                entries.updated_at,
                entries.processed_at,
                entries.extra_info::text AS extra_info
            "#;

        conn.transaction::<_, diesel::result::Error, _>(|conn| {
            let entries = diesel::sql_query(sql_query)
                .bind::<Array<Int4>, _>(criteria.statuses)
                .bind::<Int4, _>(criteria.max_attempts)
                .bind::<BigInt, _>(spec.limit as i64)
                .bind::<Int4, _>(i32::from(CommandStatus::Reserved))
                .bind::<SqlUuid, _>(reservation_id)
                .load::<crate::infra_diesel::reservation::prelude::RawReservedEntry>(conn)?;
            crate::infra_diesel::reservation::prelude::finish_reservation(
                conn,
                entries,
                reservation_id,
            )
        })
        .map_err(|e| CommandError::Reservation(e.to_string()))
    }
}

impl CommandProcessPort for DieselPgCommandRepository {
    fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), CommandError> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| CommandError::Storage(e.to_string()))?;

        let updated = diesel::update(
            command_entries::table
                .filter(command_entries::id.eq(id))
                .filter(command_entries::reservation_id.eq(reservation_id))
                .filter(command_entries::status.eq(i32::from(CommandStatus::Reserved))),
        )
        .set((
            command_entries::status.eq(i32::from(CommandStatus::Completed)),
            command_entries::processed_at.eq(diesel::dsl::now),
            command_entries::updated_at.eq(diesel::dsl::now),
            command_entries::reservation_id.eq(None::<Uuid>),
            command_entries::reserved_at.eq(None::<DateTime<Utc>>),
        ))
        .execute(&mut conn)
        .map_err(|e| CommandError::Storage(e.to_string()))?;

        if updated == 0 {
            return Err(CommandError::MissingReservation { id });
        }

        Ok(())
    }

    fn failed(
        &self,
        id: Uuid,
        reservation_id: Uuid,
        _max_attempts: i32,
        reason: Option<String>,
    ) -> Result<(), CommandError> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| CommandError::Storage(e.to_string()))?;
        conn.transaction::<(), diesel::result::Error, _>(|conn| {
            crate::infra_diesel::reservation::prelude::record_failure(
                conn,
                id,
                reservation_id,
                reason.as_deref(),
            )
        })
        .map_err(|e| match e {
            diesel::result::Error::NotFound => CommandError::MissingReservation { id },
            e => CommandError::Storage(e.to_string()),
        })
    }
}

struct ReservationCriteria {
    statuses: Vec<i32>,
    max_attempts: i32,
}

impl From<&ReservableCommandSpec> for ReservationCriteria {
    fn from(spec: &ReservableCommandSpec) -> Self {
        let mut criteria = Self {
            statuses: vec![],
            max_attempts: spec.max_attempts,
        };

        for criterion in spec.criteria() {
            match criterion {
                Criterion::StatusIn(statuses) => {
                    criteria.statuses = statuses.into_iter().map(i32::from).collect();
                }
                Criterion::MaxAttempts(max) => {
                    criteria.max_attempts = max;
                }
                Criterion::ScheduledBeforeNow
                | Criterion::OrderByScheduledAtAsc
                | Criterion::ReservedBefore(_) => {}
            }
        }

        criteria
    }
}

impl CommandSweepPort for DieselPgCommandRepository {
    fn sweep(&self, spec: &StaleCommandSpec) -> Result<u64, CommandError> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| CommandError::Storage(e.to_string()))?;

        // Lock the selected rows until their timeout outcomes have been persisted.
        // This prevents a worker from completing or replacing a reservation between
        // reading its attempt count and applying the bulk update.
        conn.transaction::<u64, diesel::result::Error, _>(|conn| {
            // Reservations are timestamped by PostgreSQL, so never compare them
            // against the application server's clock. Reuse this instant throughout.
            let now = diesel::select(diesel::dsl::sql::<Timestamptz>("transaction_timestamp()"))
                .get_result::<DateTime<Utc>>(conn)?;
            let policy = ReservationTimeoutPolicy::new(
                SweepCriteria::from_spec(spec, now).cutoff,
                spec.max_attempts,
            );
            let candidates = command_entries::table
                .filter(command_entries::status.eq(i32::from(policy.starting_status)))
                .filter(command_entries::reserved_at.lt(policy.reserved_before))
                .order(command_entries::id.asc())
                .select((command_entries::id, command_entries::attempts))
                .for_update()
                .load::<(Uuid, i32)>(conn)?;

            if candidates.is_empty() {
                return Ok(0);
            }

            let mut ids = Vec::with_capacity(candidates.len());
            let mut statuses = Vec::with_capacity(candidates.len());
            let mut scheduled_times = Vec::with_capacity(candidates.len());
            for (id, attempts) in candidates {
                ids.push(id);
                statuses.push(i32::from(policy.status_after_timeout(attempts)));
                scheduled_times.push(now + policy.retry_delay(attempts));
            }

            let sql_query = r#"
                UPDATE command_entries AS entries
                SET
                    status         = outcomes.status,
                    reservation_id = NULL,
                    reserved_at    = NULL,
                    scheduled_at   = outcomes.scheduled_at,
                    updated_at     = $4,
                    extra_info     = jsonb_set(
                        COALESCE(entries.extra_info, '{}'::jsonb),
                        '{errors}',
                        COALESCE(entries.extra_info->'errors', '[]'::jsonb)
                            || jsonb_build_array($5::text),
                        true
                    )
                FROM unnest($1::uuid[], $2::int4[], $3::timestamptz[])
                    AS outcomes(id, status, scheduled_at)
                WHERE entries.id = outcomes.id
                "#;

            let affected = diesel::sql_query(sql_query)
                .bind::<Array<SqlUuid>, _>(ids)
                .bind::<Array<Int4>, _>(statuses)
                .bind::<Array<Timestamptz>, _>(scheduled_times)
                .bind::<Timestamptz, _>(now)
                .bind::<diesel::sql_types::Text, _>("command reservation timed out")
                .execute(conn)?;
            Ok(affected as u64)
        })
        .map_err(|e| CommandError::Storage(e.to_string()))
    }
}

struct SweepCriteria {
    cutoff: DateTime<Utc>,
}

impl SweepCriteria {
    fn from_spec(spec: &StaleCommandSpec, now: DateTime<Utc>) -> Self {
        let mut criteria = Self {
            cutoff: DateTime::<Utc>::MIN_UTC,
        };

        for criterion in spec.criteria(now) {
            match criterion {
                Criterion::ReservedBefore(cutoff) => {
                    criteria.cutoff = cutoff;
                }
                _ => {}
            }
        }

        criteria
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::prelude::ExtraInfo;

    #[test]
    fn repository_implements_all_command_storage_ports() {
        fn assert_ports<
            T: CommandStorePort + CommandReservePort + CommandProcessPort + CommandSweepPort,
        >() {
        }
        assert_ports::<DieselPgCommandRepository>();
    }

    #[test]
    #[ignore = "requires DATABASE_URL_TEST pointing to PostgreSQL"]
    fn bulk_sweep_applies_policy_and_preserves_unselected_rows() {
        use diesel::connection::SimpleConnection;
        use diesel::r2d2::{ConnectionManager, Pool};

        for max_attempts in [6, 3] {
            let url = std::env::var("DATABASE_URL_TEST").expect("set DATABASE_URL_TEST");
            // A single connection keeps the fixture in a session-local temporary table.
            // No persistent application tables are modified.
            let pool = Pool::builder()
                .max_size(1)
                .build(ConnectionManager::<PgConnection>::new(url))
                .unwrap();
            let database_now = {
                let mut conn = pool.get().unwrap();
                conn.begin_test_transaction().unwrap();
                conn.batch_execute(
                    r#"
                CREATE TEMP TABLE command_entries (
                    id uuid PRIMARY KEY,
                    attempts int4 NOT NULL,
                    status int4 NOT NULL,
                    reservation_id uuid,
                    reserved_at timestamptz,
                    scheduled_at timestamptz NOT NULL,
                    updated_at timestamptz NOT NULL,
                    extra_info jsonb
                );
                INSERT INTO command_entries
                SELECT md5(n::text)::uuid, n,
                    CASE WHEN n = 21 THEN 5 ELSE 2 END,
                    md5(n::text)::uuid,
                    CASE WHEN n = 22 THEN NULL
                         WHEN n = 23 THEN now() - interval '5 minutes'
                         WHEN n = 20 THEN now()
                         ELSE now() - interval '1 hour' END,
                    now() - interval '1 hour', now() - interval '1 hour',
                    CASE WHEN n = 1 THEN '{"errors":["existing"]}'::jsonb ELSE NULL END
                FROM unnest(ARRAY[1,3,5,6,10,20,21,22,23]) AS n;
            "#,
                )
                .unwrap();
                diesel::select(diesel::dsl::sql::<Timestamptz>("transaction_timestamp()"))
                    .get_result::<DateTime<Utc>>(&mut conn)
                    .unwrap()
            };
            let repo = DieselPgCommandRepository::new(pool.clone());
            let spec = StaleCommandSpec::new(Duration::minutes(5)).with_max_attempts(max_attempts);
            assert_eq!(repo.sweep(&spec).unwrap(), 5);
            assert_eq!(repo.sweep(&spec).unwrap(), 0);

            let mut conn = pool.get().unwrap();
            let rows = command_entries::table
                .order(command_entries::attempts.asc())
                .select((
                    command_entries::attempts,
                    command_entries::status,
                    command_entries::reservation_id,
                    command_entries::reserved_at,
                    command_entries::scheduled_at,
                    command_entries::updated_at,
                    command_entries::extra_info,
                ))
                .load::<(
                    i32,
                    i32,
                    Option<Uuid>,
                    Option<DateTime<Utc>>,
                    DateTime<Utc>,
                    DateTime<Utc>,
                    Option<super::super::models::ExtraInfoJsonb>,
                )>(&mut conn)
                .unwrap();
            assert_eq!(rows.len(), 9);
            for (
                attempts,
                status,
                reservation_id,
                reserved_at,
                scheduled_at,
                updated_at,
                extra_info,
            ) in rows
            {
                if attempts <= 10 {
                    let expected_status = CommandStatus::Failed;
                    assert_eq!(status, i32::from(expected_status));
                    assert!(reservation_id.is_none());
                    assert!(reserved_at.is_none());
                    assert_eq!(updated_at, database_now);
                    assert_eq!(
                        scheduled_at - updated_at,
                        Duration::seconds(i64::from(attempts) * 30)
                    );
                    let extra_info: ExtraInfo = extra_info.unwrap().into();
                    let expected = if attempts == 1 {
                        vec!["existing", "command reservation timed out"]
                    } else {
                        vec!["command reservation timed out"]
                    };
                    assert_eq!(extra_info.errors(), expected);
                } else {
                    assert_eq!(status, if attempts == 21 { 5 } else { 2 });
                    assert!(reservation_id.is_some());
                    assert_eq!(reserved_at.is_none(), attempts == 22);
                    assert_eq!(scheduled_at, updated_at);
                    assert!(extra_info.is_none());
                }
            }
        }
    }
}
