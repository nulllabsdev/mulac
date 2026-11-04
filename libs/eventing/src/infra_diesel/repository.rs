use mulac_diesel::DbPool;

pub struct DieselPgEventRepository {
    pub(crate) pool: DbPool,
}

impl DieselPgEventRepository {
    pub(crate) const RETRY_BACKOFF_SECONDS: i64 = 30;
    pub(crate) const MAX_RETRY_BACKOFF_SECONDS: i64 = 120;

    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}

use super::entity::NewEventEntry;
use super::schema::event_entries;

impl EventStorePort for DieselPgEventRepository {
    fn record(&self, envelope: &NewEventEnvelope) -> Result<(), EventError> {
        let entry = NewEventEntry::try_from(envelope)?;
        let mut conn = self
            .pool
            .get()
            .map_err(|e| EventError::Storage(e.to_string()))?;

        diesel::insert_into(event_entries::table)
            .values(&entry)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .map_err(|e| EventError::Storage(e.to_string()))?;

        Ok(())
    }
}

use crate::application::prelude::EventReservePort;
use crate::application::prelude::{
    EventEnvelope,
    EventError,
    EventProcessPort,
    //
    EventStorePort,
    NewEventEnvelope,
};
use crate::execution::prelude::ReservableEventSpec;

use crate::application::prelude::Criterion;
use crate::domain::prelude::EventStatus;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::sql_types::{Array, BigInt, Int4, Uuid as SqlUuid};
use uuid::Uuid;

impl EventReservePort for DieselPgEventRepository {
    fn reserve(&self, spec: &ReservableEventSpec) -> Result<Vec<EventEnvelope>, EventError> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| EventError::Reservation(e.to_string()))?;

        let criteria = ReservationCriteria::from(spec);
        let reservation_id = Uuid::now_v7();

        conn.transaction::<_, diesel::result::Error, _>(|conn| {
            let entries = diesel::sql_query(
                r#"
                WITH candidates AS (
                    SELECT id
                    FROM event_entries
                    WHERE status = ANY($1)
                      AND scheduled_at <= now()
                      AND attempts < $2
                    ORDER BY scheduled_at ASC
                    LIMIT $3
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE event_entries AS entries
                SET status = $4,
                    reservation_id = $5,
                    reserved_at = now(),
                    attempts = entries.attempts + 1,
                    updated_at = now()
                FROM candidates
                WHERE entries.id = candidates.id
                RETURNING
                    entries.id,
                    entries.event_type,
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
                "#,
            )
            .bind::<Array<Int4>, _>(criteria.statuses)
            .bind::<Int4, _>(criteria.max_attempts)
            .bind::<BigInt, _>(spec.limit as i64)
            .bind::<Int4, _>(i32::from(EventStatus::Reserved))
            .bind::<SqlUuid, _>(reservation_id)
            .load::<crate::infra_diesel::reservation::prelude::RawReservedEntry>(conn)?;
            crate::infra_diesel::reservation::prelude::finish_reservation(
                conn,
                entries,
                reservation_id,
            )
        })
        .map_err(|e| EventError::Reservation(e.to_string()))
    }
}

impl EventProcessPort for DieselPgEventRepository {
    fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), EventError> {
        use crate::infra_diesel::schema::event_entries;

        let mut conn = self
            .pool
            .get()
            .map_err(|e| EventError::Storage(e.to_string()))?;

        let updated = diesel::update(
            event_entries::table
                .filter(event_entries::id.eq(id))
                .filter(event_entries::reservation_id.eq(reservation_id))
                .filter(event_entries::status.eq(i32::from(EventStatus::Reserved))),
        )
        .set((
            event_entries::status.eq(i32::from(EventStatus::Completed)),
            event_entries::processed_at.eq(diesel::dsl::now),
            event_entries::updated_at.eq(diesel::dsl::now),
            event_entries::reservation_id.eq(None::<Uuid>),
            event_entries::reserved_at.eq(None::<DateTime<Utc>>),
        ))
        .execute(&mut conn)
        .map_err(|e| EventError::Storage(e.to_string()))?;

        if updated == 0 {
            return Err(EventError::MissingReservation { id });
        }

        Ok(())
    }

    fn failed(
        &self,
        id: Uuid,
        reservation_id: Uuid,
        _max_attempts: i32,
        reason: Option<String>,
    ) -> Result<(), EventError> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| EventError::Storage(e.to_string()))?;
        conn.transaction::<(), diesel::result::Error, _>(|conn| {
            crate::infra_diesel::reservation::prelude::record_failure(
                conn,
                id,
                reservation_id,
                reason.as_deref(),
            )
        })
        .map_err(|e| match e {
            diesel::result::Error::NotFound => EventError::MissingReservation { id },
            e => EventError::Storage(e.to_string()),
        })
    }
}

struct ReservationCriteria {
    statuses: Vec<i32>,
    max_attempts: i32,
}

impl From<&ReservableEventSpec> for ReservationCriteria {
    fn from(spec: &ReservableEventSpec) -> Self {
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
                Criterion::ScheduledBeforeNow | Criterion::OrderByScheduledAtAsc => {}
            }
        }

        criteria
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::EventSweepPort;
    #[test]
    fn repository_implements_all_storage_ports() {
        fn assert_ports<
            T: EventStorePort + EventReservePort + EventProcessPort + EventSweepPort,
        >() {
        }
        assert_ports::<DieselPgEventRepository>();
    }
}

impl crate::application::prelude::EventSweepPort for DieselPgEventRepository {
    fn sweep(
        &self,
        spec: &crate::application::prelude::StaleEventSpec,
    ) -> Result<u64, crate::application::prelude::EventError> {
        use crate::application::prelude::EventError;
        use crate::domain::prelude::{EventStatus, ReservationTimeoutPolicy};
        use crate::infra_diesel::schema::event_entries;
        use chrono::{DateTime, Utc};
        use diesel::prelude::*;
        use diesel::sql_types::{Array, Int4, Text, Timestamptz, Uuid as SqlUuid};
        use uuid::Uuid;
        let mut conn = self
            .pool
            .get()
            .map_err(|e| EventError::Storage(e.to_string()))?;
        conn.transaction::<u64, diesel::result::Error, _>(|conn| {
 let now=diesel::select(diesel::dsl::sql::<Timestamptz>("transaction_timestamp()")).get_result::<DateTime<Utc>>(conn)?;
 let policy=ReservationTimeoutPolicy::new(now-spec.timeout,spec.max_attempts);
 let candidates=event_entries::table.filter(event_entries::status.eq(i32::from(EventStatus::Reserved)))
 .filter(event_entries::reserved_at.lt(policy.reserved_before)).order(event_entries::id.asc())
 .select((event_entries::id,event_entries::attempts,event_entries::scheduled_at)).for_update().load::<(Uuid,i32,DateTime<Utc>)>(conn)?;
 if candidates.is_empty() {return Ok(0);}
 let mut ids=Vec::with_capacity(candidates.len()); let mut statuses=Vec::with_capacity(candidates.len()); let mut schedules=Vec::with_capacity(candidates.len());
 for (id,attempts,previous) in candidates { let (status,scheduled)=policy.outcome(attempts,now,previous);ids.push(id);statuses.push(i32::from(status));schedules.push(scheduled); }
 let sql=r#"UPDATE event_entries AS entries SET status=outcomes.status, reservation_id=NULL, reserved_at=NULL,
 scheduled_at=outcomes.scheduled_at, updated_at=$4,
 extra_info=jsonb_set(COALESCE(entries.extra_info,'{}'::jsonb),'{errors}',COALESCE(entries.extra_info->'errors','[]'::jsonb)||jsonb_build_array($5::text),true)
 FROM unnest($1::uuid[],$2::int4[],$3::timestamptz[]) AS outcomes(id,status,scheduled_at) WHERE entries.id=outcomes.id"#;
 let affected=diesel::sql_query(sql).bind::<Array<SqlUuid>,_>(ids).bind::<Array<Int4>,_>(statuses).bind::<Array<Timestamptz>,_>(schedules).bind::<Timestamptz,_>(now).bind::<Text,_>("event reservation timed out").execute(conn)?;
 Ok(affected as u64)
 }).map_err(|e| EventError::Storage(e.to_string()))
    }
}
