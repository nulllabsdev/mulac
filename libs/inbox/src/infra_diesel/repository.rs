use mulac_diesel::DbPool;

pub struct DieselPgInboxRepository {
    pub(crate) pool: DbPool,
}

impl DieselPgInboxRepository {
    pub(crate) const RETRY_BACKOFF_SECONDS: i64 = 30;

    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}

mod record_messages_impl {

    use super::DieselPgInboxRepository;
    use crate::application::prelude::InboxError;
    use crate::application::prelude::{InboxStorePort, NewInboxMessageEnvelope};
    use crate::infra_diesel::entity::NewInboxEntry;
    use crate::infra_diesel::schema::inbox_entries;
    use diesel::prelude::*;

    impl InboxStorePort for DieselPgInboxRepository {
        fn store(&self, msg: NewInboxMessageEnvelope) -> Result<(), InboxError> {
            let mut conn = self
                .pool
                .get()
                .map_err(|e| InboxError::Storage(e.to_string()))?;
            let entry = NewInboxEntry::from(msg);
            diesel::insert_into(inbox_entries::table)
                .values(&entry)
                .on_conflict(inbox_entries::id)
                .do_nothing()
                .execute(&mut conn)
                .map_err(|e| InboxError::Storage(e.to_string()))?;

            Ok(())
        }
    }
}
mod inbox_consumer_impl {

    use super::DieselPgInboxRepository;
    use crate::application::prelude::InboxReservePort;
    use crate::application::prelude::{Criterion, InboxStatus};
    use crate::application::prelude::{InboxError, InboxMessageEnvelope, InboxProcessPort};
    use crate::execution::prelude::ReservableInboxSpec;

    use crate::infra_diesel::schema::inbox_entries;
    use chrono::{DateTime, Utc};
    use diesel::prelude::*;
    use diesel::sql_types::{Array, BigInt, Int4, Uuid as SqlUuid};
    use uuid::Uuid;

    impl InboxReservePort for DieselPgInboxRepository {
        fn reserve(
            &self,
            spec: &ReservableInboxSpec,
        ) -> Result<Vec<InboxMessageEnvelope>, InboxError> {
            let mut conn = self
                .pool
                .get()
                .map_err(|e| InboxError::Storage(e.to_string()))?;

            let criteria = ReservationCriteria::from(spec);
            let reservation_id = Uuid::now_v7();

            conn.transaction::<_, diesel::result::Error, _>(|conn| {
                let entries = diesel::sql_query(
                    r#"
                WITH candidates AS (
                    SELECT id
                    FROM inbox_entries
                    WHERE status = ANY($1)
                      AND scheduled_at <= now()
                      AND attempts < $2
                    ORDER BY scheduled_at ASC
                    LIMIT $3
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE inbox_entries AS entries
                SET status = $4,
                    reservation_id = $5,
                    reserved_at = now(),
                    attempts = entries.attempts + 1,
                    updated_at = now()
                FROM candidates
                WHERE entries.id = candidates.id
                RETURNING
                    entries.id,
                    entries.payload,
                    entries.meta::text AS meta,
                    entries.status,
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
                .bind::<Int4, _>(i32::from(InboxStatus::Reserved))
                .bind::<SqlUuid, _>(reservation_id)
                .load::<crate::infra_diesel::reservation::prelude::RawReservedEntry>(conn)?;
                crate::infra_diesel::reservation::prelude::finish_reservation(
                    conn,
                    entries,
                    reservation_id,
                )
            })
            .map_err(|e| InboxError::Storage(e.to_string()))
        }
    }

    impl InboxProcessPort for DieselPgInboxRepository {
        fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), InboxError> {
            let mut conn = self
                .pool
                .get()
                .map_err(|e| InboxError::Storage(e.to_string()))?;

            let updated = diesel::update(
                inbox_entries::table
                    .filter(inbox_entries::id.eq(id))
                    .filter(inbox_entries::reservation_id.eq(reservation_id))
                    .filter(inbox_entries::status.eq(i32::from(InboxStatus::Reserved))),
            )
            .set((
                inbox_entries::status.eq(i32::from(InboxStatus::Completed)),
                inbox_entries::processed_at.eq(diesel::dsl::now),
                inbox_entries::updated_at.eq(diesel::dsl::now),
                inbox_entries::reservation_id.eq(None::<Uuid>),
                inbox_entries::reserved_at.eq(None::<DateTime<Utc>>),
            ))
            .execute(&mut conn)
            .map_err(|e| InboxError::Storage(e.to_string()))?;

            if updated == 0 {
                return Err(InboxError::ReservationNotOwned { id, reservation_id });
            }

            Ok(())
        }

        fn failed(
            &self,
            id: Uuid,
            reservation_id: Uuid,
            _max_attempts: i32,
            reason: Option<String>,
        ) -> Result<(), InboxError> {
            let mut conn = self
                .pool
                .get()
                .map_err(|e| InboxError::Storage(e.to_string()))?;
            conn.transaction::<(), diesel::result::Error, _>(|conn| {
                crate::infra_diesel::reservation::prelude::record_failure(
                    conn,
                    id,
                    reservation_id,
                    reason.as_deref(),
                )
            })
            .map_err(|e| match e {
                diesel::result::Error::NotFound => {
                    InboxError::ReservationNotOwned { id, reservation_id }
                }
                e => InboxError::Storage(e.to_string()),
            })
        }
    }

    struct ReservationCriteria {
        statuses: Vec<i32>,
        max_attempts: i32,
    }

    impl From<&ReservableInboxSpec> for ReservationCriteria {
        fn from(spec: &ReservableInboxSpec) -> Self {
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
}

impl crate::application::prelude::InboxSweepPort for DieselPgInboxRepository {
    fn sweep(
        &self,
        spec: &crate::application::prelude::StaleReservationSpec,
    ) -> Result<usize, crate::application::prelude::InboxError> {
        use crate::application::prelude::InboxError;
        use crate::domain::prelude::{InboxStatus, ReservationTimeoutPolicy};
        use crate::infra_diesel::schema::inbox_entries;
        use chrono::{DateTime, Utc};
        use diesel::prelude::*;
        use diesel::sql_types::{Array, Int4, Text, Timestamptz, Uuid as SqlUuid};
        use uuid::Uuid;
        let mut conn = self
            .pool
            .get()
            .map_err(|e| InboxError::Storage(e.to_string()))?;
        conn.transaction::<usize, diesel::result::Error, _>(|conn| {
 let now=diesel::select(diesel::dsl::sql::<Timestamptz>("transaction_timestamp()")).get_result::<DateTime<Utc>>(conn)?;
 let policy=ReservationTimeoutPolicy::new(now-spec.timeout,spec.max_attempts);
 let candidates=inbox_entries::table.filter(inbox_entries::status.eq(i32::from(InboxStatus::Reserved)))
 .filter(inbox_entries::reserved_at.lt(policy.reserved_before)).order(inbox_entries::id.asc())
 .select((inbox_entries::id,inbox_entries::attempts,inbox_entries::scheduled_at)).for_update().load::<(Uuid,i32,DateTime<Utc>)>(conn)?;
 if candidates.is_empty() {return Ok(0);}
 let mut ids=Vec::with_capacity(candidates.len()); let mut statuses=Vec::with_capacity(candidates.len()); let mut schedules=Vec::with_capacity(candidates.len());
 for (id,attempts,previous) in candidates { let (status,scheduled)=policy.outcome(attempts,now,previous);ids.push(id);statuses.push(i32::from(status));schedules.push(scheduled); }
 let sql=r#"UPDATE inbox_entries AS entries SET status=outcomes.status, reservation_id=NULL, reserved_at=NULL,
 scheduled_at=outcomes.scheduled_at, updated_at=$4,
 extra_info=jsonb_set(COALESCE(entries.extra_info,'{}'::jsonb),'{errors}',COALESCE(entries.extra_info->'errors','[]'::jsonb)||jsonb_build_array($5::text),true)
 FROM unnest($1::uuid[],$2::int4[],$3::timestamptz[]) AS outcomes(id,status,scheduled_at) WHERE entries.id=outcomes.id"#;
 let affected=diesel::sql_query(sql).bind::<Array<SqlUuid>,_>(ids).bind::<Array<Int4>,_>(statuses).bind::<Array<Timestamptz>,_>(schedules).bind::<Timestamptz,_>(now).bind::<Text,_>("inbox reservation timed out").execute(conn)?;
 Ok(affected as usize)
 }).map_err(|e| InboxError::Storage(e.to_string()))
    }
}

#[cfg(test)]
mod port_contract_tests {
    use crate::prelude::*;
    #[test]
    fn repository_implements_all_storage_ports() {
        fn check<T: InboxStorePort + InboxReservePort + InboxProcessPort + InboxSweepPort>() {}
        check::<DieselPgInboxRepository>();
    }
}
