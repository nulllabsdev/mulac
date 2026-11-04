use super::entity::NewOutboxEntryRecord;
use super::schema::outbox_entries;
use crate::application::prelude::{NewOutboxEntry, OutboxStatus, append_error};
use crate::application::prelude::{
    OutboxEntryEnvelope,
    OutboxError,
    OutboxProcessPort,
    OutboxReservePort,
    OutboxStorePort,
    //
};
use crate::execution::prelude::ReservableOutboxSpec;

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::sql_types::{Array, BigInt, Int4, Uuid as SqlUuid};
use mulac_diesel::DbPool;
use uuid::Uuid;

impl OutboxStorePort for DieselPgOutboxRepository {
    fn record(&self, entry: &NewOutboxEntry) -> Result<(), OutboxError> {
        crate::application::prelude::validate_sequence(entry.meta.sequence)
            .map_err(|message| OutboxError::Routing(message.into()))?;
        let mut conn = self
            .pool
            .get()
            .map_err(|err| OutboxError::Storage(err.to_string()))?;
        let record = NewOutboxEntryRecord::from(entry);

        diesel::insert_into(outbox_entries::table)
            .values(&record)
            .on_conflict(outbox_entries::id)
            .do_nothing()
            .execute(&mut conn)
            .map_err(|err| OutboxError::Storage(err.to_string()))?;

        Ok(())
    }
}

pub struct DieselPgOutboxRepository {
    pub(crate) pool: DbPool,
}

impl DieselPgOutboxRepository {
    pub(crate) const RETRY_BACKOFF_SECONDS: i64 = 30;
    pub(crate) const RETRY_BACKOFF_CAP_SECONDS: i64 = 120;

    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}

impl OutboxReservePort for DieselPgOutboxRepository {
    fn reserve(
        &self,
        spec: &ReservableOutboxSpec,
    ) -> Result<Vec<OutboxEntryEnvelope>, OutboxError> {
        let mut conn = self
            .pool
            .get()
            .map_err(|err| OutboxError::Storage(err.to_string()))?;
        let reservation_id = Uuid::now_v7();

        conn.transaction::<_, diesel::result::Error, _>(|conn| {
            let entries = diesel::sql_query(
                r#"
                WITH candidates AS (
                    SELECT id
                    FROM outbox_entries
                    WHERE status = ANY($1)
                      AND scheduled_at <= now()
                      AND attempts < $2
                    ORDER BY scheduled_at ASC
                    LIMIT $3
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE outbox_entries AS entries
                SET status = $4,
                    reservation_id = $5,
                    reserved_at = now(),
                    attempts = entries.attempts + 1,
                    updated_at = now()
                FROM candidates
                WHERE entries.id = candidates.id
                RETURNING
                    entries.id,
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
                    entries.last_error,
                    entries.extra_info::text AS extra_info
                "#,
            )
            .bind::<Array<Int4>, _>(vec![
                i32::from(OutboxStatus::Received),
                i32::from(OutboxStatus::Failed),
            ])
            .bind::<Int4, _>(spec.max_attempts)
            .bind::<BigInt, _>(spec.limit as i64)
            .bind::<Int4, _>(i32::from(OutboxStatus::Reserved))
            .bind::<SqlUuid, _>(reservation_id)
            .load::<crate::infra_diesel::reservation::prelude::RawReservedEntry>(conn)?;
            crate::infra_diesel::reservation::prelude::finish_reservation(
                conn,
                entries,
                reservation_id,
                spec.max_attempts,
            )
        })
        .map_err(|e| OutboxError::Storage(e.to_string()))
    }
}

impl OutboxProcessPort for DieselPgOutboxRepository {
    fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), OutboxError> {
        let mut conn = self
            .pool
            .get()
            .map_err(|err| OutboxError::Storage(err.to_string()))?;

        let updated = diesel::update(
            outbox_entries::table
                .filter(outbox_entries::id.eq(id))
                .filter(outbox_entries::reservation_id.eq(reservation_id))
                .filter(outbox_entries::status.eq(i32::from(OutboxStatus::Reserved))),
        )
        .set((
            outbox_entries::status.eq(i32::from(OutboxStatus::Completed)),
            outbox_entries::processed_at.eq(diesel::dsl::now),
            outbox_entries::updated_at.eq(diesel::dsl::now),
            outbox_entries::reservation_id.eq(None::<Uuid>),
            outbox_entries::reserved_at.eq(None::<DateTime<Utc>>),
            outbox_entries::last_error.eq(None::<String>),
        ))
        .execute(&mut conn)
        .map_err(|err| OutboxError::Storage(err.to_string()))?;

        if updated == 0 {
            return Err(reservation_not_owned(id, reservation_id));
        }

        Ok(())
    }

    fn failed(
        &self,
        id: Uuid,
        reservation_id: Uuid,
        max_attempts: i32,
        reason: Option<String>,
    ) -> Result<(), OutboxError> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        conn.transaction::<(), diesel::result::Error, _>(|conn| {
            crate::infra_diesel::reservation::prelude::record_failure(
                conn,
                id,
                reservation_id,
                max_attempts,
                reason.as_deref(),
            )
        })
        .map_err(|e| match e {
            diesel::result::Error::NotFound => reservation_not_owned(id, reservation_id),
            e => OutboxError::Storage(e.to_string()),
        })
    }

    fn dead(
        &self,
        id: Uuid,
        reservation_id: Uuid,
        reason: Option<String>,
    ) -> Result<(), OutboxError> {
        let mut conn = self
            .pool
            .get()
            .map_err(|err| OutboxError::Storage(err.to_string()))?;

        conn.transaction::<(), diesel::result::Error, _>(|conn| {
            let extra_info = outbox_entries::table
                .filter(outbox_entries::id.eq(id))
                .filter(outbox_entries::reservation_id.eq(reservation_id))
                .filter(outbox_entries::status.eq(i32::from(OutboxStatus::Reserved)))
                .select(outbox_entries::extra_info)
                .for_update()
                .get_result::<Option<super::models::ExtraInfoJsonb>>(conn)?;

            let extra_info = match reason.clone() {
                Some(reason) => Some(super::models::ExtraInfoJsonb::from(append_error(
                    extra_info.map(Into::into),
                    reason,
                ))),
                None => extra_info,
            };

            let updated = diesel::update(
                outbox_entries::table
                    .filter(outbox_entries::id.eq(id))
                    .filter(outbox_entries::reservation_id.eq(reservation_id))
                    .filter(outbox_entries::status.eq(i32::from(OutboxStatus::Reserved))),
            )
            .set((
                outbox_entries::status.eq(i32::from(OutboxStatus::Dead)),
                outbox_entries::reservation_id.eq(None::<Uuid>),
                outbox_entries::reserved_at.eq(None::<DateTime<Utc>>),
                outbox_entries::updated_at.eq(diesel::dsl::now),
                outbox_entries::last_error.eq(reason.clone()),
                outbox_entries::extra_info.eq(extra_info),
            ))
            .execute(conn)?;

            if updated == 0 {
                return Err(diesel::result::Error::NotFound);
            }

            Ok(())
        })
        .map_err(|err| match err {
            diesel::result::Error::NotFound => reservation_not_owned(id, reservation_id),
            other => OutboxError::Storage(other.to_string()),
        })?;

        Ok(())
    }
}

fn reservation_not_owned(id: Uuid, reservation_id: Uuid) -> OutboxError {
    OutboxError::Reservation(format!(
        "outbox entry {id} is not reserved by reservation {reservation_id}"
    ))
}

impl crate::application::prelude::OutboxSweepPort for DieselPgOutboxRepository {
    fn sweep(
        &self,
        spec: &crate::application::prelude::StaleReservationSpec,
    ) -> Result<u64, crate::application::prelude::OutboxError> {
        use crate::application::prelude::OutboxError;
        use crate::domain::prelude::{OutboxStatus, ReservationTimeoutPolicy};
        use crate::infra_diesel::schema::outbox_entries;
        use chrono::{DateTime, Utc};
        use diesel::prelude::*;
        use diesel::sql_types::{Array, Int4, Text, Timestamptz, Uuid as SqlUuid};
        use uuid::Uuid;
        let mut conn = self
            .pool
            .get()
            .map_err(|e| OutboxError::Storage(e.to_string()))?;
        conn.transaction::<u64, diesel::result::Error, _>(|conn| {
 let now=diesel::select(diesel::dsl::sql::<Timestamptz>("transaction_timestamp()")).get_result::<DateTime<Utc>>(conn)?;
 let policy=ReservationTimeoutPolicy::new(now-spec.timeout,spec.max_attempts);
 let candidates=outbox_entries::table.filter(outbox_entries::status.eq(i32::from(OutboxStatus::Reserved)))
 .filter(outbox_entries::reserved_at.lt(policy.reserved_before)).order(outbox_entries::id.asc())
 .select((outbox_entries::id,outbox_entries::attempts,outbox_entries::scheduled_at)).for_update().load::<(Uuid,i32,DateTime<Utc>)>(conn)?;
 if candidates.is_empty() {return Ok(0);}
 let mut ids=Vec::with_capacity(candidates.len()); let mut statuses=Vec::with_capacity(candidates.len()); let mut schedules=Vec::with_capacity(candidates.len());
 for (id,attempts,previous) in candidates { let (status,scheduled)=policy.outcome(attempts,now,previous);ids.push(id);statuses.push(i32::from(status));schedules.push(scheduled); }
 let sql=r#"UPDATE outbox_entries AS entries SET status=outcomes.status, reservation_id=NULL, reserved_at=NULL,
 scheduled_at=outcomes.scheduled_at, updated_at=$4,
 extra_info=jsonb_set(COALESCE(entries.extra_info,'{}'::jsonb),'{errors}',COALESCE(entries.extra_info->'errors','[]'::jsonb)||jsonb_build_array($5::text),true)
 FROM unnest($1::uuid[],$2::int4[],$3::timestamptz[]) AS outcomes(id,status,scheduled_at) WHERE entries.id=outcomes.id"#;
 let affected=diesel::sql_query(sql).bind::<Array<SqlUuid>,_>(ids).bind::<Array<Int4>,_>(statuses).bind::<Array<Timestamptz>,_>(schedules).bind::<Timestamptz,_>(now).bind::<Text,_>("publication reservation timed out").execute(conn)?;
 Ok(affected as u64)
 }).map_err(|e| OutboxError::Storage(e.to_string()))
    }
}

#[cfg(test)]
mod port_contract_tests {
    use crate::prelude::*;
    #[test]
    fn repository_implements_all_storage_ports() {
        fn check<T: OutboxStorePort + OutboxReservePort + OutboxProcessPort + OutboxSweepPort>() {}
        check::<DieselPgOutboxRepository>();
    }
}
