//! Raw reservation rows: JSON shape errors must not abort Diesel's batch load.
pub mod prelude {
    pub(crate) use super::{RawReservedEntry, finish_reservation, record_failure};
}
use super::models::ExtraInfoJsonb;
use crate::application::prelude::*;
use crate::domain::prelude::*;
use crate::execution::prelude::guard;
use crate::infra_diesel::prelude::DieselPgCommandRepository;
use diesel::prelude::*;
use diesel::sql_types::{Int4, Nullable, Text, Uuid as SqlUuid};
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(QueryableByName)]
pub(crate) struct RawReservedEntry {
    #[diesel(sql_type = SqlUuid)]
    id: Uuid,
    #[diesel(sql_type = Int4)]
    status: i32,
    #[diesel(sql_type = Text)]
    payload: String,
    #[diesel(sql_type = Nullable<Text>)]
    meta: Option<String>,
    #[diesel(sql_type = Int4)]
    attempts: i32,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    reservation_id: Option<Uuid>,
    #[diesel(sql_type = Nullable<Text>)]
    extra_info: Option<String>,
    #[diesel(sql_type = Text)]
    command_type: String,
}

impl RawReservedEntry {
    fn decode(&self) -> Result<CommandEnvelope, &'static str> {
        let extra_info = self
            .extra_info
            .as_deref()
            .map(serde_json::from_str::<ExtraInfoJsonb>)
            .transpose()
            .map_err(|_| "invalid extra_info")?
            .map(Into::into);
        let reservation_id = self.reservation_id.ok_or("missing reservation")?;
        let metadata = self
            .meta
            .as_deref()
            .map(serde_json::from_str::<CommandMetadata>)
            .transpose()
            .map_err(|_| "invalid metadata")?;
        Ok(CommandEnvelope {
            command: Command {
                id: self.id,
                reservation_id,
                command_type: self.command_type.clone(),
                payload: self.payload.clone(),
                attempts: self.attempts,
                extra_info,
            },
            metadata,
        })
    }
}

/// Called on the reservation transaction's connection; never reacquire the pool.
pub(crate) fn finish_reservation(
    conn: &mut PgConnection,
    rows: Vec<RawReservedEntry>,
    reservation_id: Uuid,
) -> QueryResult<Vec<CommandEnvelope>> {
    let mut healthy = Vec::with_capacity(rows.len());
    for row in rows {
        if row.reservation_id != Some(reservation_id)
            || row.status != i32::from(CommandStatus::Reserved)
        {
            return Err(diesel::result::Error::NotFound);
        }
        match guard(|| row.decode(), || "decoding panicked") {
            Ok(envelope) => healthy.push(envelope),
            Err(error) => {
                let reason = format!(
                    "item {}, reservation {reservation_id}, stage reservation: {error}",
                    row.id
                );
                write_failure(
                    conn,
                    row.id,
                    reservation_id,
                    row.attempts,
                    row.extra_info,
                    Some(&reason),
                )?;
            }
        }
    }
    Ok(healthy)
}

#[derive(QueryableByName)]
struct FailureRow {
    #[diesel(sql_type = Int4)]
    attempts: i32,
    #[diesel(sql_type = Nullable<Text>)]
    extra_info: Option<String>,
}

/// Must run inside a transaction on the caller's connection.
pub(crate) fn record_failure(
    conn: &mut PgConnection,
    id: Uuid,
    reservation_id: Uuid,
    reason: Option<&str>,
) -> QueryResult<()> {
    let row = diesel::sql_query(
        "SELECT attempts, extra_info::text AS extra_info FROM command_entries
        WHERE id=$1 AND reservation_id=$2 AND status=$3 FOR UPDATE",
    )
    .bind::<SqlUuid, _>(id)
    .bind::<SqlUuid, _>(reservation_id)
    .bind::<Int4, _>(i32::from(CommandStatus::Reserved))
    .get_result::<FailureRow>(conn)?;
    write_failure(
        conn,
        id,
        reservation_id,
        row.attempts,
        row.extra_info,
        reason,
    )
}

fn write_failure(
    conn: &mut PgConnection,
    id: Uuid,
    reservation_id: Uuid,
    attempts: i32,
    original: Option<String>,
    reason: Option<&str>,
) -> QueryResult<()> {
    let extra_info = match reason {
        Some(reason) => Some(append_failure(raw_extra_info(original), reason).to_string()),
        None => original,
    };
    let delay = i64::from(attempts.max(1)) * DieselPgCommandRepository::RETRY_BACKOFF_SECONDS;
    let delay = delay.min(DieselPgCommandRepository::MAX_RETRY_BACKOFF_SECONDS);
    let updated = diesel::sql_query(
        r#"
        UPDATE command_entries SET status=$3, reservation_id=NULL, reserved_at=NULL,
            updated_at=now(), extra_info=$4::jsonb, scheduled_at=now() + ($6 * interval '1 second')
        WHERE id=$1 AND reservation_id=$2 AND status=$5
    "#,
    )
    .bind::<SqlUuid, _>(id)
    .bind::<SqlUuid, _>(reservation_id)
    .bind::<Int4, _>(i32::from(CommandStatus::Failed))
    .bind::<Nullable<Text>, _>(extra_info)
    .bind::<Int4, _>(i32::from(CommandStatus::Reserved))
    .bind::<diesel::sql_types::BigInt, _>(delay)
    .execute(conn)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(())
}

// Keep even JSON numbers outside serde_json::Value's representable range.
// Text loading prevents these values from failing the entire PostgreSQL batch.
fn raw_extra_info(original: Option<String>) -> Option<Value> {
    original.map(|text| serde_json::from_str(&text).unwrap_or(Value::String(text)))
}

fn append_failure(original: Option<Value>, reason: &str) -> Value {
    match original {
        None => json!({"errors": [reason]}),
        Some(Value::Object(mut object))
            if object.get("errors").is_none_or(|errors| {
                errors
                    .as_array()
                    .is_some_and(|values| values.iter().all(Value::is_string))
            }) =>
        {
            let mut errors = object
                .remove("errors")
                .and_then(|v| v.as_array().cloned())
                .unwrap_or_default();
            errors.push(Value::String(reason.to_owned()));
            object.insert("errors".into(), Value::Array(errors));
            Value::Object(object)
        }
        Some(original) => json!({"errors": [reason], "quarantined_extra_info": original}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_history_and_unrecognized_fields() {
        assert_eq!(
            append_failure(Some(json!({"errors":["old"],"other":42})), "new"),
            json!({"errors":["old","new"],"other":42})
        );
        assert_eq!(append_failure(None, "new"), json!({"errors":["new"]}));
    }
    #[test]
    fn retains_malformed_diagnostics_without_rejecting_failure_update() {
        for original in [
            json!(null),
            json!([1]),
            json!({"errors":42}),
            json!({"errors":[false]}),
        ] {
            let updated = append_failure(Some(original.clone()), "new");
            assert_eq!(updated["quarantined_extra_info"], original);
            assert_eq!(updated["errors"], json!(["new"]));
            assert!(serde_json::from_value::<ExtraInfoJsonb>(updated).is_ok());
        }
    }
    fn row() -> RawReservedEntry {
        RawReservedEntry {
            id: Uuid::nil(),
            status: 2,
            payload: "{}".into(),
            attempts: 1,
            reservation_id: Some(Uuid::now_v7()),
            extra_info: None,
            meta: Some(json!({"command_id":Uuid::nil()}).to_string()),
            command_type: "test".into(),
        }
    }
    #[test]
    fn decoding_is_local_and_preserves_existing_metadata_rules() {
        assert!(row().decode().is_ok());
        let mut missing = row();
        missing.meta = None;
        assert!(missing.decode().is_ok());
        for meta in [
            "null",
            "[]",
            "false",
            "42",
            "\"not metadata\"",
            "{\"source\":42}",
        ] {
            let mut bad = row();
            bad.meta = Some(meta.into());
            assert_eq!(bad.decode().unwrap_err(), "invalid metadata", "{meta}");
            assert!(row().decode().is_ok());
        }
        for extra in ["null", "42", "{\"errors\":[42]}", "{\"errors\":1e1000}"] {
            let mut bad = row();
            bad.extra_info = Some(extra.into());
            assert_eq!(bad.decode().unwrap_err(), "invalid extra_info", "{extra}");
        }
    }
    #[test]
    fn retains_json_numbers_outside_serde_value_range() {
        let original = "{\"errors\":1e1000}";
        assert!(serde_json::from_str::<Value>(original).is_err());
        let updated = append_failure(raw_extra_info(Some(original.into())), "new");
        assert_eq!(updated["quarantined_extra_info"], original);
        assert_eq!(updated["errors"], json!(["new"]));
        assert!(serde_json::from_value::<ExtraInfoJsonb>(updated).is_ok());
    }
}
