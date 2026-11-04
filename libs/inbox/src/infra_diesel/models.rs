use crate::application::prelude::InboxMessageMetadata;
use crate::domain::prelude::ExtraInfo;
use diesel::deserialize::{FromSql, Result as DeserializeResult};
use diesel::pg::{Pg, PgValue};
use diesel::serialize::{IsNull, Output, Result as SerializeResult, ToSql};
use diesel::sql_types::Jsonb;
use diesel::{AsExpression, FromSqlRow};
use serde_json::{from_slice, to_writer};
use std::io::Write;

/// Newtype to implement diesel JSONB serialization for `InboxMessageMetadata`
/// without violating orphan rules (both `ToSql`/`FromSql` and the type are foreign).
#[derive(Debug, Clone, PartialEq, Eq, AsExpression, FromSqlRow)]
#[diesel(sql_type = Jsonb)]
pub struct MetadataJsonb(pub InboxMessageMetadata);

impl ToSql<Jsonb, Pg> for MetadataJsonb {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> SerializeResult {
        // Postgres jsonb stores a leading format-version byte before the payload.
        out.write_all(&[1])?;
        to_writer(out, &self.0)?;
        Ok(IsNull::No)
    }
}

impl FromSql<Jsonb, Pg> for MetadataJsonb {
    fn from_sql(bytes: PgValue<'_>) -> DeserializeResult<Self> {
        let bytes = bytes.as_bytes();
        if bytes.is_empty() {
            return Err("empty jsonb value".into());
        }

        if bytes[0] != 1 {
            return Err(format!("unsupported jsonb version: {}", bytes[0]).into());
        }

        Ok(MetadataJsonb(from_slice(&bytes[1..])?))
    }
}

/// Canonical wire shape for `extra_info` is `{"errors": [...]}`.
/// The stale-reservation sweep SQL appends into the `{errors}` path directly.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, AsExpression, FromSqlRow,
)]
#[diesel(sql_type = Jsonb)]
pub struct ExtraInfoJsonb {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    errors: Vec<String>,
}

impl From<ExtraInfo> for ExtraInfoJsonb {
    fn from(value: ExtraInfo) -> Self {
        Self {
            errors: value.errors().to_vec(),
        }
    }
}

impl From<ExtraInfoJsonb> for ExtraInfo {
    fn from(value: ExtraInfoJsonb) -> Self {
        ExtraInfo::with_errors(value.errors)
    }
}

impl ToSql<Jsonb, Pg> for ExtraInfoJsonb {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> SerializeResult {
        // Postgres jsonb stores a leading format-version byte before the payload.
        out.write_all(&[1])?;
        to_writer(out, self)?;
        Ok(IsNull::No)
    }
}

impl FromSql<Jsonb, Pg> for ExtraInfoJsonb {
    fn from_sql(bytes: PgValue<'_>) -> DeserializeResult<Self> {
        let bytes = bytes.as_bytes();
        if bytes.is_empty() {
            return Err("empty jsonb value".into());
        }

        // Postgres jsonb stores a leading format-version byte before the payload.
        if bytes[0] != 1 {
            return Err(format!("unsupported jsonb version: {}", bytes[0]).into());
        }

        Ok(from_slice(&bytes[1..])?)
    }
}
