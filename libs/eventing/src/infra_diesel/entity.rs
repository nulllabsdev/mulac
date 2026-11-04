use super::models::{ExtraInfoJsonb, MetadataJsonb};
use chrono::{DateTime, Utc};
use diesel::{Insertable, Queryable, QueryableByName, Selectable};
use uuid::Uuid;

#[derive(Debug, Insertable)]
#[diesel(table_name = super::schema::event_entries)]
pub struct NewEventEntry {
    pub id: Uuid,
    pub event_type: String,
    pub payload: String,
    pub meta: Option<MetadataJsonb>,
    pub scheduled_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub extra_info: Option<ExtraInfoJsonb>,
}

#[derive(Debug, Queryable, QueryableByName, Selectable)]
#[diesel(table_name = super::schema::event_entries)]
pub struct EventEntry {
    pub id: Uuid,
    pub event_type: String,
    pub status: i32,
    pub payload: String,
    pub meta: Option<MetadataJsonb>,
    pub scheduled_at: DateTime<Utc>,
    pub attempts: i32,
    pub reservation_id: Option<Uuid>,
    pub reserved_at: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
    pub extra_info: Option<ExtraInfoJsonb>,
}
