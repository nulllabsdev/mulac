use super::models::{ExtraInfoJsonb, MetadataJsonb};
use crate::application::prelude::InboxMessageMetadata;
use crate::domain::prelude::ExtraInfo;
use chrono::{DateTime, Utc};
use diesel::{Insertable, Queryable, QueryableByName, Selectable};
use uuid::Uuid;

#[derive(Debug, Insertable)]
#[diesel(table_name = super::schema::inbox_entries)]
pub struct NewInboxEntry {
    id: Uuid,
    payload: String,
    meta: MetadataJsonb,
    status: i32,
    scheduled_at: DateTime<Utc>,
    attempts: i32,
    received_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    extra_info: Option<ExtraInfoJsonb>,
}

impl NewInboxEntry {
    pub(super) fn new(
        id: Uuid,
        payload: String,
        meta: MetadataJsonb,
        status: i32,
        scheduled_at: DateTime<Utc>,
        attempts: i32,
        received_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            payload,
            meta,
            status,
            scheduled_at,
            attempts,
            received_at,
            updated_at,
            extra_info: None,
        }
    }
}

#[derive(Debug, Queryable, QueryableByName, Selectable)]
#[diesel(table_name = super::schema::inbox_entries)]
pub struct InboxEntry {
    id: Uuid,
    payload: String,
    meta: MetadataJsonb,
    status: i32,
    scheduled_at: DateTime<Utc>,
    attempts: i32,
    reservation_id: Option<Uuid>,
    reserved_at: Option<DateTime<Utc>>,
    received_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    processed_at: Option<DateTime<Utc>>,
    extra_info: Option<ExtraInfoJsonb>,
}

impl InboxEntry {
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn payload(&self) -> &str {
        &self.payload
    }
    pub fn meta(&self) -> &InboxMessageMetadata {
        &self.meta.0
    }
    pub fn status(&self) -> i32 {
        self.status
    }
    pub fn scheduled_at(&self) -> DateTime<Utc> {
        self.scheduled_at
    }
    pub fn attempts(&self) -> i32 {
        self.attempts
    }
    pub fn reservation_id(&self) -> Option<Uuid> {
        self.reservation_id
    }
    pub fn reserved_at(&self) -> Option<DateTime<Utc>> {
        self.reserved_at
    }
    pub fn received_at(&self) -> DateTime<Utc> {
        self.received_at
    }
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
    pub fn processed_at(&self) -> Option<DateTime<Utc>> {
        self.processed_at
    }
    pub fn extra_info(&self) -> Option<ExtraInfo> {
        self.extra_info.clone().map(Into::into)
    }
}
