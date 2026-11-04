use super::models::{ExtraInfoJsonb, MetadataJsonb};
use crate::application::prelude::{ExtraInfo, OutboxEntryMetadata};
use chrono::{DateTime, Utc};
use diesel::{Insertable, Queryable, QueryableByName, Selectable};
use uuid::Uuid;

#[derive(Debug, Insertable)]
#[diesel(table_name = super::schema::outbox_entries)]
pub struct NewOutboxEntryRecord {
    id: Uuid,
    status: i32,
    payload: String,
    meta: MetadataJsonb,
    scheduled_at: DateTime<Utc>,
    attempts: i32,
    received_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    extra_info: Option<ExtraInfoJsonb>,
}

impl NewOutboxEntryRecord {
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
            status,
            payload,
            meta,
            scheduled_at,
            attempts,
            received_at,
            updated_at,
            extra_info: None,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn status(&self) -> i32 {
        self.status
    }

    pub fn payload(&self) -> &str {
        &self.payload
    }

    pub fn meta(&self) -> &OutboxEntryMetadata {
        &self.meta.0
    }

    pub fn scheduled_at(&self) -> DateTime<Utc> {
        self.scheduled_at
    }

    pub fn attempts(&self) -> i32 {
        self.attempts
    }

    pub fn received_at(&self) -> DateTime<Utc> {
        self.received_at
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}

#[derive(Debug, Queryable, QueryableByName, Selectable)]
#[diesel(table_name = super::schema::outbox_entries)]
pub struct OutboxEntryRecord {
    id: Uuid,
    status: i32,
    payload: String,
    meta: MetadataJsonb,
    scheduled_at: DateTime<Utc>,
    attempts: i32,
    reservation_id: Option<Uuid>,
    reserved_at: Option<DateTime<Utc>>,
    received_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    processed_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    extra_info: Option<ExtraInfoJsonb>,
}

impl OutboxEntryRecord {
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        id: Uuid,
        status: i32,
        payload: String,
        meta: MetadataJsonb,
        scheduled_at: DateTime<Utc>,
        attempts: i32,
        reservation_id: Option<Uuid>,
        reserved_at: Option<DateTime<Utc>>,
        received_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        processed_at: Option<DateTime<Utc>>,
        last_error: Option<String>,
        extra_info: Option<ExtraInfo>,
    ) -> Self {
        Self {
            id,
            status,
            payload,
            meta,
            scheduled_at,
            attempts,
            reservation_id,
            reserved_at,
            received_at,
            updated_at,
            processed_at,
            last_error,
            extra_info: extra_info.map(ExtraInfoJsonb::from),
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn status(&self) -> i32 {
        self.status
    }

    pub fn payload(&self) -> &str {
        &self.payload
    }

    pub fn meta(&self) -> &OutboxEntryMetadata {
        &self.meta.0
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

    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    pub fn extra_info(&self) -> Option<ExtraInfo> {
        self.extra_info.clone().map(Into::into)
    }
}
