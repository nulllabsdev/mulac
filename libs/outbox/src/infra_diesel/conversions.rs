use crate::application::prelude::{
    NewOutboxEntry as DomainNewOutboxEntry,
    OutboxEntry as DomainOutboxEntry,
    OutboxStatus,
    //
};
use crate::application::prelude::{OutboxEntryEnvelope, OutboxError};

use super::entity::{NewOutboxEntryRecord, OutboxEntryRecord};
use super::models::MetadataJsonb;

impl From<&DomainNewOutboxEntry> for NewOutboxEntryRecord {
    fn from(entry: &DomainNewOutboxEntry) -> Self {
        Self::new(
            entry.id,
            entry.payload.clone(),
            MetadataJsonb(entry.meta.clone()),
            OutboxStatus::Received as i32,
            entry.scheduled_at,
            0,
            entry.received_at,
            entry.received_at,
        )
    }
}

impl TryFrom<OutboxEntryRecord> for OutboxEntryEnvelope {
    type Error = OutboxError;

    fn try_from(record: OutboxEntryRecord) -> Result<Self, Self::Error> {
        let status = OutboxStatus::try_from(record.status())
            .map_err(|err| OutboxError::Storage(err.to_string()))?;
        let metadata = record.meta().clone();

        Ok(OutboxEntryEnvelope {
            message: DomainOutboxEntry {
                id: record.id(),
                status,
                payload: record.payload().to_string(),
                meta: metadata.clone(),
                scheduled_at: record.scheduled_at(),
                attempts: record.attempts(),
                reservation_id: record.reservation_id(),
                reserved_at: record.reserved_at(),
                received_at: record.received_at(),
                updated_at: record.updated_at(),
                processed_at: record.processed_at(),
                last_error: record.last_error().map(str::to_owned),
                extra_info: record.extra_info(),
            },
            metadata,
        })
    }
}
