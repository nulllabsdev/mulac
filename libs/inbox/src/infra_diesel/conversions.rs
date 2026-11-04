use super::entity::{InboxEntry, NewInboxEntry};
use super::models::MetadataJsonb;
use crate::application::prelude::NewInboxMessageEnvelope;
use crate::application::prelude::{InboxError, InboxMessageEnvelope};
use crate::application::prelude::{InboxMessage, InboxStatus};
use chrono::Utc;

impl From<NewInboxMessageEnvelope> for NewInboxEntry {
    fn from(message: NewInboxMessageEnvelope) -> Self {
        let now = Utc::now();
        NewInboxEntry::new(
            *message.id(),
            message.payload().to_string(),
            MetadataJsonb(message.meta.into()),
            InboxStatus::Received as i32,
            now,
            0,
            now,
            now,
        )
    }
}

impl TryFrom<InboxEntry> for InboxMessageEnvelope {
    type Error = InboxError;

    fn try_from(entry: InboxEntry) -> Result<Self, Self::Error> {
        let status = InboxStatus::try_from(entry.status())
            .map_err(|e| InboxError::Storage(e.to_string()))?;

        Ok(InboxMessageEnvelope {
            msg: InboxMessage {
                id: entry.id(),
                payload: entry.payload().to_string(),
                status,
                scheduled_at: entry.scheduled_at(),
                attempts: entry.attempts(),
                reservation_id: entry.reservation_id(),
                reserved_at: entry.reserved_at(),
                extra_info: entry.extra_info(),
            },
            meta: entry.meta().clone(),
        })
    }
}
