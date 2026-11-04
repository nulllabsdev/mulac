use super::entity::{EventEntry, NewEventEntry};
use super::models::MetadataJsonb;
use crate::application::prelude::{EventEnvelope, EventError, EventMetadata, NewEventEnvelope};
use chrono::Utc;

impl TryFrom<&NewEventEnvelope> for NewEventEntry {
    type Error = EventError;

    fn try_from(envelope: &NewEventEnvelope) -> Result<Self, Self::Error> {
        let metadata = envelope
            .metadata
            .as_ref()
            .ok_or_else(EventError::missing_event_metadata)?;

        let now = Utc::now();
        if metadata
            .sequence
            .is_some_and(|value| value == 0 || value > i64::MAX as u64)
        {
            return Err(EventError::invalid_event_sequence());
        }
        let meta = Some(MetadataJsonb(
            serde_json::to_value(metadata).map_err(EventError::conversion)?,
        ));

        Ok(NewEventEntry {
            id: metadata.event_id,
            event_type: envelope.event_type.clone(),
            payload: envelope.payload.clone(),
            meta,
            scheduled_at: now,
            received_at: now,
            extra_info: None,
        })
    }
}

impl TryFrom<EventEntry> for EventEnvelope {
    type Error = EventError;

    fn try_from(entry: EventEntry) -> Result<Self, Self::Error> {
        let metadata = entry
            .meta
            .as_ref()
            .map(|m| serde_json::from_value::<EventMetadata>(m.0.clone()))
            .transpose()
            .map_err(EventError::conversion)?;

        let reservation_id = entry
            .reservation_id
            .ok_or_else(|| EventError::MissingReservation { id: entry.id })?;

        Ok(EventEnvelope {
            id: entry.id,
            reservation_id,
            event_type: entry.event_type,
            payload: entry.payload,
            attempts: entry.attempts,
            metadata,
            extra_info: entry.extra_info.map(Into::into),
        })
    }
}
