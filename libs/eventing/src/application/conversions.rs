use crate::application::prelude::{
    EventEnvelope,
    EventMetadata,
    NewEventEnvelope,
    NewEventMetadata,
    //
};

impl From<&EventMetadata> for NewEventMetadata {
    fn from(meta: &EventMetadata) -> Self {
        NewEventMetadata {
            sequence: meta.sequence,
            event_id: meta.event_id,
            correlation_id: meta.correlation_id,
            causation_id: meta.causation_id,
            source: meta.source.clone(),
        }
    }
}

impl From<&EventEnvelope> for NewEventEnvelope {
    fn from(envelope: &EventEnvelope) -> Self {
        NewEventEnvelope {
            event_type: envelope.event_type.clone(),
            payload: envelope.payload.clone(),
            metadata: envelope.metadata.as_ref().map(NewEventMetadata::from),
        }
    }
}

impl From<EventMetadata> for NewEventMetadata {
    fn from(meta: EventMetadata) -> Self {
        NewEventMetadata {
            sequence: meta.sequence,
            event_id: meta.event_id,
            correlation_id: meta.correlation_id,
            causation_id: meta.causation_id,
            source: meta.source,
        }
    }
}
