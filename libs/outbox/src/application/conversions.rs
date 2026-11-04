use crate::application::prelude::{
    OutboundMessageEnvelope,
    OutboxEntryEnvelope,
    OutboxError,
    //
};

impl TryFrom<&OutboxEntryEnvelope> for OutboundMessageEnvelope {
    type Error = OutboxError;

    fn try_from(entry: &OutboxEntryEnvelope) -> Result<Self, Self::Error> {
        if entry.metadata.routing_key.trim().is_empty() {
            return Err(OutboxError::Conversion("missing routing_key".into()));
        }

        Ok(Self {
            payload: entry.message.payload.clone().into_bytes(),
            metadata: entry.metadata.clone(),
        })
    }
}
