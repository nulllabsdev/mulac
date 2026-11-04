use crate::application::prelude::{
    InboundMessageEnvelope,
    InboxError,
    InboxMessageEnvelope,
    InboxMessageMetadata,
    NewInboxMessage,
    NewInboxMessageEnvelope,
    NewInboxMessageMetadata,
    //
};
use commanding::prelude::{NewCommand, NewCommandEnvelope, NewCommandMetadata};
use uuid::Uuid;

impl From<NewInboxMessageMetadata> for InboxMessageMetadata {
    fn from(message: NewInboxMessageMetadata) -> Self {
        InboxMessageMetadata {
            message_id: Some(message.message_id),
            correlation_id: message.correlation_id,
            source: message.source,
            routing_key: message.routing_key,
        }
    }
}

impl From<InboundMessageEnvelope> for NewInboxMessageEnvelope {
    /// Convert an inbound transport envelope into a write-side envelope.
    ///
    /// If the transport did not provide a `message_id`, a new UUID v7 is
    /// generated. All other metadata fields are passed through unchanged.
    fn from(msg: InboundMessageEnvelope) -> Self {
        let meta = NewInboxMessageMetadata {
            message_id: msg.message_id.unwrap_or_else(Uuid::now_v7),
            correlation_id: msg.correlation_id,
            source: msg.source,
            routing_key: msg.routing_key,
        };

        NewInboxMessageEnvelope {
            msg: NewInboxMessage {
                payload: msg.payload,
            },
            meta,
        }
    }
}

impl TryFrom<InboxMessageEnvelope> for NewCommandEnvelope {
    type Error = InboxError;

    fn try_from(value: InboxMessageEnvelope) -> Result<Self, Self::Error> {
        let payload = value.payload().to_string();
        let command_type = value
            .meta
            .routing_key
            .ok_or_else(|| InboxError::Conversion("missing routing_key".into()))?;
        let metadata = NewCommandMetadata {
            command_id: Uuid::now_v7(),
            correlation_id: value.meta.correlation_id,
            causation_id: value.meta.message_id,
            source: value.meta.source,
        };
        Ok(NewCommandEnvelope {
            command: NewCommand {
                command_type,
                payload,
            },
            metadata: Some(metadata),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inbound(
        payload: &str,
        message_id: Option<Uuid>,
        correlation_id: Option<Uuid>,
        source: Option<&str>,
        routing_key: Option<&str>,
    ) -> InboundMessageEnvelope {
        InboundMessageEnvelope {
            payload: payload.into(),
            message_id,
            correlation_id,
            source: source.map(Into::into),
            routing_key: routing_key.map(Into::into),
        }
    }

    #[test]
    fn preserves_existing_message_id() {
        let id = Uuid::new_v4();
        let envelope = NewInboxMessageEnvelope::from(inbound("hello", Some(id), None, None, None));
        assert_eq!(*envelope.id(), id);
    }

    #[test]
    fn generates_message_id_when_absent() {
        let envelope = NewInboxMessageEnvelope::from(inbound("hello", None, None, None, None));
        assert_ne!(*envelope.id(), Uuid::nil());
    }

    #[test]
    fn two_conversions_without_id_produce_distinct_ids() {
        let a = NewInboxMessageEnvelope::from(inbound("x", None, None, None, None));
        let b = NewInboxMessageEnvelope::from(inbound("x", None, None, None, None));
        assert_ne!(a.id(), b.id());
    }

    #[test]
    fn preserves_payload() {
        let envelope = NewInboxMessageEnvelope::from(inbound("my-payload", None, None, None, None));
        assert_eq!(envelope.payload(), "my-payload");
    }

    #[test]
    fn preserves_metadata_fields() {
        let corr = Uuid::new_v4();
        let envelope = NewInboxMessageEnvelope::from(inbound(
            "data",
            None,
            Some(corr),
            Some("payments"),
            Some("user.created"),
        ));
        assert_eq!(envelope.meta.correlation_id, Some(corr));
        assert_eq!(envelope.meta.source.as_deref(), Some("payments"));
        assert_eq!(envelope.meta.routing_key.as_deref(), Some("user.created"));
    }

    #[test]
    fn metadata_conversion_wraps_message_id_in_some() {
        let id = Uuid::new_v4();
        let meta = NewInboxMessageMetadata {
            message_id: id,
            correlation_id: None,
            source: None,
            routing_key: None,
        };
        let converted = InboxMessageMetadata::from(meta);
        assert_eq!(converted.message_id, Some(id));
    }
}
