//! Outbox application-layer envelopes and errors.
//!
//! Application types are added incrementally according to the implementation checklist.

pub mod prelude {
    pub use super::models::{
        NewOutboxEnvelope,
        NewOutboxMetadata,
        OutboundMessageEnvelope,
        OutboxEntryEnvelope,
        OutboxError,
        //
    };
    pub(crate) use super::validate_sequence;
    pub use super::{NewOutboxEntry, OutboxEntry, OutboxEntryMetadata};
}

mod models {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct NewOutboxEnvelope {
        pub payload: String,
        pub metadata: NewOutboxMetadata,
    }

    #[derive(Debug, thiserror::Error)]
    pub enum OutboxError {
        #[error("storage error: {0}")]
        Storage(String),
        #[error("routing error: {0}")]
        Routing(String),
        #[error("serialization error: {0}")]
        Serialization(String),
        #[error("transport error: {0}")]
        Transport(String),
        #[error("reservation error: {0}")]
        Reservation(String),
        #[error("publish error: {0}")]
        Publish(String),
        #[error("missing reservation for entry {id}")]
        MissingReservation { id: uuid::Uuid },
        #[error("conversion error: {0}")]
        Conversion(String),
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct OutboundMessageEnvelope {
        pub payload: Vec<u8>,
        pub metadata: crate::application::prelude::OutboxEntryMetadata,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct OutboxEntryEnvelope {
        pub message: crate::application::prelude::OutboxEntry,
        pub metadata: crate::application::prelude::OutboxEntryMetadata,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct NewOutboxMetadata {
        pub sequence: Option<u64>,
        pub event_id: uuid::Uuid,
        pub message_id: Option<uuid::Uuid>,
        pub correlation_id: Option<uuid::Uuid>,
        pub causation_id: Option<uuid::Uuid>,
        pub event_type: String,
        pub routing_key: String,
        pub source: Option<String>,
        pub content_type: Option<String>,
    }
}

mod conversions {
    use super::models::NewOutboxMetadata;
    use crate::application::prelude::OutboxEntryMetadata;

    impl From<NewOutboxMetadata> for OutboxEntryMetadata {
        fn from(metadata: NewOutboxMetadata) -> Self {
            let message_id = metadata.message_id.unwrap_or(metadata.event_id);
            OutboxEntryMetadata {
                sequence: metadata.sequence,
                event_id: metadata.event_id,
                message_id,
                correlation_id: metadata.correlation_id,
                causation_id: metadata.causation_id,
                event_type: metadata.event_type,
                routing_key: metadata.routing_key,
                source: metadata.source,
                content_type: metadata.content_type,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::models::NewOutboxMetadata;
    use crate::application::prelude::OutboxEntryMetadata;
    use uuid::Uuid;

    fn metadata(event_id: Uuid, message_id: Option<Uuid>) -> NewOutboxMetadata {
        NewOutboxMetadata {
            sequence: None,
            event_id,
            message_id,
            correlation_id: None,
            causation_id: None,
            event_type: "UserRegistered".into(),
            routing_key: "users.registered".into(),
            source: Some("identity-service".into()),
            content_type: Some("application/json".into()),
        }
    }

    #[test]
    fn missing_message_id_defaults_to_event_id() {
        let event_id = Uuid::now_v7();
        let converted = OutboxEntryMetadata::from(metadata(event_id, None));
        assert_eq!(converted.event_id, event_id);
        assert_eq!(converted.message_id, event_id);
    }

    #[test]
    fn supplied_message_id_is_preserved() {
        let event_id = Uuid::now_v7();
        let message_id = Uuid::now_v7();
        let mut input = metadata(event_id, Some(message_id));
        input.sequence = Some(7);
        let converted = OutboxEntryMetadata::from(input);
        assert_eq!(converted.sequence, Some(7));
        assert_eq!(converted.event_id, event_id);
        assert_eq!(converted.message_id, message_id);
    }
}

use crate::application::prelude::{ExtraInfo, OutboxStatus};
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OutboxEntryMetadata {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "sequence"
    )]
    pub sequence: Option<u64>,
    pub event_id: uuid::Uuid,
    pub message_id: uuid::Uuid,
    pub correlation_id: Option<uuid::Uuid>,
    pub causation_id: Option<uuid::Uuid>,
    pub event_type: String,
    pub routing_key: String,
    pub source: Option<String>,
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutboxEntry {
    pub id: uuid::Uuid,
    pub status: OutboxStatus,
    pub payload: String,
    pub meta: OutboxEntryMetadata,
    pub scheduled_at: chrono::DateTime<chrono::Utc>,
    pub attempts: i32,
    pub reservation_id: Option<uuid::Uuid>,
    pub reserved_at: Option<chrono::DateTime<chrono::Utc>>,
    pub received_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub processed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_error: Option<String>,
    pub extra_info: Option<ExtraInfo>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewOutboxEntry {
    pub id: uuid::Uuid,
    pub payload: String,
    pub meta: OutboxEntryMetadata,
    pub scheduled_at: chrono::DateTime<chrono::Utc>,
    pub received_at: chrono::DateTime<chrono::Utc>,
}

fn sequence<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<u64>, D::Error> {
    let value = <Option<u64> as serde::Deserialize>::deserialize(deserializer)?;
    validate_sequence(value).map_err(serde::de::Error::custom)?;
    Ok(value)
}

pub(crate) fn validate_sequence(value: Option<u64>) -> Result<(), &'static str> {
    if value.is_some_and(|sequence| sequence == 0 || sequence > i64::MAX as u64) {
        return Err("sequence must be between 1 and i64::MAX");
    }
    Ok(())
}
