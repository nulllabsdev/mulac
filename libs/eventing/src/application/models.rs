pub mod prelude {
    pub use super::models::{
        EventEnvelope,
        EventError,
        EventMetadata,
        NewEventEnvelope,
        NewEventMetadata,
        //
    };
}

mod models {

    fn sequence<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u64>, D::Error> {
        let value = <Option<u64> as serde::Deserialize>::deserialize(deserializer)?;
        if value.is_some_and(|sequence| sequence == 0 || sequence > i64::MAX as u64) {
            return Err(serde::de::Error::custom(
                "sequence must be between 1 and i64::MAX",
            ));
        }
        Ok(value)
    }

    use crate::domain::prelude::ExtraInfo;
    use uuid::Uuid;

    /// Event envelope produced by a command handler and forwarded to the event
    /// dispatch port. Defined here (in commanding) because both commanding and
    /// eventing depend on this shared type; eventing re-exports it.
    #[derive(Debug, Clone)]
    pub struct NewEventEnvelope {
        pub event_type: String,
        pub payload: String,
        pub metadata: Option<NewEventMetadata>,
    }

    impl NewEventEnvelope {
        /// Build an envelope from an event type, serialized payload, and optional metadata.
        pub fn new(
            event_type: String,
            payload: String,
            metadata: Option<NewEventMetadata>,
        ) -> Self {
            Self {
                event_type,
                payload,
                metadata,
            }
        }
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct NewEventMetadata {
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "sequence"
        )]
        pub sequence: Option<u64>,
        pub event_id: Uuid,
        pub correlation_id: Option<Uuid>,
        pub causation_id: Option<Uuid>,
        pub source: Option<String>,
    }

    /// Read-side envelope returned to the consumer after reservation.
    ///
    /// Constructed by the infra layer from the stored entry; contains all fields
    /// the consumer needs without exposing infra types.
    #[derive(Debug)]
    pub struct EventEnvelope {
        pub id: Uuid,
        pub reservation_id: Uuid,
        pub event_type: String,
        pub payload: String,
        pub attempts: i32,
        pub metadata: Option<EventMetadata>,
        pub extra_info: Option<ExtraInfo>,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct EventMetadata {
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "sequence"
        )]
        pub sequence: Option<u64>,
        pub event_id: Uuid,
        pub correlation_id: Option<Uuid>,
        pub causation_id: Option<Uuid>,
        pub source: Option<String>,
    }

    #[derive(Debug, thiserror::Error)]
    pub enum EventError {
        #[error("storage error: {0}")]
        Storage(String),
        #[error("reservation error: {0}")]
        Reservation(String),
        #[error("no subscriber registered for event type '{0}'")]
        SubscriberNotFound(String),
        #[error("subscriber execution error: {0}")]
        SubscriberExecution(String),
        #[error("missing reservation for entry {id}")]
        MissingReservation { id: Uuid },
        #[error("conversion error: {0}")]
        Conversion(String),
        #[error("event dispatch error: {0}")]
        EventDispatch(String),
    }

    impl EventError {
        pub fn conversion(error: impl std::fmt::Display) -> Self {
            Self::Conversion(error.to_string())
        }

        pub fn missing_event_metadata() -> Self {
            Self::Conversion("event_id is required: metadata is missing".to_string())
        }

        pub fn invalid_event_sequence() -> Self {
            Self::Conversion("sequence must be between 1 and i64::MAX".to_string())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::EventError;

        #[test]
        fn missing_event_metadata_returns_conversion_error() {
            assert!(matches!(
                EventError::missing_event_metadata(),
                EventError::Conversion(message)
                    if message == "event_id is required: metadata is missing"
            ));
        }

        #[test]
        fn conversion_preserves_error_text() {
            let error = "not a number".parse::<u64>().unwrap_err();
            let expected = error.to_string();
            assert!(matches!(
                EventError::conversion(error),
                EventError::Conversion(message) if message == expected
            ));
        }

        #[test]
        fn invalid_event_sequence_returns_conversion_error() {
            assert!(matches!(
                EventError::invalid_event_sequence(),
                EventError::Conversion(message)
                    if message == "sequence must be between 1 and i64::MAX"
            ));
        }
    }
}

#[cfg(test)]
mod envelope_tests {
    use crate::prelude::{NewEventEnvelope, NewEventMetadata};

    #[test]
    fn constructor_preserves_fields_with_and_without_metadata() {
        let metadata = NewEventMetadata {
            sequence: Some(1),
            event_id: uuid::Uuid::now_v7(),
            correlation_id: Some(uuid::Uuid::now_v7()),
            causation_id: Some(uuid::Uuid::now_v7()),
            source: Some("test".into()),
        };

        for metadata in [None, Some(metadata)] {
            let envelope = NewEventEnvelope::new(
                "test-event".into(),
                r#"{"value":1}"#.into(),
                metadata.clone(),
            );
            assert_eq!(envelope.event_type, "test-event");
            assert_eq!(envelope.payload, r#"{"value":1}"#);
            assert_eq!(
                serde_json::to_value(envelope.metadata).unwrap(),
                serde_json::to_value(metadata).unwrap()
            );
        }
    }
}

#[cfg(test)]
mod sequence_tests {
    use super::prelude::*;
    #[test]
    fn metadata_rejects_invalid_revisions_and_preserves_legacy_omission() {
        let mut value = serde_json::json!({"event_id":uuid::Uuid::now_v7(),"correlation_id":null,"causation_id":null,"source":null});
        let old: NewEventMetadata = serde_json::from_value(value.clone()).unwrap();
        assert!(old.sequence.is_none());
        assert_eq!(serde_json::to_value(old).unwrap(), value);
        for invalid in [
            serde_json::json!(0),
            serde_json::json!(-1),
            serde_json::json!(i64::MAX as u64 + 1),
            serde_json::json!("1"),
        ] {
            value["sequence"] = invalid;
            assert!(serde_json::from_value::<NewEventMetadata>(value.clone()).is_err());
            assert!(serde_json::from_value::<EventMetadata>(value.clone()).is_err());
        }
    }
}
