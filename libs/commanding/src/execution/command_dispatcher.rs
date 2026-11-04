pub mod prelude {
    pub use super::{
        ApplicationEvent, CommandDispatcher, CommandHandlerPort, ErasedCommandHandler, wrap_handler,
    };
}

use crate::application::prelude::{CommandError, NewCommandEnvelope};
use eventing::prelude::{EventDispatchPort, NewEventEnvelope, NewEventMetadata};
use serde::de::DeserializeOwned;
use std::sync::Arc;
use uuid::Uuid;

#[rustfmt::skip]
pub trait ApplicationEvent: serde::Serialize + Send + Sync {
    fn event_type(&self) -> &'static str;
    /// Optional application revision; framework identity remains authoritative.
    fn sequence(&self) -> Option<u64> { None }
}

#[rustfmt::skip]
pub trait CommandHandlerPort<C, E>: Send + Sync {
    fn execute(&self, command: C) -> Result<Vec<E>, CommandError>;
}

#[rustfmt::skip]
pub trait ErasedCommandHandler: Send + Sync {
    fn execute(&self, envelope: &NewCommandEnvelope) -> Result<Vec<NewEventEnvelope>, CommandError>;
}

struct HandlerWrapper<C, E> {
    inner: Arc<dyn CommandHandlerPort<C, E>>,
}

impl<C, E> ErasedCommandHandler for HandlerWrapper<C, E>
where
    C: DeserializeOwned + Send + Sync,
    E: ApplicationEvent,
{
    fn execute(
        &self,
        envelope: &NewCommandEnvelope,
    ) -> Result<Vec<NewEventEnvelope>, CommandError> {
        let command: C =
            serde_json::from_str(&envelope.command.payload).map_err(CommandError::conversion)?;

        let command_metadata = envelope.metadata.as_ref();
        let causation_id = command_metadata.map(|m| m.command_id);
        let correlation_id = command_metadata
            .and_then(|m| m.correlation_id)
            .or(causation_id);

        self.inner
            .execute(command)?
            .into_iter()
            .map(|event| {
                let sequence = event.sequence();
                if sequence.is_some_and(|value| value == 0 || value > i64::MAX as u64) {
                    return Err(CommandError::invalid_event_sequence());
                }

                let event_type = event.event_type().to_string();
                let payload = serde_json::to_string(&event).map_err(CommandError::conversion)?;
                let event_metadata = NewEventMetadata {
                    sequence,
                    event_id: Uuid::now_v7(),
                    correlation_id,
                    causation_id,
                    source: None,
                };

                let event_envelope =
                    NewEventEnvelope::new(event_type, payload, Some(event_metadata));

                Ok(event_envelope)
            })
            .collect()
    }
}

#[rustfmt::skip]
pub fn wrap_handler<C, E>(handler: Arc<dyn CommandHandlerPort<C, E>>) -> Arc<dyn ErasedCommandHandler>
where
    C: DeserializeOwned + Send + Sync + 'static,
    E: ApplicationEvent + 'static,
{
    Arc::new(HandlerWrapper { inner: handler })
}

pub struct CommandDispatcher {
    handler: Arc<dyn ErasedCommandHandler>,
    event_dispatcher: Arc<dyn EventDispatchPort>,
}

impl CommandDispatcher {
    #[rustfmt::skip]
    pub fn new(handler: Arc<dyn ErasedCommandHandler>, event_dispatcher: Arc<dyn EventDispatchPort>) -> Self {
        Self { handler, event_dispatcher }
    }

    pub fn dispatch(&self, envelope: &NewCommandEnvelope) -> Result<(), CommandError> {
        let events = self.handler.execute(envelope)?;
        for event in events {
            self.event_dispatcher.dispatch(event)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod sequence_tests {
    use super::prelude::*;
    use crate::application::prelude::{
        CommandError, NewCommand, NewCommandEnvelope, NewCommandMetadata,
    };
    use std::sync::Arc;
    #[derive(serde::Serialize)]
    struct Event {
        value: String,
        #[serde(skip)]
        revision: Option<u64>,
    }
    impl ApplicationEvent for Event {
        fn event_type(&self) -> &'static str {
            "Example"
        }
        fn sequence(&self) -> Option<u64> {
            self.revision
        }
    }
    struct Handler;
    impl CommandHandlerPort<Option<u64>, Event> for Handler {
        fn execute(&self, revision: Option<u64>) -> Result<Vec<Event>, CommandError> {
            Ok(vec![Event {
                value: "unchanged".into(),
                revision,
            }])
        }
    }
    #[test]
    fn optional_sequence_preserves_framework_identity_and_payload() {
        let wrapper = wrap_handler(Arc::new(Handler));
        let id = uuid::Uuid::now_v7();
        let mut command = NewCommandEnvelope {
            command: NewCommand {
                command_type: "Example".into(),
                payload: "null".into(),
            },
            metadata: Some(NewCommandMetadata {
                command_id: id,
                correlation_id: None,
                causation_id: None,
                source: None,
            }),
        };
        for revision in [None, Some(1), Some(i64::MAX as u64)] {
            command.command.payload = serde_json::to_string(&revision).unwrap();
            let events = wrapper.execute(&command).unwrap();
            let metadata = events[0].metadata.as_ref().unwrap();
            assert_eq!(metadata.sequence, revision);
            assert_eq!(metadata.causation_id, Some(id));
            assert_eq!(metadata.correlation_id, Some(id));
            assert_ne!(metadata.event_id, id);
            assert_eq!(events[0].payload, r#"{"value":"unchanged"}"#);
            let json = serde_json::to_value(metadata).unwrap();
            assert_eq!(json.get("sequence").is_some(), revision.is_some());
            let read: eventing::prelude::EventMetadata = serde_json::from_value(json).unwrap();
            let forwarded = eventing::prelude::NewEventMetadata::from(&read);
            assert_eq!(forwarded.sequence, revision);
        }
        for revision in [0, i64::MAX as u64 + 1] {
            command.command.payload = revision.to_string();
            assert!(wrapper.execute(&command).is_err());
        }
    }
}
