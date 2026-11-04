pub mod prelude {
    pub use super::EventGateway;
}

use crate::application::prelude::EventStorePort;
use crate::application::prelude::{EventDispatchPort, EventError, NewEventEnvelope};
use crate::execution::prelude::EventDispatcher;
use std::sync::Arc;

pub enum EventGateway {
    Direct { dispatcher: Arc<EventDispatcher> },
    TwoPhased { store: Arc<dyn EventStorePort> },
}

impl EventGateway {
    pub fn direct(dispatcher: Arc<EventDispatcher>) -> Self {
        Self::Direct { dispatcher }
    }

    /// Record events directly through the supplied storage port for later consumption.
    pub fn two_phased(store: Arc<dyn EventStorePort>) -> Self {
        Self::TwoPhased { store }
    }

    pub fn dispatch(&self, envelope: NewEventEnvelope) -> Result<(), EventError> {
        if envelope.metadata.is_none() {
            return Err(EventError::missing_event_metadata());
        }
        match self {
            Self::Direct { dispatcher } => dispatcher.dispatch(&envelope),
            Self::TwoPhased { store } => store.record(&envelope),
        }
    }
}

impl EventDispatchPort for EventGateway {
    fn dispatch(&self, event: NewEventEnvelope) -> Result<(), EventError> {
        if event.metadata.is_none() {
            return Err(EventError::EventDispatch(
                "event_id is required: metadata is missing".into(),
            ));
        }
        match self {
            Self::Direct { dispatcher } => dispatcher
                .dispatch(&event)
                .map_err(|e| EventError::EventDispatch(e.to_string())),
            Self::TwoPhased { store } => store
                .record(&event)
                .map_err(|e| EventError::EventDispatch(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::{
        EventDispatchPort,
        EventError,
        EventGateway,
        EventStorePort,
        NewEventEnvelope,
        NewEventMetadata,
        //
    };
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    #[derive(Default)]
    struct RecordingStore(Mutex<Vec<NewEventEnvelope>>);

    impl EventStorePort for RecordingStore {
        fn record(&self, envelope: &NewEventEnvelope) -> Result<(), EventError> {
            self.0.lock().unwrap().push(envelope.clone());
            Ok(())
        }
    }

    struct FailingStore;

    impl EventStorePort for FailingStore {
        fn record(&self, _: &NewEventEnvelope) -> Result<(), EventError> {
            Err(EventError::Storage("unavailable".into()))
        }
    }

    fn envelope() -> NewEventEnvelope {
        let metadata = NewEventMetadata {
            sequence: Some(1),
            event_id: Uuid::now_v7(),
            correlation_id: Some(Uuid::now_v7()),
            causation_id: Some(Uuid::now_v7()),
            source: Some("gateway-test".into()),
        };
        NewEventEnvelope::new("test".into(), "{}".into(), Some(metadata))
    }

    fn assert_recorded_unchanged(store: &RecordingStore, event: &NewEventEnvelope) {
        let recorded = store.0.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].event_type, event.event_type);
        assert_eq!(recorded[0].payload, event.payload);
        assert_eq!(
            serde_json::to_value(&recorded[0].metadata).unwrap(),
            serde_json::to_value(&event.metadata).unwrap()
        );
    }

    #[test]
    fn two_phased_records_through_storage_port_without_changing_envelope() {
        let store = Arc::new(RecordingStore::default());
        let gateway = EventGateway::two_phased(store.clone());
        let event = envelope();
        gateway.dispatch(event.clone()).unwrap();
        assert_recorded_unchanged(&store, &event);
    }

    #[test]
    fn dispatch_port_records_through_storage_port_without_changing_envelope() {
        let store = Arc::new(RecordingStore::default());
        let gateway = EventGateway::two_phased(store.clone());
        let event = envelope();
        EventDispatchPort::dispatch(&gateway, event.clone()).unwrap();
        assert_recorded_unchanged(&store, &event);
    }

    #[test]
    fn missing_metadata_is_rejected_before_recording() {
        let store = Arc::new(RecordingStore::default());
        let gateway = EventGateway::two_phased(store.clone());
        let mut event = envelope();
        event.metadata = None;

        assert!(matches!(
            gateway.dispatch(event.clone()),
            Err(EventError::Conversion(_))
        ));
        assert!(matches!(
            EventDispatchPort::dispatch(&gateway, event),
            Err(EventError::EventDispatch(_))
        ));
        assert!(store.0.lock().unwrap().is_empty());
    }

    #[test]
    fn storage_errors_propagate_to_caller() {
        let store: Arc<dyn EventStorePort> = Arc::new(FailingStore);
        let gateway = EventGateway::two_phased(store);

        assert!(matches!(
            gateway.dispatch(envelope()),
            Err(EventError::Storage(message)) if message == "unavailable"
        ));
        assert!(matches!(
            EventDispatchPort::dispatch(&gateway, envelope()),
            Err(EventError::EventDispatch(message)) if message == "storage error: unavailable"
        ));
    }
}
