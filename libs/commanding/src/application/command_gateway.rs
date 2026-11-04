pub mod prelude {
    pub use super::gateway::CommandGateway;
}

mod gateway {
    use crate::application::prelude::CommandStorePort;
    use crate::application::prelude::{CommandError, NewCommandEnvelope};
    use crate::execution::prelude::CommandDispatcher;
    use std::sync::Arc;

    pub enum CommandGateway {
        Direct { dispatcher: Arc<CommandDispatcher> },
        TwoPhased { store: Arc<dyn CommandStorePort> },
    }

    impl CommandGateway {
        pub fn direct(dispatcher: Arc<CommandDispatcher>) -> Self {
            Self::Direct { dispatcher }
        }

        /// Record commands directly through the supplied storage port for later consumption.
        pub fn two_phased(store: Arc<dyn CommandStorePort>) -> Self {
            Self::TwoPhased { store }
        }

        pub fn dispatch(&self, envelope: NewCommandEnvelope) -> Result<(), CommandError> {
            if envelope.metadata.is_none() {
                return Err(CommandError::missing_command_metadata());
            }
            match self {
                Self::Direct { dispatcher } => dispatcher.dispatch(&envelope),
                Self::TwoPhased { store } => store.record(&envelope),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::{
        CommandError, CommandGateway, CommandStorePort, NewCommand, NewCommandEnvelope,
        NewCommandMetadata,
    };
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    #[derive(Default)]
    struct RecordingStore(Mutex<Vec<NewCommandEnvelope>>);

    impl CommandStorePort for RecordingStore {
        fn record(&self, envelope: &NewCommandEnvelope) -> Result<(), CommandError> {
            self.0.lock().unwrap().push(envelope.clone());
            Ok(())
        }
    }

    fn envelope() -> NewCommandEnvelope {
        NewCommandEnvelope {
            command: NewCommand {
                command_type: "test".into(),
                payload: "{}".into(),
            },
            metadata: Some(NewCommandMetadata {
                command_id: Uuid::now_v7(),
                correlation_id: Some(Uuid::now_v7()),
                causation_id: Some(Uuid::now_v7()),
                source: Some("gateway-test".into()),
            }),
        }
    }

    #[test]
    fn two_phased_records_through_storage_port_without_changing_envelope() {
        let store = Arc::new(RecordingStore::default());
        let gateway = CommandGateway::two_phased(store.clone());
        let command = envelope();
        gateway.dispatch(command.clone()).unwrap();

        let recorded = store.0.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(
            recorded[0].command.command_type,
            command.command.command_type
        );
        assert_eq!(recorded[0].command.payload, command.command.payload);
        assert_eq!(
            serde_json::to_value(&recorded[0].metadata).unwrap(),
            serde_json::to_value(&command.metadata).unwrap()
        );
    }

    #[test]
    fn missing_metadata_is_rejected_before_recording() {
        let store = Arc::new(RecordingStore::default());
        let gateway = CommandGateway::two_phased(store.clone());
        let mut command = envelope();
        command.metadata = None;

        assert!(matches!(
            gateway.dispatch(command),
            Err(CommandError::Conversion(_))
        ));
        assert!(store.0.lock().unwrap().is_empty());
    }

    #[test]
    fn storage_errors_propagate_to_caller() {
        struct FailingStore;
        impl CommandStorePort for FailingStore {
            fn record(&self, _: &NewCommandEnvelope) -> Result<(), CommandError> {
                Err(CommandError::Storage("unavailable".into()))
            }
        }

        let store: Arc<dyn CommandStorePort> = Arc::new(FailingStore);
        let gateway = CommandGateway::two_phased(store);
        assert!(matches!(
            gateway.dispatch(envelope()),
            Err(CommandError::Storage(message)) if message == "unavailable"
        ));
    }
}
