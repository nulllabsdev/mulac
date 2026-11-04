mod command_gateway;
mod conversions;
mod criteria;
mod ports;
mod stale_command_sweep;

pub mod prelude {
    pub use super::command_gateway::prelude::CommandGateway;
    pub use super::criteria::Criterion;
    pub use super::models::{
        Command,
        CommandEnvelope,
        CommandError,
        CommandMetadata,
        NewCommand,
        NewCommandEnvelope,
        NewCommandMetadata,
        //
    };
    pub use super::ports::prelude::*;
    pub use super::stale_command_sweep::prelude::{CommandSweeper, StaleCommandSpec};
}

mod models {
    use crate::domain::prelude::ExtraInfo;
    use uuid::Uuid;

    #[derive(Debug, Clone)]
    pub struct NewCommand {
        pub command_type: String,
        pub payload: String,
    }

    /// Gateway input envelope. The caller must supply `command_id`; no ID
    /// generation occurs inside the system boundary.
    #[derive(Debug, Clone)]
    pub struct NewCommandEnvelope {
        pub command: NewCommand,
        pub metadata: Option<NewCommandMetadata>,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct NewCommandMetadata {
        pub command_id: Uuid,
        pub correlation_id: Option<Uuid>,
        pub causation_id: Option<Uuid>,
        pub source: Option<String>,
    }

    #[derive(Debug)]
    pub struct Command {
        pub id: Uuid,
        pub reservation_id: Uuid,
        pub command_type: String,
        pub payload: String,
        pub attempts: i32,
        pub extra_info: Option<ExtraInfo>,
    }

    /// Read-side envelope returned to the consumer after reservation.
    ///
    /// Constructed by the infra layer from the stored entry; contains all fields
    /// the consumer needs without exposing infra types.
    #[derive(Debug)]
    pub struct CommandEnvelope {
        pub command: Command,
        pub metadata: Option<CommandMetadata>,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct CommandMetadata {
        pub command_id: Uuid,
        pub correlation_id: Option<Uuid>,
        pub causation_id: Option<Uuid>,
        pub source: Option<String>,
    }

    #[derive(Debug, thiserror::Error)]
    pub enum CommandError {
        #[error("storage error: {0}")]
        Storage(String),
        #[error("reservation error: {0}")]
        Reservation(String),
        #[error("no handler registered for command type '{0}'")]
        HandlerNotFound(String),
        #[error("handler execution error: {0}")]
        HandlerExecution(String),
        #[error("missing reservation for entry {id}")]
        MissingReservation { id: Uuid },
        #[error("conversion error: {0}")]
        Conversion(String),
        #[error("eventing error: {0}")]
        Eventing(#[from] eventing::prelude::EventError),
    }

    impl CommandError {
        pub fn conversion(error: impl std::fmt::Display) -> Self {
            Self::Conversion(error.to_string())
        }

        pub fn missing_command_metadata() -> Self {
            Self::Conversion("command_id is required: metadata is missing".to_string())
        }

        pub fn invalid_event_sequence() -> Self {
            Self::Conversion("sequence must be between 1 and i64::MAX".to_string())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::CommandError;

        #[test]
        fn missing_command_metadata_returns_conversion_error() {
            assert!(matches!(
                CommandError::missing_command_metadata(),
                CommandError::Conversion(message)
                    if message == "command_id is required: metadata is missing"
            ));
        }

        #[test]
        fn conversion_preserves_error_text() {
            let error = "not a number".parse::<u64>().unwrap_err();
            let expected = error.to_string();
            assert!(matches!(
                CommandError::conversion(error),
                CommandError::Conversion(message) if message == expected
            ));
        }

        #[test]
        fn invalid_event_sequence_returns_conversion_error() {
            assert!(matches!(
                CommandError::invalid_event_sequence(),
                CommandError::Conversion(message)
                    if message == "sequence must be between 1 and i64::MAX"
            ));
        }
    }
}
