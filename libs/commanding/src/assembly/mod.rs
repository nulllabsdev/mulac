mod application;
pub(crate) mod domain;

#[cfg(feature = "diesel")]
pub(crate) mod infra_diesel;

pub mod io {
    pub use super::application::io::{
        Command,
        CommandEnvelope,
        CommandError,
        CommandMetadata,
        CommandProcessPort,
        CommandStorePort,
        NewCommand,
        NewCommandEnvelope,
        NewCommandMetadata,
        //
    };
    #[cfg(feature = "diesel")]
    pub(crate) use super::domain::Criterion;
    pub use super::domain::{CommandStatus, ExtraInfo, UnknownCommandStatus};
    #[cfg(feature = "diesel")]
    pub use super::infra_diesel::io::{
        CommandConsumerStorage,
        CommandEntry,
        CommandStoreStorage,
        NewCommandEntry,
        //
    };
    #[cfg(feature = "diesel")]
    pub(crate) use super::infra_diesel::schema::command_entries;
}
