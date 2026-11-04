#[cfg(feature = "api")]
mod http;

mod application;
mod domain;
mod execution;
#[cfg(feature = "diesel")]
mod infra_diesel;

pub mod prelude {
    pub use crate::application::prelude::*;
    pub use crate::domain::prelude::{
        CommandStatus, ExtraInfo, ReservationTimeoutPolicy, UnknownCommandStatus,
    };
    pub use crate::execution::prelude::*;
    #[cfg(feature = "api")]
    pub use crate::http::prelude::*;

    // Diesel backend (feature-gated)
    #[cfg(feature = "diesel")]
    pub use crate::infra_diesel::prelude::{
        CommandEntry,
        DieselPgCommandRepository,
        NewCommandEntry,
        //
    };
}
