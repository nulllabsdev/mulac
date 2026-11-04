#[cfg(feature = "api")]
mod http;

mod application;
mod domain;
mod execution;
#[cfg(feature = "diesel")]
mod infra_diesel;

pub mod prelude {
    pub use crate::application::prelude::*;
    pub use crate::domain::prelude::{EventStatus, ExtraInfo, UnknownEventStatus};
    pub use crate::execution::prelude::*;
    #[cfg(feature = "api")]
    pub use crate::http::prelude::*;
    #[cfg(feature = "diesel")]
    pub use crate::infra_diesel::prelude::*;
}
