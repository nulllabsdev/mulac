//! Framework-free commanding HTTP contract and operations.
//!
//! HTTP adapters such as `mulac_poem` and `mulac_axum` parse request bodies
//! into JSON values and delegate validation and execution here. The command
//! consumer, with its dispatcher and handlers, is built by the application or
//! the kernel.
pub mod io {
    pub use crate::contract::io::{
        RunCommandsRequest,
        RunCommandsResponse,
        //
    };
    pub use crate::error::io::CommandingApiError;
    #[cfg(feature = "tokio")]
    pub use crate::runtime::io::AsyncCommandingService;
    pub use crate::service::io::CommandingService;
    #[cfg(feature = "testing")]
    pub use crate::testing::io as testing;
}

mod contract;
mod error;
#[cfg(feature = "tokio")]
mod runtime;
mod service;
#[cfg(feature = "testing")]
mod testing;
