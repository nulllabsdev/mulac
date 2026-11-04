//! HTTP-facing contracts, execution adaptation, and framework adapters.
//!
//! This boundary validates requests, bridges blocking execution, and formats
//! responses. Command execution and lifecycle rules belong to the application
//! layer; the host assembles the consumer, dispatcher, and handlers.
//!
//! The shared contract remains framework- and runtime-independent. The execution
//! bridge requires Tokio; callers without it use `CommandConsumer` directly.
pub mod prelude {
    #[cfg(feature = "axum")]
    pub use super::axum::prelude as axum;
    pub use super::contract::prelude::{
        RunCommandsRequest,
        RunCommandsResponse,
        //
    };
    pub use super::error::prelude::CommandingApiError;
    #[cfg(feature = "api-tokio")]
    pub use super::execution::prelude::AsyncCommandingService;
    #[cfg(feature = "poem")]
    pub use super::poem::prelude as poem;
    #[cfg(feature = "api-testing")]
    pub use super::testing::prelude as testing;
}

#[cfg(feature = "axum")]
mod axum;
mod contract;
mod error;
#[cfg(feature = "api-tokio")]
mod execution;
#[cfg(feature = "poem")]
mod poem;
#[cfg(feature = "api-testing")]
mod testing;
