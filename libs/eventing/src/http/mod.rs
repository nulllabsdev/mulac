//! HTTP-facing contracts, execution adaptation, and framework adapters.
//!
//! This boundary validates requests, bridges blocking execution, and formats
//! responses. Event dispatch and lifecycle rules belong to the execution layer;
//! the host assembles the consumer, dispatcher, and subscribers.
//!
//! The shared contract remains framework- and runtime-independent. The execution
//! bridge requires Tokio; callers without it use `EventConsumer` directly.
pub mod prelude {
    #[cfg(feature = "axum")]
    pub use super::axum::prelude as axum;
    pub use super::contract::prelude::{
        RunEventsRequest,
        RunEventsResponse,
        //
    };
    pub use super::error::prelude::EventingApiError;
    #[cfg(feature = "api-tokio")]
    pub use super::execution::prelude::AsyncEventingService;
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
