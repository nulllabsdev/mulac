//! HTTP-facing contracts, execution adaptation, and framework adapters.
//!
//! This boundary validates requests, bridges blocking execution, and formats
//! responses. Publication and lifecycle rules belong to the execution layer; the
//! host assembles the consumer and owns the publisher.
//!
//! The shared contract remains framework- and runtime-independent. The execution
//! bridge requires Tokio; callers without it use `OutboxConsumer` directly.
pub mod prelude {
    #[cfg(feature = "axum")]
    pub use super::axum::prelude as axum;
    pub use super::contract::prelude::{
        RunOutboxRequest,
        RunOutboxResponse,
        //
    };
    pub use super::error::prelude::{
        OutboxApiError,
        OutboxApiErrorKind,
        //
    };
    #[cfg(feature = "api-tokio")]
    pub use super::execution::prelude::AsyncOutboxService;
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
