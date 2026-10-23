//! Poem and poem-openapi adapter for the shared Mulac HTTP APIs.
//!
//! Applications declare the endpoints and delegate to [`io::InboxAdapter`] or,
//! with the `commanding`, `eventing` and `outbox` features,
//! `io::CommandingAdapter`, `io::EventingAdapter` and `io::OutboxAdapter`;
//! parsing, validation and execution live in `inbox_api`, `commanding_api`,
//! `eventing_api` and `outbox_api`.
pub mod io {
    pub use crate::adapter::io::{
        InboxAdapter,
        InboxSubmission,
        //
    };
    #[cfg(feature = "commanding")]
    pub use crate::commanding::io::CommandingAdapter;
    #[cfg(feature = "eventing")]
    pub use crate::eventing::io::EventingAdapter;
    #[cfg(feature = "outbox")]
    pub use crate::outbox::io::OutboxAdapter;
    pub use crate::types::io::{
        RunRequest,
        RunResponse,
        SubmitInboxRequest,
        SubmitInboxResponse,
        //
    };
}

mod adapter;
#[cfg(feature = "commanding")]
mod commanding;
#[cfg(feature = "eventing")]
mod eventing;
#[cfg(feature = "outbox")]
mod outbox;
mod types;
