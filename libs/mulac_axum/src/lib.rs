//! Axum adapter for the shared Mulac HTTP APIs.
//!
//! Applications declare the routes and delegate to [`io::InboxAdapter`] or,
//! with the `commanding`, `eventing` and `outbox` features,
//! `io::CommandingAdapter`, `io::EventingAdapter` and `io::OutboxAdapter`;
//! parsing, validation and execution live in `inbox_api`, `commanding_api`,
//! `eventing_api` and `outbox_api`.
pub mod io {
    pub use crate::adapter::io::{
        InboxAdapter,
        InboxRunResponse,
        InboxSubmission,
        //
    };
    #[cfg(feature = "commanding")]
    pub use crate::commanding::io::{
        CommandingAdapter,
        CommandingHttpError,
        CommandsRunResponse,
        RunCommands,
        //
    };
    #[cfg(feature = "eventing")]
    pub use crate::eventing::io::{
        EventingAdapter,
        EventingHttpError,
        EventsRunResponse,
        RunEvents,
        //
    };
    pub use crate::extract::io::{
        InboxHttpError,
        RunInbox,
        SubmitInbox,
        //
    };
    #[cfg(feature = "outbox")]
    pub use crate::outbox::io::{
        OutboxAdapter,
        OutboxHttpError,
        OutboxRunResponse,
        RunOutbox,
        //
    };
}

mod adapter;
mod body;
#[cfg(feature = "commanding")]
mod commanding;
#[cfg(feature = "eventing")]
mod eventing;
mod extract;
#[cfg(feature = "outbox")]
mod outbox;
#[cfg(feature = "utoipa")]
mod schema;
