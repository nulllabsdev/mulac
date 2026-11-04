//! Synchronous command execution shared by HTTP triggers and application workers.
//!
//! The consumer owns batch reservation and outcome recording. The dispatcher
//! invokes typed handlers and dispatches their events; direct gateways also use
//! it without queue consumption. HTTP adaptation and worker scheduling remain
//! outside this module. Contracts and storage ports remain in `application`.

mod command_consumer;
mod command_dispatcher;
mod item_boundary;
mod reservable;

pub mod prelude {
    pub use super::command_consumer::prelude::CommandConsumer;
    pub use super::command_dispatcher::prelude::{
        ApplicationEvent, CommandDispatcher, CommandHandlerPort, ErasedCommandHandler, wrap_handler,
    };
    #[cfg(feature = "diesel")]
    pub(crate) use super::item_boundary::prelude::guard;
    pub(crate) use super::item_boundary::prelude::run_item;
    pub use super::reservable::prelude::ReservableCommandSpec;
}
