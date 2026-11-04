//! Synchronous event dispatch shared by HTTP triggers and application workers.
//!
//! The consumer owns batch reservation and outcome recording. The dispatcher
//! invokes the subscriber; direct gateways also use it without queue consumption.
//! HTTP adaptation and worker scheduling remain outside this module. Contracts
//! and storage ports remain in `application`.

mod event_consumer;
mod event_dispatcher;
mod item_boundary;
mod reservable;

pub mod prelude {
    pub use super::event_consumer::prelude::EventConsumer;
    pub use super::event_dispatcher::prelude::{EventDispatcher, EventSubscriberPort};
    #[cfg(feature = "diesel")]
    pub(crate) use super::item_boundary::prelude::guard;
    pub(crate) use super::item_boundary::prelude::run_item;
    pub use super::reservable::prelude::ReservableEventSpec;
}
