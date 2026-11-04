//! Synchronous outbox publication, called by HTTP triggers or directly by applications.
//!
//! The consumer owns batch reservation, publication through the publisher port,
//! and outcome recording. HTTP adaptation and scheduling remain outside this
//! module. Contracts and storage ports remain in `application`.

mod item_boundary;
mod outbox_consumer;
mod reservable;

pub mod prelude {
    #[cfg(feature = "diesel")]
    pub(crate) use super::item_boundary::prelude::guard;
    pub(crate) use super::item_boundary::prelude::run_item;
    pub use super::outbox_consumer::prelude::OutboxConsumer;
    pub use super::reservable::prelude::ReservableOutboxSpec;
}
