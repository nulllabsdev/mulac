//! Synchronous inbox processing, called by HTTP triggers or directly by applications.
//!
//! The consumer owns batch reservation, forwarding through the command gateway,
//! and outcome recording. HTTP adaptation and scheduling remain outside this
//! module. Contracts and storage ports remain in `application`.

mod inbox_consumer;
mod item_boundary;
mod reservable;

pub mod prelude {
    pub use super::inbox_consumer::prelude::InboxConsumer;
    #[cfg(feature = "diesel")]
    pub(crate) use super::item_boundary::prelude::guard;
    pub(crate) use super::item_boundary::prelude::run_item;
    pub use super::reservable::prelude::ReservableInboxSpec;
}
