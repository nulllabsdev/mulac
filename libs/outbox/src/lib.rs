pub mod io {
    pub use crate::assembly::io::*;
    pub use crate::outbox_consumer::io::{
        OutboxConsumer,
        OutboxConsumerRepository,
        ReservableOutboxSpec,
        //
    };
    pub use crate::record_events::io::{
        OutboxRecorder,
        OutboxRecorderRepository,
        //
    };
    pub use crate::stale_reservation_sweep::io::{
        ReservationSweeper,
        StaleReservationSpec,
        //
    };

    #[cfg(feature = "diesel")]
    pub use crate::assembly::infra_diesel::io::{
        DbPool,
        OutboxConsumerStorage,
        OutboxStoreStorage,
        build_pool,
        //
    };
    #[cfg(feature = "diesel")]
    pub use crate::outbox_consumer::io::repository as consumer_repository;
    #[cfg(feature = "diesel")]
    pub use crate::record_events::io::repository as recorder_repository;
    #[cfg(feature = "diesel")]
    pub use crate::stale_reservation_sweep::io::sweeper as reservation_sweeper;

    #[cfg(feature = "amqp")]
    pub use crate::amqp_publisher::io::{AmqpPublishConfig, AmqpPublisher};
    #[cfg(feature = "amqp")]
    pub use crate::confirmed_amqp_publisher::io::ConfirmedAmqpPublisher;
}

#[cfg(feature = "amqp")]
mod amqp_publisher;
mod assembly;
#[cfg(feature = "amqp")]
mod confirmed_amqp_publisher;
mod outbox_consumer;
mod record_events;
mod stale_reservation_sweep;
