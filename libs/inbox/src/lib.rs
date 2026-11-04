//! Inbox components
#[cfg(feature = "api")]
mod http;

mod amqp_consumption;
mod application;
mod domain;
mod execution;
#[cfg(feature = "diesel")]
mod infra_diesel;

pub mod prelude {
    #[cfg(feature = "amqp")]
    pub use crate::amqp_consumption::prelude::{
        AmqpClientError,
        AmqpTransport,
        AmqpWorker,
        Channel,
        Connection,
        ConnectionProperties,
        connection,
        //
    };
    pub use crate::application::prelude::{
        AcknowledgeHandle,
        ExtraInfo,
        InboundMessageEnvelope,
        InboxError,
        InboxMessageEnvelope,
        InboxMessageMetadata,
        InboxProcessPort,
        InboxReservePort,
        InboxStorePort,
        InboxSweepPort,
        InboxTransportFuture,
        InboxTransportPort,
        InboxTransportResult,
        NewInboxMessageEnvelope,
        ReservationSweeper,
        StaleReservationSpec,
        //
    };
    pub use crate::execution::prelude::*;
    #[cfg(feature = "api")]
    pub use crate::http::prelude::*;
    #[cfg(feature = "diesel")]
    pub use crate::infra_diesel::prelude::{
        DbPool,
        DieselPgInboxRepository,
        build_pool,
        //
    };
}
