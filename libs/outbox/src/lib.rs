#[cfg(feature = "api")]
mod http;

#[cfg(feature = "amqp")]
mod amqp_publisher;
mod application;
#[cfg(feature = "amqp")]
mod confirmed_amqp_publisher;
mod domain;
mod execution;
#[cfg(feature = "diesel")]
mod infra_diesel;

pub mod prelude {
    #[cfg(feature = "amqp")]
    pub use crate::amqp_publisher::prelude::{AmqpPublishConfig, AmqpPublisher};
    pub use crate::application::prelude::*;
    #[cfg(feature = "amqp")]
    pub use crate::confirmed_amqp_publisher::prelude::ConfirmedAmqpPublisher;
    pub use crate::execution::prelude::*;
    #[cfg(feature = "api")]
    pub use crate::http::prelude::*;
    #[cfg(feature = "diesel")]
    pub use crate::infra_diesel::prelude::{
        DbPool,
        DieselPgOutboxRepository,
        build_pool,
        //
    };
}
