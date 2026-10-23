//! Framework-free inbox HTTP contract and operations.
//!
//! HTTP adapters such as `mulac_poem` and `mulac_axum` parse request bodies
//! into JSON values and delegate parsing, validation and execution here.
pub mod io {
    pub use crate::contract::io::{
        RunRequest,
        RunResponse,
        SubmitInboxRequest,
        SubmitInboxResponse,
        //
    };
    pub use crate::error::io::InboxApiError;
    #[cfg(feature = "tokio")]
    pub use crate::runtime::io::AsyncInboxService;
    pub use crate::service::io::InboxService;
    #[cfg(feature = "testing")]
    pub use crate::testing::io as testing;
}

mod contract;
mod error;
#[cfg(feature = "tokio")]
mod runtime;
mod service;
#[cfg(feature = "testing")]
mod testing;
