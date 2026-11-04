mod adapter;
mod routes;
mod types;
pub mod prelude {
    pub use super::adapter::prelude::*;
    pub use super::routes::InboxApi;
    pub use super::types::prelude::*;
    pub use mulac_http::prelude::poem::{RunRequest, RunRequestData, RunResponse, RunResponseData};
}
