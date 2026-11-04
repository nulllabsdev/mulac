mod adapter;
mod routes;
pub mod prelude {
    pub use super::adapter::prelude::*;
    pub use super::routes::EventingApi;
    pub use mulac_http::prelude::poem::{RunRequest, RunRequestData, RunResponse, RunResponseData};
}
