mod adapter;
mod routes;
#[cfg(feature = "axum-openapi")]
mod schema;
pub mod prelude {
    pub use super::adapter::prelude::*;
    pub use super::routes::outbox_routes;
}
