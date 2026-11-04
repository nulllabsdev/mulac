mod adapter;
mod extract;
mod routes;
#[cfg(feature = "axum-openapi")]
mod schema;
pub mod prelude {
    pub use super::adapter::prelude::*;
    pub use super::extract::prelude::*;
    pub use super::routes::inbox_routes;
}
