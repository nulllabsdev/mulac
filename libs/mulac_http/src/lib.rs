#[cfg(feature = "axum")]
mod body;
mod error_report;
#[cfg(feature = "poem")]
mod poem;
#[cfg(feature = "axum-openapi")]
mod schema;
pub mod prelude {
    #[cfg(feature = "axum")]
    pub use crate::body::prelude as axum;
    pub use crate::error_report::prelude::*;
    #[cfg(feature = "poem")]
    pub use crate::poem::prelude as poem;
    #[cfg(feature = "axum-openapi")]
    pub use crate::schema::prelude as schema;
}
