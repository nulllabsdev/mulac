mod conversions;
mod entity;
mod models;
mod repository;
mod reservation;
mod schema;
pub mod prelude {
    pub use super::repository::DieselPgInboxRepository;
    pub use mulac_diesel::{DbPool, build_pool};
}
