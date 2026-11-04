mod conversions;
mod entity;
mod models;
mod repository;
mod reservation;
mod schema;
#[cfg(test)]
mod tests;
pub mod prelude {
    pub use super::repository::DieselPgOutboxRepository;
    pub use mulac_diesel::{DbPool, build_pool};
}
