//! Axum service exposing the event batch route under `/api`, without OpenAPI.
//!
//! The application owns the event consumer. This example uses a test double
//! from `mulac-eventing`'s `api-testing` feature; a real service passes
//! `PersistentKernelHandle::event_consumer()` to `event_routes`.
//!
//! ```sh
//! cargo run -p mulac-eventing --features axum,api-testing --example axum_eventing
//! ```
use axum::Router;
use mulac_eventing::prelude::axum::event_routes;
use mulac_eventing::prelude::testing;
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let app = Router::new().nest("/api/events", event_routes(testing::queued_consumer(10)));

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    axum::serve(listener, app).await
}
