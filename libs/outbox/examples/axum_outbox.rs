//! Axum service exposing the outbox batch route under `/api`, without OpenAPI.
//!
//! The application owns the publisher. This example uses a test double
//! from `mulac-outbox`'s `api-testing` feature; a real service passes its pool and
//! publisher to `AsyncOutboxService::from_postgres`, or uses
//! `OutboxAdapter::unconfigured()` to answer `503` while delivery is off.
//!
//! ```sh
//! cargo run -p mulac-outbox --features axum,api-testing --example axum_outbox
//! ```
use axum::Router;
use mulac_outbox::prelude::axum::outbox_routes;
use mulac_outbox::prelude::testing;
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let app = Router::new().nest("/api/outbox", outbox_routes(testing::queued_consumer(10)));

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    axum::serve(listener, app).await
}
