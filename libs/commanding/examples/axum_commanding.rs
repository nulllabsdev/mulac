//! Axum service exposing the command batch route under `/api`, without OpenAPI.
//!
//! The application owns the command consumer. This example uses a test double
//! from `mulac-commanding`'s `api-testing` feature; a real service passes
//! `PersistentKernelHandle::command_consumer()` to `command_routes`.
//!
//! ```sh
//! cargo run -p mulac-commanding --features axum,api-testing --example axum_commanding
//! ```
use axum::Router;
use mulac_commanding::prelude::axum::command_routes;
use mulac_commanding::prelude::testing;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let app = Router::new().nest(
        "/api/commands",
        command_routes(testing::queued_consumer(10)),
    );

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    axum::serve(listener, app).await
}
