//! Axum service exposing the outbox batch route under `/api`, without OpenAPI.
//!
//! The application owns the publisher. This example uses a test double
//! from `outbox_api`'s `testing` feature; a real service passes its pool and
//! publisher to `OutboxService::from_postgres`, or uses
//! `OutboxAdapter::unconfigured()` to answer `503` while delivery is off.
//!
//! ```sh
//! cargo run -p mulac_axum --features outbox --example axum_outbox
//! ```
use axum::Router;
use axum::extract::State;
use axum::routing::post;
use mulac_axum::io::{
    OutboxAdapter,
    OutboxHttpError,
    OutboxRunResponse,
    RunOutbox,
    //
};
use outbox_api::io::testing;

async fn run_outbox(
    State(outbox): State<OutboxAdapter>,
    request: RunOutbox,
) -> Result<OutboxRunResponse, OutboxHttpError> {
    outbox.run(request).await
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let outbox = OutboxAdapter::new(testing::queued_service(10));
    let routes = Router::new()
        .route("/outbox/run", post(run_outbox))
        .with_state(outbox);
    let app = Router::new().nest("/api", routes);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    axum::serve(listener, app).await
}
