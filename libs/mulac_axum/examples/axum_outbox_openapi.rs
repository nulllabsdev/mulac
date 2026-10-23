//! Axum outbox batch route with an application-owned Utoipa annotation.
//!
//! The application owns the publisher. This example uses a test double
//! from `outbox_api`'s `testing` feature; a real service passes its pool and
//! publisher to `OutboxService::from_postgres`, or uses
//! `OutboxAdapter::unconfigured()` to answer `503` while delivery is off.
//!
//! ```sh
//! cargo run -p mulac_axum --features outbox,utoipa --example axum_outbox_openapi
//! ```
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use mulac_axum::io::{
    OutboxAdapter,
    OutboxHttpError,
    OutboxRunResponse,
    RunOutbox,
    //
};
use outbox_api::io::testing;
use utoipa::OpenApi;

/// Publish one batch; returns 503 when delivery is not configured.
#[utoipa::path(
    post,
    path = "/outbox/run",
    request_body = RunOutbox,
    responses(
        (status = 200, body = OutboxRunResponse),
        (status = 400, body = String),
        (status = 500, body = String),
        (status = 503, body = String),
    ),
)]
async fn run_outbox(
    State(outbox): State<OutboxAdapter>,
    request: RunOutbox,
) -> Result<OutboxRunResponse, OutboxHttpError> {
    outbox.run(request).await
}

#[derive(OpenApi)]
#[openapi(paths(run_outbox), servers((url = "/api")))]
struct ApiDoc;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let outbox = OutboxAdapter::new(testing::queued_service(10));
    let routes = Router::new()
        .route("/outbox/run", post(run_outbox))
        .with_state(outbox);
    let app = Router::new()
        .route(
            "/api/openapi.json",
            get(|| async { Json(ApiDoc::openapi()) }),
        )
        .nest("/api", routes);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    axum::serve(listener, app).await
}
