//! Axum service with application-owned Utoipa path annotations.
//!
//! ```sh
//! DATABASE_URL=postgres://... cargo run -p mulac_axum --features utoipa --example axum_openapi
//! ```
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use inbox_api::io::InboxService;
use mulac_axum::io::{
    InboxAdapter,
    InboxHttpError,
    InboxRunResponse,
    InboxSubmission,
    RunInbox,
    SubmitInbox,
    //
};
use utoipa::OpenApi;

/// Store one message without processing it. Unknown routing keys are accepted.
#[utoipa::path(
    post,
    path = "/inbox",
    request_body = SubmitInbox,
    responses(
        (status = 202, body = InboxSubmission),
        (status = 400, body = String),
        (status = 500, body = String),
    ),
)]
async fn submit_inbox(
    State(inbox): State<InboxAdapter>,
    request: SubmitInbox,
) -> Result<InboxSubmission, InboxHttpError> {
    inbox.submit(request).await
}

/// Queue commands from stored inbox messages, without executing them.
#[utoipa::path(
    post,
    path = "/inbox/run",
    request_body = RunInbox,
    responses((status = 200, body = InboxRunResponse), (status = 400, body = String)),
)]
async fn run_inbox(
    State(inbox): State<InboxAdapter>,
    request: RunInbox,
) -> Result<InboxRunResponse, InboxHttpError> {
    inbox.run(request).await
}

#[derive(OpenApi)]
#[openapi(paths(submit_inbox, run_inbox), servers((url = "/api")))]
struct ApiDoc;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    let pool = inbox::io::build_pool(&database_url).map_err(std::io::Error::other)?;
    let inbox_adapter = InboxAdapter::new(InboxService::from_postgres(pool));

    let inbox_routes = Router::new()
        .route("/inbox", post(submit_inbox))
        .route("/inbox/run", post(run_inbox))
        .with_state(inbox_adapter);
    let app = Router::new()
        .route(
            "/api/openapi.json",
            get(|| async { Json(ApiDoc::openapi()) }),
        )
        .nest("/api", inbox_routes);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    axum::serve(listener, app).await
}
