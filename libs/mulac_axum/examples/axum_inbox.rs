//! Axum service exposing the inbox routes under `/api`, without OpenAPI.
//!
//! ```sh
//! DATABASE_URL=postgres://... cargo run -p mulac_axum --example axum_inbox
//! ```
use axum::Router;
use axum::extract::State;
use axum::routing::post;
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

async fn submit_inbox(
    State(inbox): State<InboxAdapter>,
    request: SubmitInbox,
) -> Result<InboxSubmission, InboxHttpError> {
    inbox.submit(request).await
}

async fn run_inbox(
    State(inbox): State<InboxAdapter>,
    request: RunInbox,
) -> Result<InboxRunResponse, InboxHttpError> {
    inbox.run(request).await
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    let pool = inbox::io::build_pool(&database_url).map_err(std::io::Error::other)?;
    let inbox_adapter = InboxAdapter::new(InboxService::from_postgres(pool));

    let inbox_routes = Router::new()
        .route("/inbox", post(submit_inbox))
        .route("/inbox/run", post(run_inbox))
        .with_state(inbox_adapter);
    let app = Router::new().nest("/api", inbox_routes);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    axum::serve(listener, app).await
}
