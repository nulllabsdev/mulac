//! Axum command batch route with an application-owned Utoipa annotation.
//!
//! The application owns the command consumer. This example uses a test double
//! from `commanding_api`'s `testing` feature; a real service passes
//! `PersistentKernelHandle::command_consumer()` to `CommandingService::new`.
//!
//! ```sh
//! cargo run -p mulac_axum --features commanding,utoipa --example axum_commanding_openapi
//! ```
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use commanding_api::io::testing;
use mulac_axum::io::{
    CommandingAdapter,
    CommandingHttpError,
    CommandsRunResponse,
    RunCommands,
    //
};
use utoipa::OpenApi;

/// Execute one batch of due commands; resulting events remain queued.
#[utoipa::path(
    post,
    path = "/commands/run",
    request_body = RunCommands,
    responses(
        (status = 200, body = CommandsRunResponse),
        (status = 400, body = String),
        (status = 500, body = String),
    ),
)]
async fn run_commands(
    State(commands): State<CommandingAdapter>,
    request: RunCommands,
) -> Result<CommandsRunResponse, CommandingHttpError> {
    commands.run(request).await
}

#[derive(OpenApi)]
#[openapi(paths(run_commands), servers((url = "/api")))]
struct ApiDoc;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let commands = CommandingAdapter::new(testing::queued_service(10));
    let routes = Router::new()
        .route("/commands/run", post(run_commands))
        .with_state(commands);
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
