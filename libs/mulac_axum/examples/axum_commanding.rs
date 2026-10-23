//! Axum service exposing the command batch route under `/api`, without OpenAPI.
//!
//! The application owns the command consumer. This example uses a test double
//! from `commanding_api`'s `testing` feature; a real service passes
//! `PersistentKernelHandle::command_consumer()` to `CommandingService::new`.
//!
//! ```sh
//! cargo run -p mulac_axum --features commanding --example axum_commanding
//! ```
use axum::Router;
use axum::extract::State;
use axum::routing::post;
use commanding_api::io::testing;
use mulac_axum::io::{
    CommandingAdapter,
    CommandingHttpError,
    CommandsRunResponse,
    RunCommands,
    //
};

async fn run_commands(
    State(commands): State<CommandingAdapter>,
    request: RunCommands,
) -> Result<CommandsRunResponse, CommandingHttpError> {
    commands.run(request).await
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let commands = CommandingAdapter::new(testing::queued_service(10));
    let routes = Router::new()
        .route("/commands/run", post(run_commands))
        .with_state(commands);
    let app = Router::new().nest("/api", routes);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    axum::serve(listener, app).await
}
