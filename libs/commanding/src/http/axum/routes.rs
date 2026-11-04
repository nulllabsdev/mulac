use super::adapter::prelude::{
    CommandingAdapter, CommandingHttpError, CommandsRunResponse, RunCommands,
};
use crate::http::prelude::AsyncCommandingService;
use axum::Router;
use axum::extract::State;
use axum::routing::post;

/// Build command-processing routes with `POST /run`.
///
/// Mount under an application-owned prefix (for example `/commands`). The host
/// must apply its own authentication, authorization, and other middleware.
/// This processes queued commands; it does not submit new commands.
///
/// ```ignore
/// let app = Router::new().nest("/commands", command_routes(kernel.command_consumer()));
/// ```
pub fn command_routes(service: impl Into<AsyncCommandingService>) -> Router {
    Router::new()
        .route("/run", post(run_commands))
        .with_state(CommandingAdapter::new(service))
}

async fn run_commands(
    State(commands): State<CommandingAdapter>,
    request: RunCommands,
) -> Result<CommandsRunResponse, CommandingHttpError> {
    commands.run(request).await
}
