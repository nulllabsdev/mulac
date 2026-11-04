use super::adapter::prelude::CommandingAdapter;
use crate::http::prelude::AsyncCommandingService;
use mulac_http::prelude::poem::{RunRequest, RunResponse};
use poem::Result;
use poem_openapi::OpenApi;
use poem_openapi::payload::Json;

/// Reusable OpenAPI endpoint exposing `POST /run` for queued commands.
///
/// The host chooses the mount prefix and applies authentication, authorization,
/// and middleware. Set the OpenAPI server URL to match that prefix.
///
/// ```ignore
/// let api = OpenApiService::new(CommandingApi::new(kernel.command_consumer()), "commands", "1")
///     .server("/commands");
/// let app = Route::new().nest("/commands", api);
/// ```
pub struct CommandingApi {
    adapter: CommandingAdapter,
}

impl CommandingApi {
    pub fn new(service: impl Into<AsyncCommandingService>) -> Self {
        Self {
            adapter: CommandingAdapter::new(service),
        }
    }
}

#[OpenApi]
impl CommandingApi {
    /// Execute one batch of due commands, reporting command failures in the response.
    #[oai(path = "/run", method = "post")]
    async fn run_commands(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        self.adapter.run(request).await
    }
}
