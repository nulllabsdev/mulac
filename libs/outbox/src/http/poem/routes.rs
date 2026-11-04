use super::adapter::prelude::OutboxAdapter;
use crate::http::prelude::AsyncOutboxService;
use mulac_http::prelude::poem::{RunRequest, RunResponse};
use poem::Result;
use poem_openapi::OpenApi;
use poem_openapi::payload::Json;

/// Reusable OpenAPI endpoint exposing `POST /run` for queued outbox entries.
///
/// The host chooses the mount prefix and applies authentication, authorization,
/// and middleware. Set the OpenAPI server URL to match that prefix.
///
/// ```ignore
/// let consumer = Arc::new(OutboxConsumer::new(repository, publisher));
/// let api = OpenApiService::new(OutboxApi::new(consumer), "outbox", "1")
///     .server("/outbox");
/// let app = Route::new().nest("/outbox", api);
/// ```
pub struct OutboxApi {
    adapter: OutboxAdapter,
}

impl OutboxApi {
    pub fn new(service: impl Into<AsyncOutboxService>) -> Self {
        Self {
            adapter: OutboxAdapter::new(service),
        }
    }
}

#[OpenApi]
impl OutboxApi {
    /// Publish one batch of due entries, reporting publication failures in the response.
    #[oai(path = "/run", method = "post")]
    async fn run_outbox(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        self.adapter.run(request).await
    }
}
