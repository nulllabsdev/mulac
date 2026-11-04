use super::adapter::prelude::EventingAdapter;
use crate::http::prelude::AsyncEventingService;
use mulac_http::prelude::poem::{RunRequest, RunResponse};
use poem::Result;
use poem_openapi::OpenApi;
use poem_openapi::payload::Json;

/// Reusable OpenAPI endpoint exposing `POST /run` for queued events.
///
/// The host chooses the mount prefix and applies authentication, authorization,
/// and middleware. Set the OpenAPI server URL to match that prefix.
///
/// ```ignore
/// let api = OpenApiService::new(EventingApi::new(kernel.event_consumer()), "events", "1")
///     .server("/events");
/// let app = Route::new().nest("/events", api);
/// ```
pub struct EventingApi {
    adapter: EventingAdapter,
}

impl EventingApi {
    pub fn new(service: impl Into<AsyncEventingService>) -> Self {
        Self {
            adapter: EventingAdapter::new(service),
        }
    }
}

#[OpenApi]
impl EventingApi {
    /// Dispatch one batch of due events, reporting event failures in the response.
    #[oai(path = "/run", method = "post")]
    async fn run_eventing(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        self.adapter.run(request).await
    }
}
