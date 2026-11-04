pub mod prelude {
    pub use super::EventingAdapter;
}

use crate::http::prelude::{
    AsyncEventingService,
    EventingApiError,
    RunEventsRequest,
    RunEventsResponse,
    //
};
use mulac_http::prelude::poem::{
    RunRequest,
    RunResponse,
    //
};
use poem::{Error, Result, http::StatusCode};
use poem_openapi::payload::Json;

/// Executes the event batch endpoint declared by the application.
///
/// It reuses the schema-owning [`RunRequest`] and [`RunResponse`] wrappers, so
/// the OpenAPI document keeps one `RunRequest` and one `RunResponse` schema.
///
/// ```ignore
/// #[OpenApi]
/// impl EventingApi {
///     #[oai(path = "/events/run", method = "post")]
///     async fn run_events(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
///         self.adapter.run(request).await
///     }
/// }
/// ```
#[derive(Clone)]
pub struct EventingAdapter {
    service: AsyncEventingService,
}

impl EventingAdapter {
    pub fn new(service: impl Into<AsyncEventingService>) -> Self {
        Self {
            service: service.into(),
        }
    }

    /// Dispatch one batch of due events; event failures are reported in a
    /// `200` response, and a failed blocking task is a sanitized `500`.
    pub async fn run(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        let request = RunEventsRequest::try_from(request.0).map_err(err_eventing_failed)?;
        let response = self
            .service
            .run(request)
            .await
            .map_err(err_eventing_failed)?;
        Ok(Json(response.into()))
    }
}

impl TryFrom<RunRequest> for RunEventsRequest {
    type Error = EventingApiError;

    fn try_from(request: RunRequest) -> Result<Self, Self::Error> {
        Self::new(request.limit)
    }
}

impl From<RunEventsResponse> for RunResponse {
    fn from(response: RunEventsResponse) -> Self {
        Self(mulac_http::prelude::poem::RunResponseData {
            processed: response.processed,
            errors: response.errors,
        })
    }
}

fn err_eventing_failed(error: EventingApiError) -> Error {
    let status = if error.is_client_error() {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    Error::new(error, status)
}
