pub mod prelude {
    pub use super::CommandingAdapter;
}

use crate::http::prelude::{
    AsyncCommandingService,
    CommandingApiError,
    RunCommandsRequest,
    RunCommandsResponse,
    //
};
use mulac_http::prelude::poem::{
    RunRequest,
    RunResponse,
    //
};
use poem::{Error, Result, http::StatusCode};
use poem_openapi::payload::Json;

/// Executes the command batch endpoint declared by the application.
///
/// It reuses the schema-owning [`RunRequest`] and [`RunResponse`] wrappers, so
/// the OpenAPI document keeps one `RunRequest` and one `RunResponse` schema.
///
/// ```ignore
/// #[OpenApi]
/// impl CommandingApi {
///     #[oai(path = "/commands/run", method = "post")]
///     async fn run_commands(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
///         self.adapter.run(request).await
///     }
/// }
/// ```
#[derive(Clone)]
pub struct CommandingAdapter {
    service: AsyncCommandingService,
}

impl CommandingAdapter {
    pub fn new(service: impl Into<AsyncCommandingService>) -> Self {
        Self {
            service: service.into(),
        }
    }

    /// Execute one batch of due commands; command failures are reported in a
    /// `200` response, and a failed blocking task is a sanitized `500`.
    pub async fn run(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        let request = RunCommandsRequest::try_from(request.0).map_err(err_commanding_failed)?;
        let response = self
            .service
            .run(request)
            .await
            .map_err(err_commanding_failed)?;
        Ok(Json(response.into()))
    }
}

impl TryFrom<RunRequest> for RunCommandsRequest {
    type Error = CommandingApiError;

    fn try_from(request: RunRequest) -> Result<Self, Self::Error> {
        Self::new(request.limit)
    }
}

impl From<RunCommandsResponse> for RunResponse {
    fn from(response: RunCommandsResponse) -> Self {
        Self(mulac_http::prelude::poem::RunResponseData {
            processed: response.processed,
            errors: response.errors,
        })
    }
}

fn err_commanding_failed(error: CommandingApiError) -> Error {
    let status = if error.is_client_error() {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    Error::new(error, status)
}
