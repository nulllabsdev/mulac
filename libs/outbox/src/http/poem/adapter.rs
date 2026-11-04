pub mod prelude {
    pub use super::OutboxAdapter;
}

use crate::http::prelude::{
    AsyncOutboxService,
    OutboxApiError,
    OutboxApiErrorKind,
    RunOutboxRequest,
    RunOutboxResponse,
    //
};
use mulac_http::prelude::poem::{
    RunRequest,
    RunResponse,
    //
};
use poem::{Error, Result, http::StatusCode};
use poem_openapi::payload::Json;

/// Executes the outbox batch endpoint declared by the application.
///
/// It reuses the schema-owning [`RunRequest`] and [`RunResponse`] wrappers, so
/// the OpenAPI document keeps one `RunRequest` and one `RunResponse` schema.
///
/// ```ignore
/// #[OpenApi]
/// impl OutboxApi {
///     #[oai(path = "/outbox/run", method = "post")]
///     async fn run_outbox(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
///         self.adapter.run(request).await
///     }
/// }
/// ```
#[derive(Clone)]
pub struct OutboxAdapter {
    service: AsyncOutboxService,
}

impl OutboxAdapter {
    pub fn new(service: impl Into<AsyncOutboxService>) -> Self {
        Self {
            service: service.into(),
        }
    }

    /// Delivery is not configured: valid requests get a `503`.
    pub fn unconfigured() -> Self {
        Self::new(AsyncOutboxService::unconfigured())
    }

    /// Publish one batch of due entries; publication failures are reported in
    /// a `200` response, a missing publisher is a `503` and a failed blocking
    /// task is a sanitized `500`.
    pub async fn run(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        let request = RunOutboxRequest::try_from(request.0).map_err(err_outbox_failed)?;
        let response = self.service.run(request).await.map_err(err_outbox_failed)?;
        Ok(Json(response.into()))
    }
}

impl TryFrom<RunRequest> for RunOutboxRequest {
    type Error = OutboxApiError;

    fn try_from(request: RunRequest) -> Result<Self, Self::Error> {
        Self::new(request.limit)
    }
}

impl From<RunOutboxResponse> for RunResponse {
    fn from(response: RunOutboxResponse) -> Self {
        Self(mulac_http::prelude::poem::RunResponseData {
            processed: response.processed,
            errors: response.errors,
        })
    }
}

fn err_outbox_failed(error: OutboxApiError) -> Error {
    let status = match error.kind() {
        OutboxApiErrorKind::Invalid => StatusCode::BAD_REQUEST,
        OutboxApiErrorKind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        OutboxApiErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    };
    Error::new(error, status)
}
