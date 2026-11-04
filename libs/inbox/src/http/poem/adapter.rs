pub mod prelude {
    pub use super::{
        InboxAdapter,
        InboxSubmission,
        //
    };
}

use super::prelude::{
    RunRequest,
    RunResponse,
    SubmitInboxRequest,
    SubmitInboxResponse,
    //
};
use crate::http::prelude::{
    AsyncInboxService,
    InboxApiError,
    //
};
use poem::{Error, Result, http::StatusCode};
use poem_openapi::{ApiResponse, payload::Json};

#[derive(ApiResponse)]
pub enum InboxSubmission {
    /// Message stored; duplicate IDs preserve the original message.
    #[oai(status = 202)]
    Accepted(Json<SubmitInboxResponse>),
}

/// Executes inbox endpoints declared by the application.
///
/// ```ignore
/// #[OpenApi]
/// impl InboxApi {
///     #[oai(path = "/inbox", method = "post")]
///     async fn submit(&self, request: Json<SubmitInboxRequest>) -> Result<InboxSubmission> {
///         self.adapter.submit(request).await
///     }
///
///     #[oai(path = "/inbox/run", method = "post")]
///     async fn run(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
///         self.adapter.run(request).await
///     }
/// }
/// ```
#[derive(Clone)]
pub struct InboxAdapter {
    service: AsyncInboxService,
}

impl InboxAdapter {
    pub fn new(service: impl Into<AsyncInboxService>) -> Self {
        Self {
            service: service.into(),
        }
    }

    /// Store one message: `202` on success, `400` for invalid values, `500`
    /// with a sanitized message when storage or the blocking task fails.
    pub async fn submit(&self, request: Json<SubmitInboxRequest>) -> Result<InboxSubmission> {
        let response = self
            .service
            .submit(request.0.0)
            .await
            .map_err(err_inbox_failed)?;
        Ok(InboxSubmission::Accepted(Json(SubmitInboxResponse(
            response,
        ))))
    }

    /// Queue commands for one batch; consumer failures are reported in a `200`
    /// response.
    pub async fn run(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        let request =
            crate::http::prelude::RunRequest::new(request.0.limit).map_err(err_inbox_failed)?;
        let response = self.service.run(request).await.map_err(err_inbox_failed)?;
        Ok(Json(RunResponse(
            mulac_http::prelude::poem::RunResponseData {
                processed: response.processed,
                errors: response.errors,
            },
        )))
    }
}

fn err_inbox_failed(error: InboxApiError) -> Error {
    let status = if error.is_client_error() {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    Error::new(error, status)
}
