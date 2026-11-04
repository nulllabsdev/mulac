pub mod prelude {
    pub use super::{
        InboxAdapter,
        InboxRunResponse,
        InboxSubmission,
        //
    };
}

use super::extract::prelude::{
    InboxHttpError,
    RunInbox,
    SubmitInbox,
    //
};
use crate::http::prelude::{
    AsyncInboxService,
    RunResponse,
    SubmitInboxResponse,
    //
};
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// `202 Accepted` with the stored message ID.
#[derive(Debug, Clone)]
pub struct InboxSubmission(pub SubmitInboxResponse);

impl IntoResponse for InboxSubmission {
    fn into_response(self) -> Response {
        (StatusCode::ACCEPTED, Json(self.0)).into_response()
    }
}

/// `200 OK` with the batch result, including consumer failures.
#[derive(Debug, Clone)]
pub struct InboxRunResponse(pub RunResponse);

impl IntoResponse for InboxRunResponse {
    fn into_response(self) -> Response {
        Json(self.0).into_response()
    }
}

/// Executes inbox routes declared by the application. Cheap to clone, so it
/// can be router state.
///
/// ```ignore
/// async fn submit_inbox(
///     State(inbox): State<InboxAdapter>,
///     request: SubmitInbox,
/// ) -> Result<InboxSubmission, InboxHttpError> {
///     inbox.submit(request).await
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
    pub async fn submit(&self, request: SubmitInbox) -> Result<InboxSubmission, InboxHttpError> {
        let response = self.service.submit(request.0).await?;
        Ok(InboxSubmission(response))
    }

    /// Queue commands for one batch; consumer failures are reported in a `200`
    /// response.
    pub async fn run(&self, request: RunInbox) -> Result<InboxRunResponse, InboxHttpError> {
        let response = self.service.run(request.0).await?;
        Ok(InboxRunResponse(response))
    }
}
