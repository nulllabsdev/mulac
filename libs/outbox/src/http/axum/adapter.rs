pub mod prelude {
    pub use super::{
        OutboxAdapter,
        OutboxHttpError,
        OutboxRunResponse,
        RunOutbox,
        //
    };
}

use crate::http::prelude::{
    AsyncOutboxService,
    OutboxApiError,
    OutboxApiErrorKind,
    RunOutboxRequest,
    RunOutboxResponse,
    //
};
use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use mulac_http::prelude::axum::{
    json_body,
    rejection_response,
    //
};
use std::fmt;

/// Outbox batch body extractor; rejects limits outside `1..=1000`.
#[derive(Debug, Clone, Copy)]
pub struct RunOutbox(pub RunOutboxRequest);

impl<S: Send + Sync> FromRequest<S> for RunOutbox {
    type Rejection = OutboxHttpError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let value = json_body(req, state).await.map_err(err_unreadable_body)?;
        RunOutboxRequest::from_json(value)
            .map(Self)
            .map_err(OutboxHttpError::from)
    }
}

/// `200 OK` with the batch result, including publication failures.
#[derive(Debug, Clone)]
pub struct OutboxRunResponse(pub RunOutboxResponse);

impl IntoResponse for OutboxRunResponse {
    fn into_response(self) -> Response {
        Json(self.0).into_response()
    }
}

/// Executes the outbox batch route declared by the application. Cheap to
/// clone, so it can be router state.
///
/// ```ignore
/// async fn run_outbox(
///     State(outbox): State<OutboxAdapter>,
///     request: RunOutbox,
/// ) -> Result<OutboxRunResponse, OutboxHttpError> {
///     outbox.run(request).await
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
    pub async fn run(&self, request: RunOutbox) -> Result<OutboxRunResponse, OutboxHttpError> {
        let response = self.service.run(request.0).await?;
        Ok(OutboxRunResponse(response))
    }
}

/// Outbox failure rendered as a plain-text response.
#[derive(Debug)]
pub enum OutboxHttpError {
    /// Axum could not read a JSON body (syntax, content type, body limit).
    Body(JsonRejection),
    /// Invalid request values, a missing publisher, or a sanitized server
    /// failure.
    Outbox(OutboxApiError),
}

impl From<OutboxApiError> for OutboxHttpError {
    fn from(error: OutboxApiError) -> Self {
        Self::Outbox(error)
    }
}

impl fmt::Display for OutboxHttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Body(rejection) => rejection.fmt(f),
            Self::Outbox(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for OutboxHttpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Body(rejection) => Some(rejection),
            Self::Outbox(error) => Some(error),
        }
    }
}

impl IntoResponse for OutboxHttpError {
    fn into_response(self) -> Response {
        match self {
            Self::Body(rejection) => rejection_response(rejection),
            Self::Outbox(error) => (outbox_status(&error), error.to_string()).into_response(),
        }
    }
}

fn outbox_status(error: &OutboxApiError) -> StatusCode {
    match error.kind() {
        OutboxApiErrorKind::Invalid => StatusCode::BAD_REQUEST,
        OutboxApiErrorKind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        OutboxApiErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn err_unreadable_body(rejection: JsonRejection) -> OutboxHttpError {
    OutboxHttpError::Body(rejection)
}
