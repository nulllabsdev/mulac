pub mod io {
    pub use super::{
        InboxHttpError,
        RunInbox,
        SubmitInbox,
        //
    };
}

use crate::body::io::{
    json_body,
    rejection_response,
    //
};
use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use inbox_api::io::{
    InboxApiError,
    RunRequest,
    SubmitInboxRequest,
    //
};
use std::fmt;

/// Submission body extractor. Checks field types only; `InboxAdapter::submit`
/// validates values so their messages are returned verbatim.
#[derive(Debug, Clone)]
pub struct SubmitInbox(pub SubmitInboxRequest);

/// Batch body extractor; rejects limits outside `1..=1000`.
#[derive(Debug, Clone, Copy)]
pub struct RunInbox(pub RunRequest);

impl<S: Send + Sync> FromRequest<S> for SubmitInbox {
    type Rejection = InboxHttpError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let value = json_body(req, state).await.map_err(err_unreadable_body)?;
        SubmitInboxRequest::parse_json(value)
            .map(Self)
            .map_err(InboxHttpError::from)
    }
}

impl<S: Send + Sync> FromRequest<S> for RunInbox {
    type Rejection = InboxHttpError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let value = json_body(req, state).await.map_err(err_unreadable_body)?;
        RunRequest::from_json(value)
            .map(Self)
            .map_err(InboxHttpError::from)
    }
}

/// Inbox failure rendered as a plain-text response.
#[derive(Debug)]
pub enum InboxHttpError {
    /// Axum could not read a JSON body (syntax, content type, body limit).
    Body(JsonRejection),
    /// Invalid request values, or a sanitized server failure.
    Inbox(InboxApiError),
}

impl From<InboxApiError> for InboxHttpError {
    fn from(error: InboxApiError) -> Self {
        Self::Inbox(error)
    }
}

impl fmt::Display for InboxHttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Body(rejection) => rejection.fmt(f),
            Self::Inbox(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for InboxHttpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Body(rejection) => Some(rejection),
            Self::Inbox(error) => Some(error),
        }
    }
}

impl IntoResponse for InboxHttpError {
    fn into_response(self) -> Response {
        match self {
            Self::Body(rejection) => rejection_response(rejection),
            Self::Inbox(error) => (inbox_status(&error), error.to_string()).into_response(),
        }
    }
}

fn inbox_status(error: &InboxApiError) -> StatusCode {
    if error.is_client_error() {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

fn err_unreadable_body(rejection: JsonRejection) -> InboxHttpError {
    InboxHttpError::Body(rejection)
}
