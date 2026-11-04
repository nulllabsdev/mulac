pub mod prelude {
    pub use super::{
        EventingAdapter,
        EventingHttpError,
        EventsRunResponse,
        RunEvents,
        //
    };
}

use crate::http::prelude::{
    AsyncEventingService,
    EventingApiError,
    RunEventsRequest,
    RunEventsResponse,
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

/// Event batch body extractor; rejects limits outside `1..=1000`.
#[derive(Debug, Clone, Copy)]
pub struct RunEvents(pub RunEventsRequest);

impl<S: Send + Sync> FromRequest<S> for RunEvents {
    type Rejection = EventingHttpError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let value = json_body(req, state).await.map_err(err_unreadable_body)?;
        RunEventsRequest::from_json(value)
            .map(Self)
            .map_err(EventingHttpError::from)
    }
}

/// `200 OK` with the batch result, including event failures.
#[derive(Debug, Clone)]
pub struct EventsRunResponse(pub RunEventsResponse);

impl IntoResponse for EventsRunResponse {
    fn into_response(self) -> Response {
        Json(self.0).into_response()
    }
}

/// Executes the event batch route declared by the application. Cheap to
/// clone, so it can be router state.
///
/// ```ignore
/// async fn run_events(
///     State(events): State<EventingAdapter>,
///     request: RunEvents,
/// ) -> Result<EventsRunResponse, EventingHttpError> {
///     events.run(request).await
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
    pub async fn run(&self, request: RunEvents) -> Result<EventsRunResponse, EventingHttpError> {
        let response = self.service.run(request.0).await?;
        Ok(EventsRunResponse(response))
    }
}

/// Eventing failure rendered as a plain-text response.
#[derive(Debug)]
pub enum EventingHttpError {
    /// Axum could not read a JSON body (syntax, content type, body limit).
    Body(JsonRejection),
    /// Invalid request values, or a sanitized server failure.
    Eventing(EventingApiError),
}

impl From<EventingApiError> for EventingHttpError {
    fn from(error: EventingApiError) -> Self {
        Self::Eventing(error)
    }
}

impl fmt::Display for EventingHttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Body(rejection) => rejection.fmt(f),
            Self::Eventing(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for EventingHttpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Body(rejection) => Some(rejection),
            Self::Eventing(error) => Some(error),
        }
    }
}

impl IntoResponse for EventingHttpError {
    fn into_response(self) -> Response {
        match self {
            Self::Body(rejection) => rejection_response(rejection),
            Self::Eventing(error) => (eventing_status(&error), error.to_string()).into_response(),
        }
    }
}

fn eventing_status(error: &EventingApiError) -> StatusCode {
    if error.is_client_error() {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

fn err_unreadable_body(rejection: JsonRejection) -> EventingHttpError {
    EventingHttpError::Body(rejection)
}
