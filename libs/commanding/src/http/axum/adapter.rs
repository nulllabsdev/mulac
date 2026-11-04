pub mod prelude {
    pub use super::{
        CommandingAdapter,
        CommandingHttpError,
        CommandsRunResponse,
        RunCommands,
        //
    };
}

use crate::http::prelude::{
    AsyncCommandingService,
    CommandingApiError,
    RunCommandsRequest,
    RunCommandsResponse,
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

/// Command batch body extractor; rejects limits outside `1..=1000`.
#[derive(Debug, Clone, Copy)]
pub struct RunCommands(pub RunCommandsRequest);

impl<S: Send + Sync> FromRequest<S> for RunCommands {
    type Rejection = CommandingHttpError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let value = json_body(req, state).await.map_err(err_unreadable_body)?;
        RunCommandsRequest::from_json(value)
            .map(Self)
            .map_err(CommandingHttpError::from)
    }
}

/// `200 OK` with the batch result, including command failures.
#[derive(Debug, Clone)]
pub struct CommandsRunResponse(pub RunCommandsResponse);

impl IntoResponse for CommandsRunResponse {
    fn into_response(self) -> Response {
        Json(self.0).into_response()
    }
}

/// Executes the command batch route declared by the application. Cheap to
/// clone, so it can be router state.
///
/// ```ignore
/// async fn run_commands(
///     State(commands): State<CommandingAdapter>,
///     request: RunCommands,
/// ) -> Result<CommandsRunResponse, CommandingHttpError> {
///     commands.run(request).await
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
    pub async fn run(
        &self,
        request: RunCommands,
    ) -> Result<CommandsRunResponse, CommandingHttpError> {
        let response = self.service.run(request.0).await?;
        Ok(CommandsRunResponse(response))
    }
}

/// Commanding failure rendered as a plain-text response.
#[derive(Debug)]
pub enum CommandingHttpError {
    /// Axum could not read a JSON body (syntax, content type, body limit).
    Body(JsonRejection),
    /// Invalid request values, or a sanitized server failure.
    Commanding(CommandingApiError),
}

impl From<CommandingApiError> for CommandingHttpError {
    fn from(error: CommandingApiError) -> Self {
        Self::Commanding(error)
    }
}

impl fmt::Display for CommandingHttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Body(rejection) => rejection.fmt(f),
            Self::Commanding(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for CommandingHttpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Body(rejection) => Some(rejection),
            Self::Commanding(error) => Some(error),
        }
    }
}

impl IntoResponse for CommandingHttpError {
    fn into_response(self) -> Response {
        match self {
            Self::Body(rejection) => rejection_response(rejection),
            Self::Commanding(error) => {
                (commanding_status(&error), error.to_string()).into_response()
            }
        }
    }
}

fn commanding_status(error: &CommandingApiError) -> StatusCode {
    if error.is_client_error() {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

fn err_unreadable_body(rejection: JsonRejection) -> CommandingHttpError {
    CommandingHttpError::Body(rejection)
}
