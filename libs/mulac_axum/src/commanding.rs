pub mod io {
    pub use super::{
        CommandingAdapter,
        CommandingHttpError,
        CommandsRunResponse,
        RunCommands,
        //
    };
}

use crate::body::io::{
    json_body,
    rejection_response,
    //
};
use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use commanding_api::io::{
    AsyncCommandingService,
    CommandingApiError,
    RunCommandsRequest,
    RunCommandsResponse,
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::extract::{DefaultBodyLimit, State};
    use axum::http::{Request, header};
    use axum::routing::post;
    use commanding_api::io::{CommandingService, testing};
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    async fn run_commands(
        State(commands): State<CommandingAdapter>,
        request: RunCommands,
    ) -> Result<CommandsRunResponse, CommandingHttpError> {
        commands.run(request).await
    }

    fn app(service: CommandingService) -> Router {
        let routes = Router::new()
            .route("/commands/run", post(run_commands))
            .with_state(CommandingAdapter::new(service));
        Router::new().nest("/api", routes)
    }

    async fn send(
        app: &Router,
        content_type: Option<&str>,
        body: impl Into<Body>,
    ) -> (StatusCode, String) {
        let mut request = Request::post("/api/commands/run");
        if let Some(content_type) = content_type {
            request = request.header(header::CONTENT_TYPE, content_type);
        }
        let response = app
            .clone()
            .oneshot(request.body(body.into()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    async fn send_json(app: &Router, body: &Value) -> (StatusCode, String) {
        send(app, Some("application/json"), body.to_string()).await
    }

    fn json(text: &str) -> Value {
        serde_json::from_str(text).unwrap()
    }

    #[tokio::test]
    async fn runs_batches_from_the_shared_run_request_table() {
        let app = app(testing::empty_service());
        for body in testing::valid_run_requests() {
            let (status, text) = send_json(&app, &body).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(json(&text), json!({"processed":0,"errors":[]}));
        }
        for body in testing::invalid_run_requests() {
            let (status, _) = send_json(&app, &body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        }
        for (body, message) in testing::run_request_error_messages() {
            let (status, text) = send_json(&app, &body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(text, message);
        }
        let (status, _) = send(&app, Some("application/json"), "{").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn reports_counts_and_command_failures() {
        let (status, text) = send_json(&app(testing::queued_service(3)), &json!({"limit":2})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json(&text), json!({"processed":2,"errors":[]}));
        for (service, errors) in [
            (
                testing::failing_reservation_service(),
                json!([testing::RESERVATION_FAILURE]),
            ),
            (
                testing::failing_handler_and_process_service(),
                json!([testing::PROCESS_FAILURE, testing::HANDLER_FAILURE]),
            ),
        ] {
            let (status, text) = send_json(&app(service), &json!({"limit":1})).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(json(&text), json!({"processed":null,"errors":errors}));
        }
    }

    #[tokio::test]
    async fn sanitizes_task_failures() {
        let (status, text) =
            send_json(&app(testing::panicking_service()), &json!({"limit":1})).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(text, "queue processing task failed");
    }

    #[tokio::test]
    async fn preserves_content_type_and_body_limit_rejections() {
        let app = app(testing::empty_service());
        let (status, _) = send(&app, None, r#"{"limit":1}"#).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let (status, _) = send(&app, Some("text/plain"), r#"{"limit":1}"#).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let limited = app.layer(DefaultBodyLimit::max(8));
        let (status, _) = send_json(&limited, &json!({"limit":1,"extra":true})).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    }
}
