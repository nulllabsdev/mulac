pub mod io {
    pub use super::{
        OutboxAdapter,
        OutboxHttpError,
        OutboxRunResponse,
        RunOutbox,
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
use outbox_api::io::{
    AsyncOutboxService,
    OutboxApiError,
    OutboxApiErrorKind,
    RunOutboxRequest,
    RunOutboxResponse,
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::extract::{DefaultBodyLimit, State};
    use axum::http::{Request, header};
    use axum::routing::post;
    use http_body_util::BodyExt;
    use outbox_api::io::testing;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    async fn run_outbox(
        State(outbox): State<OutboxAdapter>,
        request: RunOutbox,
    ) -> Result<OutboxRunResponse, OutboxHttpError> {
        outbox.run(request).await
    }

    fn app(outbox: OutboxAdapter) -> Router {
        let routes = Router::new()
            .route("/outbox/run", post(run_outbox))
            .with_state(outbox);
        Router::new().nest("/api", routes)
    }

    async fn send(
        app: &Router,
        content_type: Option<&str>,
        body: impl Into<Body>,
    ) -> (StatusCode, String) {
        let mut request = Request::post("/api/outbox/run");
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
        let app = app(OutboxAdapter::new(testing::empty_service()));
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
    async fn reports_counts_and_publication_failures() {
        let app_with = |service| app(OutboxAdapter::new(service));
        let (status, text) =
            send_json(&app_with(testing::queued_service(3)), &json!({"limit":2})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json(&text), json!({"processed":2,"errors":[]}));
        for (service, errors) in [
            (
                testing::failing_reservation_service(),
                json!([testing::RESERVATION_FAILURE]),
            ),
            (
                testing::failing_publisher_and_process_service(),
                json!([testing::PROCESS_FAILURE, testing::PUBLISH_FAILURE]),
            ),
        ] {
            let (status, text) = send_json(&app_with(service), &json!({"limit":1})).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(json(&text), json!({"processed":null,"errors":errors}));
        }
    }

    #[tokio::test]
    async fn keeps_published_entries_of_a_partial_batch() {
        let (service, outcomes) = testing::partially_failing_service();
        let (status, text) =
            send_json(&app(OutboxAdapter::new(service)), &json!({"limit":5})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            json(&text),
            json!({"processed":null,"errors":[testing::PUBLISH_FAILURE]})
        );
        assert_eq!(
            *outcomes.lock().unwrap(),
            vec!["completed", "failed", "completed"]
        );
    }

    #[tokio::test]
    async fn unconfigured_publishers_are_unavailable_after_validation() {
        let app = app(OutboxAdapter::unconfigured());
        for body in testing::valid_run_requests() {
            let (status, text) = send_json(&app, &body).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
            assert_eq!(text, "outbox publisher is not configured");
        }
        for body in testing::invalid_run_requests() {
            let (status, _) = send_json(&app, &body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        }
    }

    #[tokio::test]
    async fn sanitizes_task_failures() {
        let (status, text) = send_json(
            &app(OutboxAdapter::new(testing::panicking_service())),
            &json!({"limit":1}),
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(text, "queue processing task failed");
    }

    #[tokio::test]
    async fn preserves_content_type_and_body_limit_rejections() {
        let app = app(OutboxAdapter::new(testing::empty_service()));
        let (status, _) = send(&app, None, r#"{"limit":1}"#).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let (status, _) = send(&app, Some("text/plain"), r#"{"limit":1}"#).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let limited = app.layer(DefaultBodyLimit::max(8));
        let (status, _) = send_json(&limited, &json!({"limit":1,"extra":true})).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    }
}
