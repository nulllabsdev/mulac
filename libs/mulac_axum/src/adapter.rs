pub mod io {
    pub use super::{
        InboxAdapter,
        InboxRunResponse,
        InboxSubmission,
        //
    };
}

use crate::extract::io::{
    InboxHttpError,
    RunInbox,
    SubmitInbox,
    //
};
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use inbox_api::io::{
    AsyncInboxService,
    RunResponse,
    SubmitInboxResponse,
    //
};

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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::extract::{DefaultBodyLimit, State};
    use axum::http::{Request, header};
    use axum::routing::post;
    use http_body_util::BodyExt;
    use inbox_api::io::{InboxService, testing};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    async fn submit_inbox(
        State(inbox): State<InboxAdapter>,
        request: SubmitInbox,
    ) -> Result<InboxSubmission, InboxHttpError> {
        inbox.submit(request).await
    }

    async fn run_inbox(
        State(inbox): State<InboxAdapter>,
        request: RunInbox,
    ) -> Result<InboxRunResponse, InboxHttpError> {
        inbox.run(request).await
    }

    fn app(service: InboxService) -> Router {
        let inbox_routes = Router::new()
            .route("/inbox", post(submit_inbox))
            .route("/inbox/run", post(run_inbox))
            .with_state(InboxAdapter::new(service));
        Router::new().nest("/api", inbox_routes)
    }

    async fn send(
        app: &Router,
        path: &str,
        content_type: Option<&str>,
        body: impl Into<Body>,
    ) -> (StatusCode, String) {
        let mut request = Request::post(path);
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

    async fn send_json(app: &Router, path: &str, body: &Value) -> (StatusCode, String) {
        send(app, path, Some("application/json"), body.to_string()).await
    }

    #[tokio::test]
    async fn accepts_valid_submissions_after_storage() {
        let (service, store) = testing::recording_service();
        let app = app(service);
        for body in testing::valid_submissions() {
            let (status, text) = send_json(&app, "/api/inbox", &body).await;
            assert_eq!(status, StatusCode::ACCEPTED, "{body}");
            let json: Value = serde_json::from_str(&text).unwrap();
            let id = json["message_id"].as_str().unwrap().to_owned();
            assert_eq!(store.stored_ids().last(), Some(&id), "{body}");
        }
    }

    #[tokio::test]
    async fn rejects_invalid_submissions_with_400() {
        let (service, store) = testing::recording_service();
        let app = app(service);
        for body in testing::invalid_submissions() {
            let (status, _) = send_json(&app, "/api/inbox", &body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        }
        for (body, message) in testing::submission_error_messages() {
            let (status, text) = send_json(&app, "/api/inbox", &body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(text, message);
        }
        let (status, _) = send(&app, "/api/inbox", Some("application/json"), "{").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(store.stored_ids().is_empty());
    }

    #[tokio::test]
    async fn preserves_content_type_and_body_limit_rejections() {
        let (service, _) = testing::recording_service();
        let app = app(service);
        for (path, body) in [
            (
                "/api/inbox",
                r#"{"payload":1,"routing_key":"x","source":"crawler"}"#,
            ),
            ("/api/inbox/run", r#"{"limit":1}"#),
        ] {
            let (status, _) = send(&app, path, None, body).await;
            assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
            let (status, _) = send(&app, path, Some("text/plain"), body).await;
            assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }
        let limited = app.layer(DefaultBodyLimit::max(8));
        let (status, _) = send_json(
            &limited,
            "/api/inbox",
            &json!({"payload":"too long","routing_key":"x","source":"crawler"}),
        )
        .await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn sanitizes_storage_and_task_failures() {
        for (service, message) in [
            (
                testing::failing_storage_service(),
                "inbox message could not be stored",
            ),
            (testing::panicking_service(), "inbox submission task failed"),
        ] {
            let (status, text) = send_json(
                &app(service),
                "/api/inbox",
                &json!({"payload":1,"routing_key":"x","source":"crawler"}),
            )
            .await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(text, message);
        }
    }

    #[tokio::test]
    async fn runs_batches_and_reports_consumer_failures() {
        let (service, _) = testing::recording_service();
        let app_ok = app(service);
        for body in testing::valid_run_requests() {
            let (status, text) = send_json(&app_ok, "/api/inbox/run", &body).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(
                serde_json::from_str::<Value>(&text).unwrap(),
                json!({"processed":0,"errors":[]})
            );
        }
        for body in testing::invalid_run_requests() {
            let (status, _) = send_json(&app_ok, "/api/inbox/run", &body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        }
        let (status, text) = send_json(
            &app(testing::failing_storage_service()),
            "/api/inbox/run",
            &json!({"limit":1}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap(),
            json!({"processed":null,"errors":[testing::RESERVATION_FAILURE]})
        );
        let (status, text) = send_json(
            &app(testing::panicking_service()),
            "/api/inbox/run",
            &json!({"limit":1}),
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(text, "queue processing task failed");
    }
}
