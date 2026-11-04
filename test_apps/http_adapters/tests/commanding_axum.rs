mod support;

mod tests {

    use axum::Router;
    use axum::body::Body;
    use axum::extract::DefaultBodyLimit;
    use axum::http::StatusCode;
    use axum::http::{Request, header};
    use commanding::prelude::axum::*;
    use commanding::prelude::{AsyncCommandingService, testing};
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    fn app(service: impl Into<AsyncCommandingService>) -> Router {
        Router::new().nest("/api/commands", command_routes(service))
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
        let app = app(testing::empty_consumer());
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
        let (status, text) =
            send_json(&app(testing::queued_consumer(3)), &json!({"limit":2})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json(&text), json!({"processed":2,"errors":[]}));
        for (service, count) in [
            (testing::failing_reservation_consumer(), 1),
            (testing::failing_handler_and_process_consumer(), 2),
        ] {
            let (status, text) = send_json(&app(service), &json!({"limit":1})).await;
            assert_eq!(status, StatusCode::OK);
            crate::support::assert_batch_failed_with_ids(&json(&text), count);
        }
    }

    #[tokio::test]
    async fn sanitizes_task_failures() {
        let (status, text) =
            send_json(&app(testing::panicking_consumer()), &json!({"limit":1})).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        crate::support::assert_is_error_id(&text);
    }

    #[tokio::test]
    async fn preserves_content_type_and_body_limit_rejections() {
        let app = app(testing::empty_consumer());
        let (status, _) = send(&app, None, r#"{"limit":1}"#).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let (status, _) = send(&app, Some("text/plain"), r#"{"limit":1}"#).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let limited = app.layer(DefaultBodyLimit::max(8));
        let (status, _) = send_json(&limited, &json!({"limit":1,"extra":true})).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    }
}
