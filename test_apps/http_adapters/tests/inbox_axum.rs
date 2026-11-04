mod support;

mod tests {
    use axum::Router;
    use axum::body::Body;
    use axum::extract::{DefaultBodyLimit, State};
    use axum::http::StatusCode;
    use axum::http::{Request, header};
    use axum::routing::post;
    use http_body_util::BodyExt;
    use inbox::prelude::axum::*;
    use inbox::prelude::axum::{
        InboxHttpError,
        RunInbox,
        SubmitInbox,
        //
    };
    use inbox::prelude::{AsyncInboxService, testing};
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

    fn app(service: impl Into<AsyncInboxService>) -> Router {
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
        let (service, store) = testing::recording_parts();
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
        let (service, store) = testing::recording_parts();
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
        let (service, _) = testing::recording_parts();
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
        for service in [testing::failing_storage_parts(), testing::panicking_parts()] {
            let (status, text) = send_json(
                &app(service),
                "/api/inbox",
                &json!({"payload":1,"routing_key":"x","source":"crawler"}),
            )
            .await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            crate::support::assert_is_error_id(&text);
        }
    }

    #[tokio::test]
    async fn runs_batches_and_reports_consumer_failures() {
        let (service, _) = testing::recording_parts();
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
            &app(testing::failing_storage_parts()),
            "/api/inbox/run",
            &json!({"limit":1}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        crate::support::assert_batch_failed_with_ids(&serde_json::from_str(&text).unwrap(), 1);
        let (status, text) = send_json(
            &app(testing::panicking_parts()),
            "/api/inbox/run",
            &json!({"limit":1}),
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        crate::support::assert_is_error_id(&text);
    }
}
