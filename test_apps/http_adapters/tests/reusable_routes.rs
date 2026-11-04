mod support;

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
async fn send(
    app: Router,
    path: &str,
    method: &str,
    content_type: Option<&str>,
    body: &str,
) -> (StatusCode, Value) {
    let mut request = Request::builder().method(method).uri(path);
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }
    let response = app
        .oneshot(request.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes)
            .unwrap_or(Value::String(String::from_utf8_lossy(&bytes).into_owned())),
    )
}
#[tokio::test]
async fn axum_factories_keep_validation_mounting_and_methods() {
    let apps = [
        Router::new().nest(
            "/custom",
            eventing::prelude::axum::event_routes(eventing::prelude::testing::queued_consumer(2)),
        ),
        Router::new().nest(
            "/custom",
            inbox::prelude::axum::inbox_routes(inbox::prelude::testing::recording_parts().0),
        ),
        Router::new().nest(
            "/custom",
            outbox::prelude::axum::outbox_routes(outbox::prelude::testing::queued_consumer(2)),
        ),
    ];
    for (i, app) in apps.into_iter().enumerate() {
        assert_eq!(
            send(app.clone(), "/custom/run", "GET", None, "").await.0,
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert_eq!(
            send(app.clone(), "/custom/run", "POST", None, "{}").await.0,
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
        assert_eq!(
            send(
                app.clone(),
                "/custom/run",
                "POST",
                Some("application/json"),
                "{\"limit\":0}"
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
        let (status, body) = send(
            app,
            "/custom/run",
            "POST",
            Some("application/json"),
            "{\"limit\":1}",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["processed"], if i == 1 { 0 } else { 1 });
    }
}
#[tokio::test]
async fn axum_factories_keep_processing_errors_and_sanitized_panics() {
    let apps = [
        eventing::prelude::axum::event_routes(eventing::prelude::testing::panicking_consumer()),
        inbox::prelude::axum::inbox_routes(inbox::prelude::testing::panicking_parts()),
        outbox::prelude::axum::outbox_routes(outbox::prelude::testing::panicking_consumer()),
    ];
    for app in apps {
        let (status, body) = send(
            app,
            "/run",
            "POST",
            Some("application/json"),
            "{\"limit\":1}",
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        support::assert_is_error_id(body.as_str().unwrap());
    }
    let apps = [
        eventing::prelude::axum::event_routes(
            eventing::prelude::testing::failing_reservation_consumer(),
        ),
        inbox::prelude::axum::inbox_routes(inbox::prelude::testing::failing_storage_parts()),
        outbox::prelude::axum::outbox_routes(
            outbox::prelude::testing::failing_reservation_consumer(),
        ),
    ];
    for app in apps {
        let (status, body) = send(
            app,
            "/run",
            "POST",
            Some("application/json"),
            "{\"limit\":1}",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        support::assert_batch_failed_with_ids(&body, 1);
    }
}
#[tokio::test]
async fn inbox_factory_submits_at_relative_submit_path() {
    let (service, store) = inbox::prelude::testing::recording_parts();
    let app = Router::new().nest("/messages", inbox::prelude::axum::inbox_routes(service));
    let (status, body) = send(
        app,
        "/messages/submit",
        "POST",
        Some("application/json"),
        &json!({"routing_key":"example","source":"test","payload":{"a":1}}).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(
        store.stored_ids(),
        vec![body["message_id"].as_str().unwrap()]
    );
}
#[tokio::test]
async fn outbox_factory_keeps_unconfigured_status() {
    let app =
        outbox::prelude::axum::outbox_routes(outbox::prelude::AsyncOutboxService::unconfigured());
    assert_eq!(
        send(
            app.clone(),
            "/run",
            "POST",
            Some("application/json"),
            "{\"limit\":0}"
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(
            app,
            "/run",
            "POST",
            Some("application/json"),
            "{\"limit\":1}"
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}
#[tokio::test]
async fn poem_objects_reuse_contracts_and_schema_names() {
    use poem::{Route, test::TestClient};
    use poem_openapi::OpenApiService;
    macro_rules! check {($api:expr)=>{{
 let api=OpenApiService::new($api,"queue","1");let spec:Value=serde_json::from_str(&api.spec()).unwrap();
 assert!(spec["components"]["schemas"].get("RunRequest").is_some());assert!(spec["components"]["schemas"].get("RunResponse").is_some());
 let client=TestClient::new(Route::new().nest("/custom",api));
 client.post("/custom/run").body_json(&json!({"limit":1})).send().await.assert_status_is_ok();
 client.post("/custom/run").body_json(&json!({"limit":0})).send().await.assert_status(poem::http::StatusCode::BAD_REQUEST);
 client.get("/custom/run").send().await.assert_status(poem::http::StatusCode::METHOD_NOT_ALLOWED);
 }};}
    check!(eventing::prelude::poem::EventingApi::new(
        eventing::prelude::testing::empty_consumer()
    ));
    check!(inbox::prelude::poem::InboxApi::new(
        inbox::prelude::testing::recording_parts().0
    ));
    check!(outbox::prelude::poem::OutboxApi::new(
        outbox::prelude::testing::empty_consumer()
    ));
    let (service, store) = inbox::prelude::testing::recording_parts();
    let client = TestClient::new(OpenApiService::new(
        inbox::prelude::poem::InboxApi::new(service),
        "inbox",
        "1",
    ));
    client
        .post("/submit")
        .body_json(&json!({"routing_key":"example","source":"test","payload":1}))
        .send()
        .await
        .assert_status(poem::http::StatusCode::ACCEPTED);
    assert_eq!(store.stored_ids().len(), 1);
    let client = TestClient::new(OpenApiService::new(
        outbox::prelude::poem::OutboxApi::new(outbox::prelude::AsyncOutboxService::unconfigured()),
        "outbox",
        "1",
    ));
    client
        .post("/run")
        .body_json(&json!({"limit":1}))
        .send()
        .await
        .assert_status(poem::http::StatusCode::SERVICE_UNAVAILABLE);
}
