//! End-to-end proof that a client's error ID leads to the real error in the logs.
//!
//! Failures happen on Tokio's blocking pool, which thread-local subscribers don't
//! reach, so this binary installs one global subscriber. Tests find their log line
//! by its unique ID, so running them in parallel is safe.
mod support;

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use poem::{Route, test::TestClient};
use poem_openapi::{OpenApi, OpenApiService};
use serde_json::{Value, json};
use std::io::{self, Write};
use std::sync::{Arc, Mutex, OnceLock};
use tower::ServiceExt;
use tracing_subscriber::filter::LevelFilter;

const RESERVATION_DIAGNOSTIC: &str = "reservation unavailable";
const PANIC_DIAGNOSTIC: &str = "private reservation diagnostics";
const STORAGE_DIAGNOSTIC: &str = "private storage diagnostics";

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn logs() -> &'static Logs {
    static LOGS: OnceLock<Logs> = OnceLock::new();
    LOGS.get_or_init(|| {
        let logs = Logs::default();
        let writer = logs.clone();
        tracing_subscriber::fmt()
            .with_max_level(LevelFilter::WARN)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .init();
        logs
    })
}

/// Assert that exactly one log line carries `id`, at `level`, with `diagnostic`.
fn assert_logged(id: &str, level: &str, diagnostic: &str) {
    let output = String::from_utf8(logs().0.lock().unwrap().clone()).unwrap();
    let lines: Vec<&str> = output.lines().filter(|line| line.contains(id)).collect();
    assert_eq!(lines.len(), 1, "log lines for {id}: {lines:?}");
    assert!(lines[0].contains(level), "{}", lines[0]);
    assert!(lines[0].contains(diagnostic), "{}", lines[0]);
}

async fn post(app: Router, path: &str, body: &Value) -> (StatusCode, String) {
    let request = Request::post(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

fn poem_client<T: OpenApi + 'static>(api: T) -> TestClient<Route> {
    TestClient::new(Route::new().nest("/", OpenApiService::new(api, "test", "1")))
}

async fn poem_post(client: &TestClient<Route>, path: &str, body: &Value) -> (StatusCode, String) {
    let response = client.post(path).body_json(body).send().await;
    let status = response.0.status();
    (status, response.0.into_body().into_string().await.unwrap())
}

fn axum_apps(kind: Fixture) -> Vec<Router> {
    use Fixture::{Failing, Healthy, Panicking};
    let commanding = match kind {
        Failing => commanding::prelude::testing::failing_reservation_consumer(),
        Panicking => commanding::prelude::testing::panicking_consumer(),
        Healthy => commanding::prelude::testing::empty_consumer(),
    };
    let eventing = match kind {
        Failing => eventing::prelude::testing::failing_reservation_consumer(),
        Panicking => eventing::prelude::testing::panicking_consumer(),
        Healthy => eventing::prelude::testing::empty_consumer(),
    };
    let outbox = match kind {
        Failing => outbox::prelude::testing::failing_reservation_consumer(),
        Panicking => outbox::prelude::testing::panicking_consumer(),
        Healthy => outbox::prelude::testing::empty_consumer(),
    };
    vec![
        commanding::prelude::axum::command_routes(commanding),
        eventing::prelude::axum::event_routes(eventing),
        inbox::prelude::axum::inbox_routes(inbox_parts(kind)),
        outbox::prelude::axum::outbox_routes(outbox),
    ]
}

fn poem_clients(kind: Fixture) -> Vec<TestClient<Route>> {
    use Fixture::{Failing, Healthy, Panicking};
    let commanding = match kind {
        Failing => commanding::prelude::testing::failing_reservation_consumer(),
        Panicking => commanding::prelude::testing::panicking_consumer(),
        Healthy => commanding::prelude::testing::empty_consumer(),
    };
    let eventing = match kind {
        Failing => eventing::prelude::testing::failing_reservation_consumer(),
        Panicking => eventing::prelude::testing::panicking_consumer(),
        Healthy => eventing::prelude::testing::empty_consumer(),
    };
    let outbox = match kind {
        Failing => outbox::prelude::testing::failing_reservation_consumer(),
        Panicking => outbox::prelude::testing::panicking_consumer(),
        Healthy => outbox::prelude::testing::empty_consumer(),
    };
    vec![
        poem_client(commanding::prelude::poem::CommandingApi::new(commanding)),
        poem_client(eventing::prelude::poem::EventingApi::new(eventing)),
        poem_client(inbox::prelude::poem::InboxApi::new(inbox_parts(kind))),
        poem_client(outbox::prelude::poem::OutboxApi::new(outbox)),
    ]
}

#[derive(Clone, Copy)]
enum Fixture {
    Failing,
    Panicking,
    Healthy,
}

fn inbox_parts(kind: Fixture) -> inbox::prelude::testing::Parts {
    match kind {
        Fixture::Failing => inbox::prelude::testing::failing_storage_parts(),
        Fixture::Panicking => inbox::prelude::testing::panicking_parts(),
        Fixture::Healthy => inbox::prelude::testing::recording_parts().0,
    }
}

fn assert_batch_failure_is_logged(body: &str) {
    assert!(!body.contains(RESERVATION_DIAGNOSTIC), "{body}");
    let body: Value = serde_json::from_str(body).unwrap();
    support::assert_batch_failed_with_ids(&body, 1);
    let id = body["errors"][0].as_str().unwrap();
    assert_logged(id, "ERROR", RESERVATION_DIAGNOSTIC);
}

#[tokio::test]
async fn axum_batch_failures_return_only_logged_ids() {
    logs();
    for app in axum_apps(Fixture::Failing) {
        let (status, body) = post(app, "/run", &json!({"limit":1})).await;
        assert_eq!(status, StatusCode::OK);
        assert_batch_failure_is_logged(&body);
    }
}

#[tokio::test]
async fn poem_batch_failures_return_only_logged_ids() {
    logs();
    for client in poem_clients(Fixture::Failing) {
        let (status, body) = poem_post(&client, "/run", &json!({"limit":1})).await;
        assert_eq!(status, StatusCode::OK);
        assert_batch_failure_is_logged(&body);
    }
}

#[tokio::test]
async fn axum_task_failures_return_only_logged_ids() {
    logs();
    for app in axum_apps(Fixture::Panicking) {
        let (status, body) = post(app, "/run", &json!({"limit":1})).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        support::assert_is_error_id(&body);
        assert_logged(&body, "ERROR", PANIC_DIAGNOSTIC);
    }
}

#[tokio::test]
async fn poem_task_failures_return_only_logged_ids() {
    logs();
    for client in poem_clients(Fixture::Panicking) {
        let (status, body) = poem_post(&client, "/run", &json!({"limit":1})).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        support::assert_is_error_id(&body);
        assert_logged(&body, "ERROR", PANIC_DIAGNOSTIC);
    }
}

#[tokio::test]
async fn inbox_submission_failures_return_only_logged_ids() {
    logs();
    let submission = json!({"payload":1,"routing_key":"x","source":"crawler"});
    for kind in [Fixture::Failing, Fixture::Panicking] {
        let app = inbox::prelude::axum::inbox_routes(inbox_parts(kind));
        let (status, body) = post(app, "/submit", &submission).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        support::assert_is_error_id(&body);
        assert_logged(&body, "ERROR", STORAGE_DIAGNOSTIC);

        let client = poem_client(inbox::prelude::poem::InboxApi::new(inbox_parts(kind)));
        let (status, body) = poem_post(&client, "/submit", &submission).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        support::assert_is_error_id(&body);
        assert_logged(&body, "ERROR", STORAGE_DIAGNOSTIC);
    }
}

#[tokio::test]
async fn axum_malformed_bodies_return_only_logged_ids() {
    logs();
    for body in eventing::prelude::testing::malformed_run_requests() {
        for app in axum_apps(Fixture::Healthy) {
            let (status, text) = post(app, "/run", &body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            support::assert_is_error_id(&text);
            assert_logged(&text, "WARN", "HTTP request rejected");
        }
    }
}

#[tokio::test]
async fn poem_malformed_bodies_end_with_only_a_logged_id() {
    logs();
    for body in eventing::prelude::testing::malformed_run_requests() {
        for client in poem_clients(Fixture::Healthy) {
            let (status, text) = poem_post(&client, "/run", &body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            // Poem adds its own prefix, which names only the public schema.
            assert!(!text.contains("RunRequestData"), "{text}");
            assert!(!text.contains("Raw"), "{text}");
            let id = text.rsplit(' ').next().unwrap();
            support::assert_is_error_id(id);
            assert_logged(id, "WARN", "HTTP request rejected");
        }
    }
}

#[tokio::test]
async fn messages_we_wrote_stay_readable_and_unlogged() {
    logs();
    for app in axum_apps(Fixture::Healthy) {
        let (status, text) = post(app, "/run", &json!({"limit":0})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(text, "limit must be between 1 and 1000");
    }
    let app = inbox::prelude::axum::inbox_routes(inbox_parts(Fixture::Healthy));
    let (status, text) = post(app, "/submit", &json!({"routing_key":"x","source":"s"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(text, "payload is required");

    let app =
        outbox::prelude::axum::outbox_routes(outbox::prelude::AsyncOutboxService::unconfigured());
    let (status, text) = post(app, "/run", &json!({"limit":1})).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(text, "outbox publisher is not configured");

    let output = String::from_utf8(logs().0.lock().unwrap().clone()).unwrap();
    for message in [
        "limit must be between",
        "payload is required",
        "outbox publisher is not configured",
    ] {
        assert!(!output.contains(message), "{message} was logged");
    }
}
