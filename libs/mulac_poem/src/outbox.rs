pub mod io {
    pub use super::OutboxAdapter;
}

use crate::types::io::{
    RunRequest,
    RunResponse,
    //
};
use outbox_api::io::{
    AsyncOutboxService,
    OutboxApiError,
    OutboxApiErrorKind,
    RunOutboxRequest,
    RunOutboxResponse,
    //
};
use poem::{Error, Result, http::StatusCode};
use poem_openapi::payload::Json;

/// Executes the outbox batch endpoint declared by the application.
///
/// It reuses the schema-owning [`RunRequest`] and [`RunResponse`] wrappers, so
/// the OpenAPI document keeps one `RunRequest` and one `RunResponse` schema.
///
/// ```ignore
/// #[OpenApi]
/// impl OutboxApi {
///     #[oai(path = "/outbox/run", method = "post")]
///     async fn run_outbox(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
///         self.adapter.run(request).await
///     }
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
    pub async fn run(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        let response = self
            .service
            .run(request.0.into())
            .await
            .map_err(err_outbox_failed)?;
        Ok(Json(response.into()))
    }
}

impl From<RunRequest> for RunOutboxRequest {
    fn from(request: RunRequest) -> Self {
        Self {
            limit: request.limit,
        }
    }
}

impl From<RunOutboxResponse> for RunResponse {
    fn from(response: RunOutboxResponse) -> Self {
        Self(inbox_api::io::RunResponse {
            processed: response.processed,
            errors: response.errors,
        })
    }
}

fn err_outbox_failed(error: OutboxApiError) -> Error {
    let status = match error.kind() {
        OutboxApiErrorKind::Invalid => StatusCode::BAD_REQUEST,
        OutboxApiErrorKind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        OutboxApiErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    };
    Error::new(error, status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::io::InboxAdapter;
    use outbox_api::io::testing;
    use poem::{Route, test::TestClient};
    use poem_openapi::{OpenApi, OpenApiService};
    use serde_json::{Value, json};

    struct TestApi {
        inbox: InboxAdapter,
        outbox: OutboxAdapter,
    }

    #[OpenApi]
    impl TestApi {
        #[oai(path = "/inbox/run", method = "post")]
        async fn run_inbox(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
            self.inbox.run(request).await
        }

        #[oai(path = "/outbox/run", method = "post")]
        async fn run_outbox(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
            self.outbox.run(request).await
        }
    }

    fn client(outbox: OutboxAdapter) -> TestClient<Route> {
        let (inbox, _) = inbox_api::io::testing::recording_service();
        let api = OpenApiService::new(
            TestApi {
                inbox: InboxAdapter::new(inbox),
                outbox,
            },
            "test",
            "1",
        )
        .server("/api");
        let spec = api.spec_endpoint();
        TestClient::new(Route::new().at("/api/openapi.json", spec).nest("/api", api))
    }

    #[tokio::test]
    async fn runs_batches_from_the_shared_run_request_table() {
        let client = client(OutboxAdapter::new(testing::empty_service()));
        for body in testing::valid_run_requests() {
            let response = client.post("/api/outbox/run").body_json(&body).send().await;
            response.assert_status_is_ok();
            response
                .assert_json(json!({"processed":0,"errors":[]}))
                .await;
        }
        for body in testing::invalid_run_requests() {
            client
                .post("/api/outbox/run")
                .body_json(&body)
                .send()
                .await
                .assert_status(StatusCode::BAD_REQUEST);
        }
        // The wrapper validates while parsing, so Poem prefixes Mulac's text.
        for (body, message) in testing::run_request_error_messages() {
            let response = client.post("/api/outbox/run").body_json(&body).send().await;
            response.assert_status(StatusCode::BAD_REQUEST);
            let text = response.0.into_body().into_string().await.unwrap();
            assert!(text.ends_with(message), "{text}");
        }
        client
            .post("/api/outbox/run")
            .content_type("application/json")
            .body("{")
            .send()
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn reports_counts_and_publication_failures() {
        let response = client(OutboxAdapter::new(testing::queued_service(3)))
            .post("/api/outbox/run")
            .body_json(&json!({"limit":2}))
            .send()
            .await;
        response.assert_status_is_ok();
        response
            .assert_json(json!({"processed":2,"errors":[]}))
            .await;
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
            let response = client(OutboxAdapter::new(service))
                .post("/api/outbox/run")
                .body_json(&json!({"limit":1}))
                .send()
                .await;
            response.assert_status_is_ok();
            response
                .assert_json(json!({"processed":null,"errors":errors}))
                .await;
        }
    }

    #[tokio::test]
    async fn keeps_published_entries_of_a_partial_batch() {
        let (service, outcomes) = testing::partially_failing_service();
        let response = client(OutboxAdapter::new(service))
            .post("/api/outbox/run")
            .body_json(&json!({"limit":5}))
            .send()
            .await;
        response.assert_status_is_ok();
        response
            .assert_json(json!({"processed":null,"errors":[testing::PUBLISH_FAILURE]}))
            .await;
        assert_eq!(
            *outcomes.lock().unwrap(),
            vec!["completed", "failed", "completed"]
        );
    }

    #[tokio::test]
    async fn unconfigured_publishers_are_unavailable_after_validation() {
        let client = client(OutboxAdapter::unconfigured());
        for body in testing::valid_run_requests() {
            let response = client.post("/api/outbox/run").body_json(&body).send().await;
            response.assert_status(StatusCode::SERVICE_UNAVAILABLE);
            response
                .assert_text("outbox publisher is not configured")
                .await;
        }
        for body in testing::invalid_run_requests() {
            client
                .post("/api/outbox/run")
                .body_json(&body)
                .send()
                .await
                .assert_status(StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn sanitizes_task_failures() {
        let response = client(OutboxAdapter::new(testing::panicking_service()))
            .post("/api/outbox/run")
            .body_json(&json!({"limit":1}))
            .send()
            .await;
        response.assert_status(StatusCode::INTERNAL_SERVER_ERROR);
        response.assert_text("queue processing task failed").await;
    }

    #[tokio::test]
    async fn requires_json_content_type() {
        let client = client(OutboxAdapter::new(testing::empty_service()));
        for content_type in [None, Some("text/plain")] {
            let mut request = client.post("/api/outbox/run").body(r#"{"limit":1}"#);
            if let Some(content_type) = content_type {
                request = request.content_type(content_type);
            }
            request
                .send()
                .await
                .assert_status(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }
    }

    #[tokio::test]
    async fn shares_run_schemas_with_the_inbox() {
        let response = client(OutboxAdapter::unconfigured())
            .get("/api/openapi.json")
            .send()
            .await;
        let spec: Value = response.json().await.value().deserialize();
        for path in ["/inbox/run", "/outbox/run"] {
            let run = &spec["paths"][path]["post"];
            assert_eq!(
                run["requestBody"]["content"]["application/json; charset=utf-8"]["schema"]["$ref"],
                "#/components/schemas/RunRequest"
            );
            assert_eq!(
                run["responses"]["200"]["content"]["application/json; charset=utf-8"]["schema"]["$ref"],
                "#/components/schemas/RunResponse"
            );
            assert_eq!(
                run["responses"]
                    .as_object()
                    .unwrap()
                    .keys()
                    .collect::<Vec<_>>(),
                vec!["200"]
            );
        }
        let schemas: Vec<_> = spec["components"]["schemas"]
            .as_object()
            .unwrap()
            .keys()
            .collect();
        assert_eq!(schemas, vec!["RunRequest", "RunResponse"]);
        let limit = &spec["components"]["schemas"]["RunRequest"]["properties"]["limit"];
        assert_eq!(limit["minimum"], 1.0);
        assert_eq!(limit["maximum"], 1000.0);
    }
}
