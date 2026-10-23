pub mod io {
    pub use super::{
        InboxAdapter,
        InboxSubmission,
        //
    };
}

use crate::types::io::{
    RunRequest,
    RunResponse,
    SubmitInboxRequest,
    SubmitInboxResponse,
    //
};
use inbox_api::io::{
    AsyncInboxService,
    InboxApiError,
    //
};
use poem::{Error, Result, http::StatusCode};
use poem_openapi::{ApiResponse, payload::Json};

#[derive(ApiResponse)]
pub enum InboxSubmission {
    /// Message stored; duplicate IDs preserve the original message.
    #[oai(status = 202)]
    Accepted(Json<SubmitInboxResponse>),
}

/// Executes inbox endpoints declared by the application.
///
/// ```ignore
/// #[OpenApi]
/// impl InboxApi {
///     #[oai(path = "/inbox", method = "post")]
///     async fn submit(&self, request: Json<SubmitInboxRequest>) -> Result<InboxSubmission> {
///         self.adapter.submit(request).await
///     }
///
///     #[oai(path = "/inbox/run", method = "post")]
///     async fn run(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
///         self.adapter.run(request).await
///     }
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
    pub async fn submit(&self, request: Json<SubmitInboxRequest>) -> Result<InboxSubmission> {
        let response = self
            .service
            .submit(request.0.0)
            .await
            .map_err(err_inbox_failed)?;
        Ok(InboxSubmission::Accepted(Json(SubmitInboxResponse(
            response,
        ))))
    }

    /// Queue commands for one batch; consumer failures are reported in a `200`
    /// response.
    pub async fn run(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        let response = self
            .service
            .run(request.0.0)
            .await
            .map_err(err_inbox_failed)?;
        Ok(Json(RunResponse(response)))
    }
}

fn err_inbox_failed(error: InboxApiError) -> Error {
    let status = if error.is_client_error() {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    Error::new(error, status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use inbox_api::io::{InboxService, testing};
    use poem::{Route, test::TestClient};
    use poem_openapi::{OpenApi, OpenApiService};
    use serde_json::{Value, json};

    struct TestApi {
        adapter: InboxAdapter,
    }

    #[OpenApi]
    impl TestApi {
        #[oai(path = "/inbox", method = "post")]
        async fn submit(&self, request: Json<SubmitInboxRequest>) -> Result<InboxSubmission> {
            self.adapter.submit(request).await
        }

        #[oai(path = "/inbox/run", method = "post")]
        async fn run(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
            self.adapter.run(request).await
        }
    }

    fn client(service: InboxService) -> TestClient<Route> {
        let api = OpenApiService::new(
            TestApi {
                adapter: InboxAdapter::new(service),
            },
            "test",
            "1",
        )
        .server("/api");
        let spec = api.spec_endpoint();
        TestClient::new(Route::new().at("/api/openapi.json", spec).nest("/api", api))
    }

    #[tokio::test]
    async fn accepts_valid_submissions_after_storage() {
        let (service, store) = testing::recording_service();
        let client = client(service);
        for body in testing::valid_submissions() {
            let response = client.post("/api/inbox").body_json(&body).send().await;
            response.assert_status(StatusCode::ACCEPTED);
            let json: Value = response.json().await.value().deserialize();
            let id = json["message_id"].as_str().unwrap().to_owned();
            assert_eq!(store.stored_ids().last(), Some(&id), "{body}");
        }
    }

    #[tokio::test]
    async fn rejects_invalid_submissions() {
        let (service, store) = testing::recording_service();
        let client = client(service);
        for body in testing::invalid_submissions() {
            let response = client.post("/api/inbox").body_json(&body).send().await;
            response.assert_status(StatusCode::BAD_REQUEST);
        }
        for (body, message) in testing::submission_error_messages() {
            let response = client.post("/api/inbox").body_json(&body).send().await;
            response.assert_status(StatusCode::BAD_REQUEST);
            response.assert_text(message).await;
        }
        client
            .post("/api/inbox")
            .content_type("application/json")
            .body("{")
            .send()
            .await
            .assert_status(StatusCode::BAD_REQUEST);
        assert!(store.stored_ids().is_empty());
    }

    #[tokio::test]
    async fn requires_json_content_type() {
        let (service, _) = testing::recording_service();
        let client = client(service);
        for (path, body) in [
            (
                "/api/inbox",
                r#"{"payload":1,"routing_key":"x","source":"crawler"}"#,
            ),
            ("/api/inbox/run", r#"{"limit":1}"#),
        ] {
            client
                .post(path)
                .body(body)
                .send()
                .await
                .assert_status(StatusCode::UNSUPPORTED_MEDIA_TYPE);
            client
                .post(path)
                .content_type("text/plain")
                .body(body)
                .send()
                .await
                .assert_status(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }
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
            let response = client(service)
                .post("/api/inbox")
                .body_json(&json!({"payload":1,"routing_key":"x","source":"crawler"}))
                .send()
                .await;
            response.assert_status(StatusCode::INTERNAL_SERVER_ERROR);
            response.assert_text(message).await;
        }
    }

    #[tokio::test]
    async fn runs_batches_and_reports_consumer_failures() {
        let (service, _) = testing::recording_service();
        let client_ok = client(service);
        for body in testing::valid_run_requests() {
            let response = client_ok
                .post("/api/inbox/run")
                .body_json(&body)
                .send()
                .await;
            response.assert_status_is_ok();
            response
                .assert_json(json!({"processed":0,"errors":[]}))
                .await;
        }
        for body in testing::invalid_run_requests() {
            client_ok
                .post("/api/inbox/run")
                .body_json(&body)
                .send()
                .await
                .assert_status(StatusCode::BAD_REQUEST);
        }
        let response = client(testing::failing_storage_service())
            .post("/api/inbox/run")
            .body_json(&json!({"limit":1}))
            .send()
            .await;
        response.assert_status_is_ok();
        response
            .assert_json(json!({"processed":null,"errors":[testing::RESERVATION_FAILURE]}))
            .await;

        let response = client(testing::panicking_service())
            .post("/api/inbox/run")
            .body_json(&json!({"limit":1}))
            .send()
            .await;
        response.assert_status(StatusCode::INTERNAL_SERVER_ERROR);
        response.assert_text("queue processing task failed").await;
    }

    #[tokio::test]
    async fn publishes_existing_openapi_schemas() {
        let (service, _) = testing::recording_service();
        let response = client(service).get("/api/openapi.json").send().await;
        let spec: Value = response.json().await.value().deserialize();
        let submission = &spec["paths"]["/inbox"]["post"];
        assert_eq!(
            submission["requestBody"]["content"]["application/json; charset=utf-8"]["schema"]["$ref"],
            "#/components/schemas/SubmitInboxRequest"
        );
        assert_eq!(
            submission["responses"]["202"]["content"]["application/json; charset=utf-8"]["schema"]
                ["$ref"],
            "#/components/schemas/SubmitInboxResponse"
        );
        let schemas = &spec["components"]["schemas"];
        assert_eq!(
            schemas["SubmitInboxRequest"]["required"],
            json!(["payload", "routing_key", "source"])
        );
        assert_eq!(
            schemas["SubmitInboxRequest"]["properties"]["routing_key"]["type"],
            "string"
        );
        let limit = &schemas["RunRequest"]["properties"]["limit"];
        assert_eq!(limit["minimum"], 1.0);
        assert_eq!(limit["maximum"], 1000.0);
        assert_eq!(schemas["RunResponse"]["required"], json!(["errors"]));
        assert_eq!(
            spec["paths"]["/inbox/run"]["post"]["responses"]["200"]["content"]["application/json; charset=utf-8"]
                ["schema"]["$ref"],
            "#/components/schemas/RunResponse"
        );
    }
}
