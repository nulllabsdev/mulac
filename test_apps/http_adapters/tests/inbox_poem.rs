mod support;

mod tests {
    use inbox::prelude::poem::*;
    use inbox::prelude::poem::{
        RunRequest,
        RunResponse,
        SubmitInboxRequest,
        //
    };
    use inbox::prelude::{AsyncInboxService, testing};
    use poem::{Result, http::StatusCode};
    use poem::{Route, test::TestClient};
    use poem_openapi::payload::Json;
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

    fn client(service: impl Into<AsyncInboxService>) -> TestClient<Route> {
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
        let (service, store) = testing::recording_parts();
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
        let (service, store) = testing::recording_parts();
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
        let (service, _) = testing::recording_parts();
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
        for service in [testing::failing_storage_parts(), testing::panicking_parts()] {
            let response = client(service)
                .post("/api/inbox")
                .body_json(&json!({"payload":1,"routing_key":"x","source":"crawler"}))
                .send()
                .await;
            response.assert_status(StatusCode::INTERNAL_SERVER_ERROR);
            let text = response.0.into_body().into_string().await.unwrap();
            crate::support::assert_is_error_id(&text);
        }
    }

    #[tokio::test]
    async fn runs_batches_and_reports_consumer_failures() {
        let (service, _) = testing::recording_parts();
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
        let response = client(testing::failing_storage_parts())
            .post("/api/inbox/run")
            .body_json(&json!({"limit":1}))
            .send()
            .await;
        response.assert_status_is_ok();
        let body: Value = response.0.into_body().into_json().await.unwrap();
        crate::support::assert_batch_failed_with_ids(&body, 1);

        let response = client(testing::panicking_parts())
            .post("/api/inbox/run")
            .body_json(&json!({"limit":1}))
            .send()
            .await;
        response.assert_status(StatusCode::INTERNAL_SERVER_ERROR);
        let text = response.0.into_body().into_string().await.unwrap();
        crate::support::assert_is_error_id(&text);
    }

    #[tokio::test]
    async fn publishes_existing_openapi_schemas() {
        let (service, _) = testing::recording_parts();
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
