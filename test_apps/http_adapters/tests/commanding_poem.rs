mod support;

mod tests {
    use commanding::prelude::poem::*;
    use commanding::prelude::{AsyncCommandingService, testing};
    use inbox::prelude::poem::InboxAdapter;
    use mulac_http::prelude::poem::{
        RunRequest,
        RunResponse,
        //
    };
    use poem::{Result, http::StatusCode};
    use poem::{Route, test::TestClient};
    use poem_openapi::payload::Json;
    use poem_openapi::{OpenApi, OpenApiService};
    use serde_json::{Value, json};

    struct TestApi {
        inbox: InboxAdapter,
        commands: CommandingAdapter,
    }

    #[OpenApi]
    impl TestApi {
        #[oai(path = "/inbox/run", method = "post")]
        async fn run_inbox(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
            self.inbox.run(request).await
        }

        #[oai(path = "/commands/run", method = "post")]
        async fn run_commands(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
            self.commands.run(request).await
        }
    }

    fn client(service: impl Into<AsyncCommandingService>) -> TestClient<Route> {
        let (inbox, _) = inbox::prelude::testing::recording_parts();
        let api = OpenApiService::new(
            TestApi {
                inbox: InboxAdapter::new(inbox),
                commands: CommandingAdapter::new(service),
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
        let client = client(testing::empty_consumer());
        for body in testing::valid_run_requests() {
            let response = client
                .post("/api/commands/run")
                .body_json(&body)
                .send()
                .await;
            response.assert_status_is_ok();
            response
                .assert_json(json!({"processed":0,"errors":[]}))
                .await;
        }
        for body in testing::invalid_run_requests() {
            client
                .post("/api/commands/run")
                .body_json(&body)
                .send()
                .await
                .assert_status(StatusCode::BAD_REQUEST);
        }
        // The wrapper validates while parsing, so Poem prefixes Mulac's text.
        for (body, message) in testing::run_request_error_messages() {
            let response = client
                .post("/api/commands/run")
                .body_json(&body)
                .send()
                .await;
            response.assert_status(StatusCode::BAD_REQUEST);
            let text = response.0.into_body().into_string().await.unwrap();
            assert!(text.ends_with(message), "{text}");
        }
        client
            .post("/api/commands/run")
            .content_type("application/json")
            .body("{")
            .send()
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn reports_counts_and_command_failures() {
        let response = client(testing::queued_consumer(3))
            .post("/api/commands/run")
            .body_json(&json!({"limit":2}))
            .send()
            .await;
        response.assert_status_is_ok();
        response
            .assert_json(json!({"processed":2,"errors":[]}))
            .await;
        for (service, count) in [
            (testing::failing_reservation_consumer(), 1),
            (testing::failing_handler_and_process_consumer(), 2),
        ] {
            let response = client(service)
                .post("/api/commands/run")
                .body_json(&json!({"limit":1}))
                .send()
                .await;
            response.assert_status_is_ok();
            let body: Value = response.0.into_body().into_json().await.unwrap();
            crate::support::assert_batch_failed_with_ids(&body, count);
        }
    }

    #[tokio::test]
    async fn sanitizes_task_failures() {
        let response = client(testing::panicking_consumer())
            .post("/api/commands/run")
            .body_json(&json!({"limit":1}))
            .send()
            .await;
        response.assert_status(StatusCode::INTERNAL_SERVER_ERROR);
        let text = response.0.into_body().into_string().await.unwrap();
        crate::support::assert_is_error_id(&text);
    }

    #[tokio::test]
    async fn requires_json_content_type() {
        let client = client(testing::empty_consumer());
        for content_type in [None, Some("text/plain")] {
            let mut request = client.post("/api/commands/run").body(r#"{"limit":1}"#);
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
        let response = client(testing::empty_consumer())
            .get("/api/openapi.json")
            .send()
            .await;
        let spec: Value = response.json().await.value().deserialize();
        for path in ["/inbox/run", "/commands/run"] {
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

mod reusable_routes {
    use commanding::prelude::poem::CommandingApi;
    use commanding::prelude::{AsyncCommandingService, testing};
    use poem::{Route, http::StatusCode, test::TestClient};
    use poem_openapi::OpenApiService;
    use serde_json::{Value, json};

    fn client(service: impl Into<AsyncCommandingService>) -> TestClient<Route> {
        let api = OpenApiService::new(CommandingApi::new(service), "commands", "1")
            .server("/ops/commands");
        let spec = api.spec_endpoint();
        TestClient::new(
            Route::new()
                .at("/openapi.json", spec)
                .nest("/ops/commands", api),
        )
    }

    #[tokio::test]
    async fn mounted_endpoint_preserves_request_contract() {
        let client = client(testing::empty_consumer());
        for body in testing::valid_run_requests() {
            let response = client
                .post("/ops/commands/run")
                .body_json(&body)
                .send()
                .await;
            response.assert_status_is_ok();
            response
                .assert_json(json!({"processed": 0, "errors": []}))
                .await;
        }
        for body in testing::invalid_run_requests() {
            client
                .post("/ops/commands/run")
                .body_json(&body)
                .send()
                .await
                .assert_status(StatusCode::BAD_REQUEST);
        }
        client
            .get("/ops/commands/run")
            .send()
            .await
            .assert_status(StatusCode::METHOD_NOT_ALLOWED);
        client
            .post("/run")
            .body_json(&json!({"limit": 1}))
            .send()
            .await
            .assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn mounted_endpoint_processes_batches_and_reports_errors_by_id() {
        let response = client(testing::queued_consumer(3))
            .post("/ops/commands/run")
            .body_json(&json!({"limit": 2}))
            .send()
            .await;
        response.assert_status_is_ok();
        response
            .assert_json(json!({"processed": 2, "errors": []}))
            .await;

        let response = client(testing::failing_handler_and_process_consumer())
            .post("/ops/commands/run")
            .body_json(&json!({"limit": 2}))
            .send()
            .await;
        response.assert_status_is_ok();
        let body: Value = response.0.into_body().into_json().await.unwrap();
        crate::support::assert_batch_failed_with_ids(&body, 2);

        let response = client(testing::panicking_consumer())
            .post("/ops/commands/run")
            .body_json(&json!({"limit": 1}))
            .send()
            .await;
        response.assert_status(StatusCode::INTERNAL_SERVER_ERROR);
        let text = response.0.into_body().into_string().await.unwrap();
        crate::support::assert_is_error_id(&text);
    }

    #[tokio::test]
    async fn openapi_describes_relative_route_and_host_prefix() {
        let client = client(testing::empty_consumer());
        let response = client.get("/openapi.json").send().await;
        response.assert_status_is_ok();
        let spec: Value =
            serde_json::from_str(&response.0.into_body().into_string().await.unwrap()).unwrap();
        assert_eq!(spec["servers"][0]["url"], "/ops/commands");
        assert!(spec["paths"]["/run"]["post"].is_object());
        assert_eq!(spec["paths"].as_object().unwrap().len(), 1);
        assert_eq!(
            spec["components"]["schemas"]["RunRequest"]["properties"]["limit"]["minimum"],
            1.0
        );
        assert_eq!(
            spec["components"]["schemas"]["RunRequest"]["properties"]["limit"]["maximum"],
            1000.0
        );
    }
}
