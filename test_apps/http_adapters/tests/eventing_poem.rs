mod support;

mod tests {
    use eventing::prelude::poem::*;
    use eventing::prelude::{AsyncEventingService, testing};
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
        events: EventingAdapter,
    }

    #[OpenApi]
    impl TestApi {
        #[oai(path = "/inbox/run", method = "post")]
        async fn run_inbox(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
            self.inbox.run(request).await
        }

        #[oai(path = "/events/run", method = "post")]
        async fn run_events(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
            self.events.run(request).await
        }
    }

    fn client(service: impl Into<AsyncEventingService>) -> TestClient<Route> {
        let (inbox, _) = inbox::prelude::testing::recording_parts();
        let api = OpenApiService::new(
            TestApi {
                inbox: InboxAdapter::new(inbox),
                events: EventingAdapter::new(service),
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
            let response = client.post("/api/events/run").body_json(&body).send().await;
            response.assert_status_is_ok();
            response
                .assert_json(json!({"processed":0,"errors":[]}))
                .await;
        }
        for body in testing::invalid_run_requests() {
            client
                .post("/api/events/run")
                .body_json(&body)
                .send()
                .await
                .assert_status(StatusCode::BAD_REQUEST);
        }
        // The wrapper validates while parsing, so Poem prefixes Mulac's text.
        for (body, message) in testing::run_request_error_messages() {
            let response = client.post("/api/events/run").body_json(&body).send().await;
            response.assert_status(StatusCode::BAD_REQUEST);
            let text = response.0.into_body().into_string().await.unwrap();
            assert!(text.ends_with(message), "{text}");
        }
        client
            .post("/api/events/run")
            .content_type("application/json")
            .body("{")
            .send()
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn reports_counts_and_event_failures() {
        let response = client(testing::queued_consumer(3))
            .post("/api/events/run")
            .body_json(&json!({"limit":2}))
            .send()
            .await;
        response.assert_status_is_ok();
        response
            .assert_json(json!({"processed":2,"errors":[]}))
            .await;
        for (service, count) in [
            (testing::failing_reservation_consumer(), 1),
            (testing::failing_subscriber_and_process_consumer(), 2),
        ] {
            let response = client(service)
                .post("/api/events/run")
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
            .post("/api/events/run")
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
            let mut request = client.post("/api/events/run").body(r#"{"limit":1}"#);
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
        for path in ["/inbox/run", "/events/run"] {
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
