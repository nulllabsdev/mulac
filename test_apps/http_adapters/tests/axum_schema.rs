mod tests {
    use commanding::prelude::axum::*;
    use eventing::prelude::axum::*;
    use inbox::prelude::axum::*;
    use outbox::prelude::axum::*;
    use serde_json::{Value, json};
    use utoipa::OpenApi;

    #[utoipa::path(post, path = "/inbox", request_body = SubmitInbox,
        responses((status = 202, body = InboxSubmission)))]
    #[allow(dead_code)]
    fn submit() {}

    #[utoipa::path(post, path = "/inbox/run", request_body = RunInbox,
        responses((status = 200, body = InboxRunResponse)))]
    #[allow(dead_code)]
    fn run() {}

    #[derive(OpenApi)]
    #[openapi(paths(submit, run))]
    struct ApiDoc;

    #[utoipa::path(post, path = "/commands/run", request_body = RunCommands,
        responses((status = 200, body = CommandsRunResponse)))]
    #[allow(dead_code)]
    fn run_commands() {}

    #[derive(OpenApi)]
    #[openapi(paths(run, run_commands))]
    struct QueueDoc;

    #[utoipa::path(post, path = "/events/run", request_body = RunEvents,
        responses((status = 200, body = EventsRunResponse)))]
    #[allow(dead_code)]
    fn run_events() {}

    #[derive(OpenApi)]
    #[openapi(paths(run, run_events))]
    struct EventQueueDoc;

    #[utoipa::path(post, path = "/outbox/run", request_body = RunOutbox,
        responses((status = 200, body = OutboxRunResponse)))]
    #[allow(dead_code)]
    fn run_outbox() {}

    #[derive(OpenApi)]
    #[openapi(paths(run, run_outbox))]
    struct OutboxQueueDoc;

    fn assert_queues_share_run_schemas(spec: Value, paths: &[&str]) {
        for path in paths {
            let run = &spec["paths"][path]["post"];
            assert_eq!(
                run["requestBody"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/RunRequest"
            );
            assert_eq!(
                run["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/RunResponse"
            );
        }
        let schemas: Vec<_> = spec["components"]["schemas"]
            .as_object()
            .unwrap()
            .keys()
            .collect();
        assert_eq!(schemas, vec!["RunRequest", "RunResponse"]);
    }

    #[test]
    fn event_queue_shares_run_schemas() {
        let spec: Value = serde_json::to_value(EventQueueDoc::openapi()).unwrap();
        assert_queues_share_run_schemas(spec, &["/inbox/run", "/events/run"]);
    }

    #[test]
    fn outbox_queue_shares_run_schemas() {
        let spec: Value = serde_json::to_value(OutboxQueueDoc::openapi()).unwrap();
        assert_queues_share_run_schemas(spec, &["/inbox/run", "/outbox/run"]);
    }

    #[test]
    fn queues_share_run_schemas() {
        let spec: Value = serde_json::to_value(QueueDoc::openapi()).unwrap();
        assert_queues_share_run_schemas(spec, &["/inbox/run", "/commands/run"]);
    }

    #[test]
    fn publishes_shared_schema_names_and_bounds() {
        let spec: Value = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let submission = &spec["paths"]["/inbox"]["post"];
        assert_eq!(
            submission["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/SubmitInboxRequest"
        );
        assert_eq!(
            submission["responses"]["202"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/SubmitInboxResponse"
        );
        let schemas = &spec["components"]["schemas"];
        assert_eq!(
            schemas["SubmitInboxRequest"]["required"],
            json!(["payload", "routing_key", "source"])
        );
        let limit = &schemas["RunRequest"]["properties"]["limit"];
        assert_eq!(limit["minimum"], 1);
        assert_eq!(limit["maximum"], 1000);
        assert!(schemas["RunResponse"].is_object());
    }
    #[derive(OpenApi)]
    #[openapi(paths(run, run_commands, run_events, run_outbox))]
    struct AllQueuesDoc;

    #[test]
    fn all_four_queues_share_schema_owners() {
        assert_queues_share_run_schemas(
            serde_json::to_value(AllQueuesDoc::openapi()).unwrap(),
            &["/inbox/run", "/commands/run", "/events/run", "/outbox/run"],
        );
    }
}
