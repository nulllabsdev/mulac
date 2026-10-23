//! OpenAPI schemas for the adapter types (feature `utoipa`).
//!
//! The extractor and response types report the schemas of private twins, so
//! `request_body = SubmitInbox` publishes `SubmitInboxRequest`.
use crate::adapter::io::{
    InboxRunResponse,
    InboxSubmission,
    //
};
#[cfg(feature = "commanding")]
use crate::commanding::io::{
    CommandsRunResponse,
    RunCommands,
    //
};
#[cfg(feature = "eventing")]
use crate::eventing::io::{
    EventsRunResponse,
    RunEvents,
    //
};
use crate::extract::io::{
    RunInbox,
    SubmitInbox,
    //
};
#[cfg(feature = "outbox")]
use crate::outbox::io::{
    OutboxRunResponse,
    RunOutbox,
    //
};
use std::borrow::Cow;
use utoipa::openapi::{RefOr, schema::Schema};
use utoipa::{PartialSchema, ToSchema};

macro_rules! schema_as {
    ($adapter:ty, $schema:ty) => {
        impl PartialSchema for $adapter {
            fn schema() -> RefOr<Schema> {
                <$schema as PartialSchema>::schema()
            }
        }

        impl ToSchema for $adapter {
            fn name() -> Cow<'static, str> {
                <$schema as ToSchema>::name()
            }

            fn schemas(schemas: &mut Vec<(String, RefOr<Schema>)>) {
                <$schema as ToSchema>::schemas(schemas)
            }
        }
    };
}

schema_as!(SubmitInbox, twins::SubmitInboxRequest);
schema_as!(InboxSubmission, twins::SubmitInboxResponse);
schema_as!(RunInbox, twins::RunRequest);
schema_as!(InboxRunResponse, twins::RunResponse);
// Every queue publishes the same twins, so the names never collide.
#[cfg(feature = "commanding")]
schema_as!(RunCommands, twins::RunRequest);
#[cfg(feature = "commanding")]
schema_as!(CommandsRunResponse, twins::RunResponse);
#[cfg(feature = "eventing")]
schema_as!(RunEvents, twins::RunRequest);
#[cfg(feature = "eventing")]
schema_as!(EventsRunResponse, twins::RunResponse);
#[cfg(feature = "outbox")]
schema_as!(RunOutbox, twins::RunRequest);
#[cfg(feature = "outbox")]
schema_as!(OutboxRunResponse, twins::RunResponse);

#[allow(dead_code)]
mod twins {
    use serde_json::Value;
    use utoipa::ToSchema;

    #[derive(ToSchema)]
    pub struct SubmitInboxRequest {
        /// Required nonempty JSON value; null, blank strings, empty arrays and objects are rejected.
        pub payload: Value,
        /// Command type used during inbox processing; must contain non-whitespace text.
        pub routing_key: String,
        /// UUID used for deduplication. Generated when omitted.
        pub message_id: Option<String>,
        /// Optional correlation UUID, preserved with the message.
        pub correlation_id: Option<String>,
        /// Message origin; must contain non-whitespace text.
        pub source: String,
    }

    #[derive(ToSchema)]
    pub struct SubmitInboxResponse {
        pub message_id: String,
    }

    #[derive(ToSchema)]
    pub struct RunRequest {
        /// Maximum number of eligible items to process in this queue only.
        #[schema(minimum = 1, maximum = 1000)]
        pub limit: usize,
    }

    #[derive(ToSchema)]
    pub struct RunResponse {
        /// Number processed on full success; null if any item failed.
        pub processed: Option<usize>,
        pub errors: Vec<String>,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[cfg(feature = "commanding")]
    #[utoipa::path(post, path = "/commands/run", request_body = RunCommands,
        responses((status = 200, body = CommandsRunResponse)))]
    #[allow(dead_code)]
    fn run_commands() {}

    #[cfg(feature = "commanding")]
    #[derive(OpenApi)]
    #[openapi(paths(run, run_commands))]
    struct QueueDoc;

    #[cfg(feature = "eventing")]
    #[utoipa::path(post, path = "/events/run", request_body = RunEvents,
        responses((status = 200, body = EventsRunResponse)))]
    #[allow(dead_code)]
    fn run_events() {}

    #[cfg(feature = "eventing")]
    #[derive(OpenApi)]
    #[openapi(paths(run, run_events))]
    struct EventQueueDoc;

    #[cfg(feature = "outbox")]
    #[utoipa::path(post, path = "/outbox/run", request_body = RunOutbox,
        responses((status = 200, body = OutboxRunResponse)))]
    #[allow(dead_code)]
    fn run_outbox() {}

    #[cfg(feature = "outbox")]
    #[derive(OpenApi)]
    #[openapi(paths(run, run_outbox))]
    struct OutboxQueueDoc;

    #[cfg(any(feature = "commanding", feature = "eventing", feature = "outbox"))]
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

    #[cfg(feature = "eventing")]
    #[test]
    fn event_queue_shares_run_schemas() {
        let spec: Value = serde_json::to_value(EventQueueDoc::openapi()).unwrap();
        assert_queues_share_run_schemas(spec, &["/inbox/run", "/events/run"]);
    }

    #[cfg(feature = "outbox")]
    #[test]
    fn outbox_queue_shares_run_schemas() {
        let spec: Value = serde_json::to_value(OutboxQueueDoc::openapi()).unwrap();
        assert_queues_share_run_schemas(spec, &["/inbox/run", "/outbox/run"]);
    }

    #[cfg(feature = "commanding")]
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
}
