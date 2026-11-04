use poem_openapi::{OpenApi, OpenApiService, payload::Json};
use serde_json::Value;

struct Queues;
#[OpenApi]
impl Queues {
    #[oai(path = "/commands/run", method = "post")]
    async fn commands(
        &self,
        request: Json<commanding::prelude::poem::RunRequest>,
    ) -> Json<commanding::prelude::poem::RunResponse> {
        Json(commanding::prelude::poem::RunResponse::from_result::<String>(Ok(request.0.limit)))
    }
    #[oai(path = "/events/run", method = "post")]
    async fn events(
        &self,
        request: Json<eventing::prelude::poem::RunRequest>,
    ) -> Json<eventing::prelude::poem::RunResponse> {
        Json(eventing::prelude::poem::RunResponse::from_result::<String>(
            Ok(request.0.limit),
        ))
    }
    #[oai(path = "/inbox/run", method = "post")]
    async fn inbox(
        &self,
        request: Json<inbox::prelude::poem::RunRequest>,
    ) -> Json<inbox::prelude::poem::RunResponse> {
        Json(inbox::prelude::poem::RunResponse::from_result::<String>(
            Ok(request.0.limit),
        ))
    }
    #[oai(path = "/outbox/run", method = "post")]
    async fn outbox(
        &self,
        request: Json<outbox::prelude::poem::RunRequest>,
    ) -> Json<outbox::prelude::poem::RunResponse> {
        Json(outbox::prelude::poem::RunResponse::from_result::<String>(
            Ok(request.0.limit),
        ))
    }
}
#[test]
fn all_four_queues_share_schema_owners() {
    let spec: Value =
        serde_json::from_str(&OpenApiService::new(Queues, "queues", "1").spec()).unwrap();
    let schemas = spec["components"]["schemas"].as_object().unwrap();
    assert_eq!(
        schemas.keys().collect::<Vec<_>>(),
        ["RunRequest", "RunResponse"]
    );
    for path in ["/commands/run", "/events/run", "/inbox/run", "/outbox/run"] {
        let operation = &spec["paths"][path]["post"];
        assert_eq!(
            operation["requestBody"]["content"]["application/json; charset=utf-8"]["schema"]["$ref"],
            "#/components/schemas/RunRequest"
        );
        assert_eq!(
            operation["responses"]["200"]["content"]["application/json; charset=utf-8"]["schema"]["$ref"],
            "#/components/schemas/RunResponse"
        );
    }
    assert_eq!(schemas["RunRequest"]["properties"]["limit"]["minimum"], 1.0);
    assert_eq!(
        schemas["RunRequest"]["properties"]["limit"]["maximum"],
        1000.0
    );
}
