//! Poem service exposing the outbox batch endpoint under `/api`.
//!
//! The application owns the publisher. This example logs each message instead
//! of sending it to a broker. Without `DATABASE_URL` the endpoint answers `503`.
//!
//! ```sh
//! DATABASE_URL=postgres://... cargo run -p mulac_poem --features outbox --example poem_outbox
//! ```
use mulac_poem::io::{
    OutboxAdapter,
    RunRequest,
    RunResponse,
    //
};
use outbox::io::{
    OutboundMessageEnvelope,
    OutboxError,
    OutboxPublisherPort,
    //
};
use outbox_api::io::OutboxService;
use poem::{Result, Route, Server, listener::TcpListener};
use poem_openapi::{OpenApi, OpenApiService, payload::Json};
use std::sync::Arc;

struct LoggingPublisher;

impl OutboxPublisherPort for LoggingPublisher {
    fn publish(&self, envelope: OutboundMessageEnvelope) -> Result<(), OutboxError> {
        println!("publish {}", envelope.metadata.routing_key);
        Ok(())
    }
}

struct OutboxApi {
    adapter: OutboxAdapter,
}

#[OpenApi]
impl OutboxApi {
    /// Publish one batch; returns 503 when delivery is not configured.
    #[oai(path = "/outbox/run", method = "post")]
    async fn run_outbox(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        self.adapter.run(request).await
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let adapter = match std::env::var("DATABASE_URL") {
        Ok(database_url) => {
            let pool = outbox::io::build_pool(&database_url).map_err(std::io::Error::other)?;
            OutboxAdapter::new(OutboxService::from_postgres(
                pool,
                Arc::new(LoggingPublisher),
            ))
        }
        Err(_) => OutboxAdapter::unconfigured(),
    };
    let api = OpenApiService::new(OutboxApi { adapter }, "Outbox", "1").server("/api");
    let app = Route::new()
        .at("/api/openapi.json", api.spec_endpoint())
        .nest("/api", api);
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    Server::new(TcpListener::bind(format!("0.0.0.0:{port}")))
        .run(app)
        .await
}
