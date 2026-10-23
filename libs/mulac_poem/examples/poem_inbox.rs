//! Poem service exposing the inbox endpoints under `/api`.
//!
//! ```sh
//! DATABASE_URL=postgres://... cargo run -p mulac_poem --example poem_inbox
//! ```
use inbox_api::io::InboxService;
use mulac_poem::io::{
    InboxAdapter,
    InboxSubmission,
    RunRequest,
    RunResponse,
    SubmitInboxRequest,
    //
};
use poem::{Result, Route, Server, listener::TcpListener};
use poem_openapi::{OpenApi, OpenApiService, payload::Json};

struct InboxApi {
    adapter: InboxAdapter,
}

#[OpenApi]
impl InboxApi {
    /// Store one message without processing it. Unknown routing keys are accepted.
    #[oai(path = "/inbox", method = "post")]
    async fn submit(&self, request: Json<SubmitInboxRequest>) -> Result<InboxSubmission> {
        self.adapter.submit(request).await
    }

    /// Queue commands from stored inbox messages, without executing them.
    #[oai(path = "/inbox/run", method = "post")]
    async fn run(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        self.adapter.run(request).await
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    let pool = inbox::io::build_pool(&database_url).map_err(std::io::Error::other)?;
    let adapter = InboxAdapter::new(InboxService::from_postgres(pool));
    let api = OpenApiService::new(InboxApi { adapter }, "Inbox", "1").server("/api");
    let app = Route::new()
        .at("/api/openapi.json", api.spec_endpoint())
        .nest("/api", api);
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    Server::new(TcpListener::bind(format!("0.0.0.0:{port}")))
        .run(app)
        .await
}
