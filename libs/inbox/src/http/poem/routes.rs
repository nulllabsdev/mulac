use super::prelude::{InboxAdapter, InboxSubmission, RunRequest, RunResponse, SubmitInboxRequest};
use crate::http::prelude::AsyncInboxService;
use poem::Result;
use poem_openapi::{OpenApi, payload::Json};
/// Relative inbox endpoints. The host owns prefixes, authentication and middleware.
pub struct InboxApi {
    adapter: InboxAdapter,
}
impl InboxApi {
    pub fn new(service: impl Into<AsyncInboxService>) -> Self {
        Self {
            adapter: InboxAdapter::new(service),
        }
    }
}
#[OpenApi]
impl InboxApi {
    #[oai(path = "/run", method = "post")]
    async fn run(&self, request: Json<RunRequest>) -> Result<Json<RunResponse>> {
        self.adapter.run(request).await
    }
    #[oai(path = "/submit", method = "post")]
    async fn submit(&self, request: Json<SubmitInboxRequest>) -> Result<InboxSubmission> {
        self.adapter.submit(request).await
    }
}
