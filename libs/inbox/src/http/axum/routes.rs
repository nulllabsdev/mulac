use super::prelude::{
    InboxAdapter, InboxHttpError, InboxRunResponse, InboxSubmission, RunInbox, SubmitInbox,
};
use crate::http::prelude::AsyncInboxService;
use axum::{Router, extract::State, routing::post};
/// Relative inbox routes. The host owns prefixes, authentication and middleware.
pub fn inbox_routes(service: impl Into<AsyncInboxService>) -> Router {
    Router::new()
        .route("/run", post(run))
        .route("/submit", post(submit))
        .with_state(InboxAdapter::new(service))
}
async fn run(
    State(inbox): State<InboxAdapter>,
    request: RunInbox,
) -> Result<InboxRunResponse, InboxHttpError> {
    inbox.run(request).await
}
async fn submit(
    State(inbox): State<InboxAdapter>,
    request: SubmitInbox,
) -> Result<InboxSubmission, InboxHttpError> {
    inbox.submit(request).await
}
