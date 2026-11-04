use super::adapter::prelude::{OutboxAdapter, OutboxHttpError, OutboxRunResponse, RunOutbox};
use crate::http::prelude::AsyncOutboxService;
use axum::Router;
use axum::extract::State;
use axum::routing::post;

/// Build queue-processing routes with `POST /run`.
///
/// Mount under an application-owned prefix (for example `/outbox`). The host
/// must apply its own authentication, authorization, and other middleware.
/// This publishes queued entries; it does not record new ones.
///
/// ```ignore
/// let consumer = Arc::new(OutboxConsumer::new(repository, publisher));
/// let app = Router::new().nest("/outbox", outbox_routes(consumer));
/// ```
pub fn outbox_routes(service: impl Into<AsyncOutboxService>) -> Router {
    Router::new()
        .route("/run", post(run_outbox))
        .with_state(OutboxAdapter::new(service))
}

async fn run_outbox(
    State(outbox): State<OutboxAdapter>,
    request: RunOutbox,
) -> Result<OutboxRunResponse, OutboxHttpError> {
    outbox.run(request).await
}
