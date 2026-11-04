use super::adapter::prelude::{EventingAdapter, EventingHttpError, EventsRunResponse, RunEvents};
use crate::http::prelude::AsyncEventingService;
use axum::Router;
use axum::extract::State;
use axum::routing::post;

/// Build queue-processing routes with `POST /run`.
///
/// Mount under an application-owned prefix (for example `/events`). The host
/// must apply its own authentication, authorization, and other middleware.
/// This processes queued events; it does not submit new events.
///
/// ```ignore
/// let app = Router::new().nest("/events", event_routes(kernel.event_consumer()));
/// ```
pub fn event_routes(service: impl Into<AsyncEventingService>) -> Router {
    Router::new()
        .route("/run", post(run_eventing))
        .with_state(EventingAdapter::new(service))
}

async fn run_eventing(
    State(events): State<EventingAdapter>,
    request: RunEvents,
) -> Result<EventsRunResponse, EventingHttpError> {
    events.run(request).await
}
