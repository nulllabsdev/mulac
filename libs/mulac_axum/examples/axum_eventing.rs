//! Axum service exposing the event batch route under `/api`, without OpenAPI.
//!
//! The application owns the event consumer. This example uses a test double
//! from `eventing_api`'s `testing` feature; a real service passes
//! `PersistentKernelHandle::event_consumer()` to `EventingService::new`.
//!
//! ```sh
//! cargo run -p mulac_axum --features eventing --example axum_eventing
//! ```
use axum::Router;
use axum::extract::State;
use axum::routing::post;
use eventing_api::io::testing;
use mulac_axum::io::{
    EventingAdapter,
    EventingHttpError,
    EventsRunResponse,
    RunEvents,
    //
};

async fn run_events(
    State(events): State<EventingAdapter>,
    request: RunEvents,
) -> Result<EventsRunResponse, EventingHttpError> {
    events.run(request).await
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let events = EventingAdapter::new(testing::queued_service(10));
    let routes = Router::new()
        .route("/events/run", post(run_events))
        .with_state(events);
    let app = Router::new().nest("/api", routes);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    axum::serve(listener, app).await
}
