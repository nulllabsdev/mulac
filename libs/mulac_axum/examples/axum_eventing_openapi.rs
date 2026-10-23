//! Axum event batch route with an application-owned Utoipa annotation.
//!
//! The application owns the event consumer. This example uses a test double
//! from `eventing_api`'s `testing` feature; a real service passes
//! `PersistentKernelHandle::event_consumer()` to `EventingService::new`.
//!
//! ```sh
//! cargo run -p mulac_axum --features eventing,utoipa --example axum_eventing_openapi
//! ```
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use eventing_api::io::testing;
use mulac_axum::io::{
    EventingAdapter,
    EventingHttpError,
    EventsRunResponse,
    RunEvents,
    //
};
use utoipa::OpenApi;

/// Dispatch one batch of due events to the registered subscribers.
#[utoipa::path(
    post,
    path = "/events/run",
    request_body = RunEvents,
    responses(
        (status = 200, body = EventsRunResponse),
        (status = 400, body = String),
        (status = 500, body = String),
    ),
)]
async fn run_events(
    State(events): State<EventingAdapter>,
    request: RunEvents,
) -> Result<EventsRunResponse, EventingHttpError> {
    events.run(request).await
}

#[derive(OpenApi)]
#[openapi(paths(run_events), servers((url = "/api")))]
struct ApiDoc;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let events = EventingAdapter::new(testing::queued_service(10));
    let routes = Router::new()
        .route("/events/run", post(run_events))
        .with_state(events);
    let app = Router::new()
        .route(
            "/api/openapi.json",
            get(|| async { Json(ApiDoc::openapi()) }),
        )
        .nest("/api", routes);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    axum::serve(listener, app).await
}
