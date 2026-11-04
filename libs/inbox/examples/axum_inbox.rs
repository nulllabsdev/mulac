//! Axum service exposing the inbox routes under `/api`, without OpenAPI.
//!
//! ```sh
//! DATABASE_URL=postgres://... cargo run -p mulac-inbox --features axum,diesel --example axum_inbox
//! ```
use axum::Router;
use mulac_inbox::prelude::AsyncInboxService;
use mulac_inbox::prelude::axum::inbox_routes;
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    let pool = mulac_inbox::prelude::build_pool(&database_url).map_err(std::io::Error::other)?;
    let app = Router::new().nest(
        "/api/inbox",
        inbox_routes(AsyncInboxService::from_postgres(pool)),
    );

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    axum::serve(listener, app).await
}
