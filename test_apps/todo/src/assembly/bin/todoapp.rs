use poem::{
    EndpointExt,
    Route,
    get,
    handler,
    listener::TcpListener,
    middleware::AddData, //
};
use poem_openapi::OpenApiService;
use std::env;
use test_app_todo::io::{
    AppState,
    CompleteApi,
    CreateApi,
    DeleteApi,
    DueDatesApi,
    GetApi,
    InboxApi,
    ListApi,
    OutboxApi,
    ReopenApi,
    UpdateApi,
    build_pool,
    run_command_worker,
    run_event_worker,
    run_migrations,
    start_mulac, //
};

#[handler]
fn health() -> &'static str {
    "ok"
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Structured logging to stdout, level controlled via RUST_LOG env var.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let command = env::args().nth(1).unwrap_or_else(|| "serve".to_string());
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    match command.as_str() {
        // Run pending diesel migrations and exit — useful for one-shot init containers.
        "migrate" => {
            run_migrations(&database_url)?;
            println!("migrations applied");
        }

        "serve" => {
            let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:33001".to_string());

            // Build the connection pool and migrate on startup so the binary is self-contained.
            run_migrations(&database_url)?;
            let pool = build_pool(&database_url)?;

            // Boot the in-process kernel and register all command handlers and event subscribers.
            let kernel = start_mulac(pool.clone()).await?;
            let token = kernel.child_token();
            tokio::spawn(run_command_worker(kernel.command_consumer(), token.clone()));
            tokio::spawn(run_event_worker(kernel.event_consumer(), token));
            let state = AppState::new(pool, kernel.state());

            // Assemble the OpenAPI service from individual feature API structs.
            let api = OpenApiService::new(
                (
                    CreateApi,
                    ListApi,
                    GetApi,
                    UpdateApi,
                    CompleteApi,
                    ReopenApi,
                    DeleteApi,
                    DueDatesApi,
                    InboxApi,
                    OutboxApi,
                ),
                "test_app_todo",
                "0.1.0",
            )
            .server(format!("http://{bind_addr}/api"));

            let swagger = api.swagger_ui();
            let app = Route::new()
                .at("/health", get(health))
                .nest("/api", api)
                .nest("/swagger", swagger)
                .with(AddData::new(state));

            tracing::info!(%bind_addr, "starting test_app_todo");

            // Run until Ctrl-C; signal the kernel to drain in-flight work before exiting.
            tokio::select! {
                result = poem::Server::new(TcpListener::bind(bind_addr)).run(app) => result?,
                _ = tokio::signal::ctrl_c() => kernel.shutdown(),
            }
            kernel.wait().await?;
        }

        other => anyhow::bail!("unknown command `{other}`; expected `serve` or `migrate`"),
    }

    Ok(())
}
