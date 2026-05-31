mod utils;
use serde_json::json;
use utils::{
    TodoRow,
    assert_bad_request_response,
    assert_command_completed,
    assert_event_completed,
    assert_ok_response,
    assert_outbox_pending,
    fetch_command_entries,
    fetch_event_entries,
    fetch_inbox,
    fetch_outbox,
    fetch_todo_row,
    start_test_app, //
};
use uuid::Uuid;

async fn create_todo(base_url: &str, title: &str, description: &str) -> serde_json::Value {
    let resp = utils::client()
        .post(format!("{base_url}/api/todos"))
        .json(&json!({
            "title": title,
            "description": description
        }))
        .send()
        .await
        .unwrap();
    assert_ok_response!(resp);
    resp.json::<serde_json::Value>().await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn create_todo_returns_todo() {
    let (base_url, pool, _guard) = start_test_app().await;

    let body = create_todo(&base_url, "Buy milk", "From the corner store").await;

    assert_eq!(body["title"], "Buy milk");
    assert_eq!(body["description"], "From the corner store");
    assert_eq!(body["status"], "active");
    assert!(body["id"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(body["created_at"].is_string());

    let todo_id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    let row: TodoRow = fetch_todo_row(&pool, todo_id).await;

    assert_eq!(row.id, todo_id);
    assert_eq!(row.title, "Buy milk");
    assert_eq!(row.description.as_deref(), Some("From the corner store"));
    assert_eq!(row.status, "active");
    assert!(row.due_at.is_none());

    assert_outbox_pending(&pool, "TodoCreated").await;
    let outbox = fetch_outbox(&pool).await;
    let event = &outbox[0];
    assert_eq!(event.attempts, 0);
    assert!(event.published_at.is_none());
    assert_eq!(event.payload["type"], "TodoCreated");
    let todo_payload = &event.payload["payload"]["todo"];
    assert_eq!(todo_payload["id"], body["id"]);
    assert_eq!(todo_payload["title"], "Buy milk");
    assert_eq!(todo_payload["description"], "From the corner store");
    assert_eq!(todo_payload["status"], "active");
    assert!(todo_payload["created_at"].is_string());
    assert!(todo_payload["updated_at"].is_string());
    assert!(todo_payload["due_at"].is_null());

    assert_command_completed(&pool, "CreateTodo").await;
    let commands = fetch_command_entries(&pool).await;
    assert_eq!(commands.len(), 1);
    let command = &commands[0];
    assert_eq!(command.attempts, 1);
    assert!(command.reservation_id.is_none());
    assert!(command.processed_at.is_some());
    assert!(command.extra_info.is_none());
    let command_payload: serde_json::Value = serde_json::from_str(&command.payload).unwrap();
    assert_eq!(command_payload["todo_id"], body["id"]);
    assert_eq!(command_payload["title"], "Buy milk");
    assert_eq!(command_payload["description"], "From the corner store");
    let command_meta = command.meta.as_ref().unwrap();
    assert!(command_meta["command_id"].is_string());
    assert!(command_meta["correlation_id"].is_string());
    assert!(command_meta["causation_id"].is_null());
    assert_eq!(command_meta["source"], "test_app_todo.http");

    assert_event_completed(&pool, "TodoCreated").await;
    let events = fetch_event_entries(&pool).await;
    assert_eq!(events.len(), 1);
    let event_entry = &events[0];
    assert_eq!(event_entry.attempts, 1);
    assert!(event_entry.reservation_id.is_none());
    assert!(event_entry.processed_at.is_some());
    assert!(event_entry.extra_info.is_none());
    let event_payload: serde_json::Value = serde_json::from_str(&event_entry.payload).unwrap();
    assert_eq!(event_payload["type"], "TodoCreated");
    assert_eq!(event_payload["payload"]["todo"]["id"], body["id"]);
    let event_meta = event_entry.meta.as_ref().unwrap();
    assert!(event_meta["event_id"].is_string());
    assert!(event_meta["correlation_id"].is_string());
    assert!(event_meta["causation_id"].is_string());

    let inbox = fetch_inbox(&pool).await;
    assert_eq!(inbox.len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn create_todo_with_blank_title_returns_400() {
    let (base_url, _pool, _guard) = start_test_app().await;

    let resp = utils::client()
        .post(format!("{base_url}/api/todos"))
        .json(&json!({"title": "   "}))
        .send()
        .await
        .unwrap();

    assert_bad_request_response!(resp);
    let body = resp.json::<serde_json::Value>().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("blank"));
}
