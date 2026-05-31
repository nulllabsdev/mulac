mod utils;
use serde_json::json;
use utils::{
    TodoRow, assert_bad_request_response, assert_command_completed, assert_event_completed,
    assert_ok_response, assert_outbox_pending, fetch_todo_row, start_test_app,
};
use uuid::Uuid;

async fn create_todo(base_url: &str, title: &str) -> Uuid {
    utils::client()
        .post(format!("{base_url}/api/todos"))
        .json(&json!({"title": title}))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

async fn update_todo(
    base_url: &str,
    todo_id: Uuid,
    title: &str,
    description: &str,
) -> reqwest::Response {
    utils::client()
        .put(format!("{base_url}/api/todos/{todo_id}"))
        .json(&json!({"title": title, "description": description}))
        .send()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn update_todo_dispatches_command_and_emits_event() {
    let (base_url, pool, _guard) = start_test_app().await;

    let todo_id = create_todo(&base_url, "Original").await;

    let update_response = update_todo(&base_url, todo_id, "Updated", "Updated desc").await;

    assert_ok_response!(update_response);

    assert_outbox_pending(&pool, "TodoUpdated").await;
    assert_command_completed(&pool, "UpdateTodo").await;
    assert_event_completed(&pool, "TodoUpdated").await;

    let row: TodoRow = fetch_todo_row(&pool, todo_id).await;
    assert_eq!(row.title, "Updated");
    assert_eq!(row.description.as_deref(), Some("Updated desc"));
}

#[tokio::test(flavor = "multi_thread")]
async fn update_todo_with_blank_title_returns_400() {
    let (base_url, _pool, _guard) = start_test_app().await;

    let todo_id = create_todo(&base_url, "Task").await;
    let response = update_todo(&base_url, todo_id, "  ", "").await;

    assert_bad_request_response!(response);
}
