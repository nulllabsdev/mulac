mod utils;
use serde_json::json;
use utils::{
    TodoRow, assert_command_completed, assert_event_completed, assert_not_found_response,
    assert_ok_response, assert_outbox_pending, client, fetch_todo_row, start_test_app,
};
use uuid::Uuid;

async fn create_todo(base_url: &str, title: &str) -> Uuid {
    let resp = client()
        .post(format!("{base_url}/api/todos"))
        .json(&json!({"title": title}))
        .send()
        .await
        .unwrap();
    assert_ok_response!(resp);
    resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

async fn complete_todo(base_url: &str, todo_id: Uuid) {
    let resp = client()
        .post(format!("{base_url}/api/todos/{todo_id}/complete"))
        .send()
        .await
        .unwrap();
    assert_ok_response!(resp);
}

async fn reopen_todo(base_url: &str, todo_id: Uuid) -> reqwest::Response {
    client()
        .post(format!("{base_url}/api/todos/{todo_id}/reopen"))
        .send()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn reopen_todo_emits_event() {
    let (base_url, pool, _guard) = start_test_app().await;

    let todo_id = create_todo(&base_url, "Task").await;
    complete_todo(&base_url, todo_id).await;

    let response = reopen_todo(&base_url, todo_id).await;

    assert_ok_response!(response);
    let body = response.json::<serde_json::Value>().await.unwrap();
    assert_eq!(body["status"], "active");

    assert_outbox_pending(&pool, "TodoReopened").await;
    assert_command_completed(&pool, "ReopenTodo").await;
    assert_event_completed(&pool, "TodoReopened").await;

    let row: TodoRow = fetch_todo_row(&pool, todo_id).await;
    assert_eq!(row.status, "active");
}

#[tokio::test(flavor = "multi_thread")]
async fn reopen_nonexistent_todo_returns_404() {
    let (base_url, _pool, _guard) = start_test_app().await;
    let id = Uuid::now_v7();

    let response = reopen_todo(&base_url, id).await;

    assert_not_found_response!(response);
}
