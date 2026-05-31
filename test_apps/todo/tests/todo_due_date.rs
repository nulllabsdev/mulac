mod utils;
use serde_json::json;
use utils::{
    TodoRow, assert_command_completed, assert_event_completed, assert_not_found_response,
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

async fn update_due_date(base_url: &str, todo_id: Uuid, due_at: &str) -> reqwest::Response {
    utils::client()
        .put(format!("{base_url}/api/todos/{todo_id}/due-date"))
        .json(&json!({"due_at": due_at}))
        .send()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn update_due_date_emits_event() {
    let (base_url, pool, _guard) = start_test_app().await;

    let todo_id = create_todo(&base_url, "Task").await;

    let due_date = "2026-12-31T23:59:59Z";
    let response = update_due_date(&base_url, todo_id, due_date).await;

    assert_ok_response!(response);
    let body = response.json::<serde_json::Value>().await.unwrap();
    assert!(
        body["due_at"]
            .as_str()
            .unwrap()
            .contains("2026-12-31T23:59:59")
    );

    assert_outbox_pending(&pool, "TodoDueDateChanged").await;
    assert_command_completed(&pool, "UpdateDueDate").await;
    assert_event_completed(&pool, "TodoDueDateChanged").await;

    let row: TodoRow = fetch_todo_row(&pool, todo_id).await;
    assert!(row.due_at.is_some());
    assert!(row.due_at.unwrap().to_string().contains("2026-12-31"));
}

#[tokio::test(flavor = "multi_thread")]
async fn update_due_date_nonexistent_todo_returns_404() {
    let (base_url, _pool, _guard) = start_test_app().await;
    let id = Uuid::now_v7();

    let response = update_due_date(&base_url, id, "2026-12-31T23:59:59Z").await;

    assert_not_found_response!(response);
}
