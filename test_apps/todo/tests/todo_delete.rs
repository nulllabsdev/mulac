mod utils;
use serde_json::json;
use utils::{
    assert_command_completed, assert_event_completed, assert_no_content_response,
    assert_not_found_response, assert_ok_response, assert_outbox_pending, client, count_todos,
    start_test_app,
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

async fn delete_todo(base_url: &str, todo_id: Uuid) -> reqwest::Response {
    client()
        .delete(format!("{base_url}/api/todos/{todo_id}"))
        .send()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_todo_emits_event() {
    let (base_url, pool, _guard) = start_test_app().await;

    let todo_id = create_todo(&base_url, "Task").await;
    let response = delete_todo(&base_url, todo_id).await;

    assert_no_content_response!(response);

    let todos_body = client()
        .get(format!("{base_url}/api/todos"))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    let todos = todos_body["items"].as_array().unwrap();
    assert!(
        todos
            .iter()
            .all(|t| t["id"].as_str().unwrap() != todo_id.to_string())
    );

    assert_outbox_pending(&pool, "TodoDeleted").await;
    assert_command_completed(&pool, "DeleteTodo").await;
    assert_event_completed(&pool, "TodoDeleted").await;

    let count = count_todos(&pool, todo_id).await;
    assert_eq!(count, 0, "todo row should be absent after delete");
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_nonexistent_todo_returns_404() {
    let (base_url, _pool, _guard) = start_test_app().await;
    let id = Uuid::now_v7();

    let response = delete_todo(&base_url, id).await;

    assert_not_found_response!(response);
}
