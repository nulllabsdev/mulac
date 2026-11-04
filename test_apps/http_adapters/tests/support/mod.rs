//! Assertions for responses that report failures by error ID.
#![allow(dead_code)]

use serde_json::Value;
use uuid::Uuid;

pub fn assert_is_error_id(text: &str) {
    assert!(
        Uuid::parse_str(text).is_ok(),
        "expected only an error ID, got {text:?}"
    );
}

pub fn assert_batch_failed_with_ids(body: &Value, count: usize) {
    assert!(body["processed"].is_null(), "{body}");
    let errors = body["errors"].as_array().expect("errors array");
    assert_eq!(errors.len(), count, "{body}");
    for id in errors {
        assert_is_error_id(id.as_str().expect("error ID string"));
    }
}
