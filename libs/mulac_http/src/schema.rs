use serde_json::Value;
use utoipa::ToSchema;

#[derive(ToSchema)]
pub struct SubmitInboxRequest {
    /// Required nonempty JSON value; null, blank strings, empty arrays and objects are rejected.
    pub payload: Value,
    /// Command type used during inbox processing; must contain non-whitespace text.
    pub routing_key: String,
    /// UUID used for deduplication. Generated when omitted.
    pub message_id: Option<String>,
    /// Optional correlation UUID, preserved with the message.
    pub correlation_id: Option<String>,
    /// Message origin; must contain non-whitespace text.
    pub source: String,
}

#[derive(ToSchema)]
pub struct SubmitInboxResponse {
    pub message_id: String,
}

#[derive(ToSchema)]
pub struct RunRequest {
    /// Maximum number of eligible items to process in this queue only.
    #[schema(minimum = 1, maximum = 1000)]
    pub limit: usize,
}

#[derive(ToSchema)]
pub struct RunResponse {
    /// Number processed on full success; null if any item failed.
    pub processed: Option<usize>,
    /// One error ID per failure; each ID is logged with the full error.
    pub errors: Vec<String>,
}

pub mod prelude {
    pub use super::{RunRequest, RunResponse, SubmitInboxRequest, SubmitInboxResponse};
}
