pub mod io {
    pub use super::InboxApiError;
    pub(crate) use super::{
        err_correlation_id_not_uuid,
        err_limit_out_of_range,
        err_malformed_request,
        err_message_id_not_uuid,
        err_payload_empty,
        err_payload_required,
        err_routing_key_blank,
        err_source_blank,
        err_storage_failed,
        //
    };
    #[cfg(feature = "tokio")]
    pub(crate) use super::{
        err_run_task_failed,
        err_submission_task_failed,
        //
    };
}

use inbox::io::InboxError;

type Cause = Box<dyn std::error::Error + Send + Sync>;

/// Failure of an inbox API operation.
///
/// Display text is safe to return to HTTP clients. Storage and task failures
/// keep their cause as [`std::error::Error::source`] without exposing it.
#[derive(Debug, thiserror::Error)]
pub enum InboxApiError {
    #[error("{0}")]
    MalformedRequest(String),
    #[error("payload is required")]
    PayloadRequired,
    #[error("payload must not be empty")]
    PayloadEmpty,
    #[error("source must contain non-whitespace text")]
    SourceBlank,
    #[error("routing_key must contain non-whitespace text")]
    RoutingKeyBlank,
    #[error("message_id must be a UUID")]
    MessageIdNotUuid(#[source] uuid::Error),
    #[error("correlation_id must be a UUID")]
    CorrelationIdNotUuid(#[source] uuid::Error),
    #[error("limit must be between {min} and {max}")]
    LimitOutOfRange {
        limit: usize,
        min: usize,
        max: usize,
    },
    #[error("inbox message could not be stored")]
    StorageFailed(#[source] InboxError),
    #[error("inbox submission task failed")]
    SubmissionTaskFailed(#[source] Cause),
    #[error("queue processing task failed")]
    RunTaskFailed(#[source] Cause),
}

impl InboxApiError {
    /// Whether the request itself is invalid, as opposed to a server failure.
    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            Self::MalformedRequest(_)
                | Self::PayloadRequired
                | Self::PayloadEmpty
                | Self::SourceBlank
                | Self::RoutingKeyBlank
                | Self::MessageIdNotUuid(_)
                | Self::CorrelationIdNotUuid(_)
                | Self::LimitOutOfRange { .. }
        )
    }
}

pub(crate) fn err_malformed_request(error: serde_json::Error) -> InboxApiError {
    InboxApiError::MalformedRequest(error.to_string())
}

pub(crate) fn err_payload_required() -> InboxApiError {
    InboxApiError::PayloadRequired
}

pub(crate) fn err_payload_empty() -> InboxApiError {
    InboxApiError::PayloadEmpty
}

pub(crate) fn err_source_blank() -> InboxApiError {
    InboxApiError::SourceBlank
}

pub(crate) fn err_routing_key_blank() -> InboxApiError {
    InboxApiError::RoutingKeyBlank
}

pub(crate) fn err_message_id_not_uuid(error: uuid::Error) -> InboxApiError {
    InboxApiError::MessageIdNotUuid(error)
}

pub(crate) fn err_correlation_id_not_uuid(error: uuid::Error) -> InboxApiError {
    InboxApiError::CorrelationIdNotUuid(error)
}

pub(crate) fn err_limit_out_of_range(limit: usize, min: usize, max: usize) -> InboxApiError {
    InboxApiError::LimitOutOfRange { limit, min, max }
}

pub(crate) fn err_storage_failed(error: InboxError) -> InboxApiError {
    InboxApiError::StorageFailed(error)
}

#[cfg(feature = "tokio")]
pub(crate) fn err_submission_task_failed(error: tokio::task::JoinError) -> InboxApiError {
    InboxApiError::SubmissionTaskFailed(Box::new(error))
}

#[cfg(feature = "tokio")]
pub(crate) fn err_run_task_failed(error: tokio::task::JoinError) -> InboxApiError {
    InboxApiError::RunTaskFailed(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn storage_failure_hides_its_cause() {
        let error = err_storage_failed(InboxError::Storage("secret dsn".into()));
        assert_eq!(error.to_string(), "inbox message could not be stored");
        assert!(!error.is_client_error());
        assert!(error.source().unwrap().to_string().contains("secret dsn"));
    }

    #[test]
    fn validation_failures_are_client_errors() {
        let uuid_error = uuid::Uuid::parse_str("x").unwrap_err();
        for error in [
            err_payload_required(),
            err_payload_empty(),
            err_source_blank(),
            err_routing_key_blank(),
            err_message_id_not_uuid(uuid_error.clone()),
            err_correlation_id_not_uuid(uuid_error),
            err_limit_out_of_range(0, 1, 1000),
        ] {
            assert!(error.is_client_error(), "{error}");
        }
    }
}
