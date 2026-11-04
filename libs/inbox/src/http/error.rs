pub mod prelude {
    pub use super::InboxApiError;
    pub(crate) use super::QUEUE;
    pub(crate) use super::{
        err_correlation_id_not_uuid,
        err_limit_out_of_range,
        err_malformed_request,
        err_message_id_not_uuid,
        err_payload_empty,
        err_payload_required,
        err_routing_key_blank,
        err_source_blank,
        //
    };
    #[cfg(feature = "api-tokio")]
    pub(crate) use super::{
        err_run_task_failed,
        err_storage_failed,
        err_submission_task_failed,
        //
    };
}

#[cfg(feature = "api-tokio")]
use crate::prelude::InboxError;
use mulac_http::prelude::ReportedError;

/// Queue name logged with this crate's reported failures.
pub(crate) const QUEUE: &str = "inbox";

/// Failure of an inbox API operation.
///
/// Display text is safe to return to HTTP clients. Unparsable bodies and server
/// failures display only an error ID: the cause is logged under that ID when the
/// error is created, and stays available as [`std::error::Error::source`].
#[derive(Debug, thiserror::Error)]
pub enum InboxApiError {
    #[error(transparent)]
    MalformedRequest(ReportedError),
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
    #[error(transparent)]
    StorageFailed(ReportedError),
    #[error(transparent)]
    SubmissionTaskFailed(ReportedError),
    #[error(transparent)]
    RunTaskFailed(ReportedError),
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
    InboxApiError::MalformedRequest(ReportedError::client(QUEUE, error))
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

#[cfg(feature = "api-tokio")]
pub(crate) fn err_storage_failed(error: InboxError) -> InboxApiError {
    InboxApiError::StorageFailed(ReportedError::server(QUEUE, error))
}

#[cfg(feature = "api-tokio")]
pub(crate) fn err_submission_task_failed(error: tokio::task::JoinError) -> InboxApiError {
    InboxApiError::SubmissionTaskFailed(ReportedError::server(QUEUE, error))
}

#[cfg(feature = "api-tokio")]
pub(crate) fn err_run_task_failed(error: tokio::task::JoinError) -> InboxApiError {
    InboxApiError::RunTaskFailed(ReportedError::server(QUEUE, error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "api-tokio")]
    #[test]
    fn storage_failure_hides_its_cause() {
        use std::error::Error;

        let error = err_storage_failed(InboxError::Storage("secret dsn".into()));
        assert!(uuid::Uuid::parse_str(&error.to_string()).is_ok(), "{error}");
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
