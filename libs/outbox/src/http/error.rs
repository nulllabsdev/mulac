pub mod prelude {
    pub(crate) use super::QUEUE;
    pub use super::{
        OutboxApiError,
        OutboxApiErrorKind,
        //
    };
    pub(crate) use super::{
        err_limit_out_of_range,
        err_malformed_request,
        //
    };
    #[cfg(feature = "api-tokio")]
    pub(crate) use super::{
        err_publisher_not_configured,
        err_run_task_failed,
        //
    };
}

use mulac_http::prelude::ReportedError;

/// Queue name logged with this crate's reported failures.
pub(crate) const QUEUE: &str = "outbox";

/// Failure of an outbox API operation.
///
/// Display text is safe to return to HTTP clients. Unparsable bodies and server
/// failures display only an error ID: the cause is logged under that ID when the
/// error is created, and stays available as [`std::error::Error::source`].
#[derive(Debug, thiserror::Error)]
pub enum OutboxApiError {
    #[error(transparent)]
    MalformedRequest(ReportedError),
    #[error("limit must be between {min} and {max}")]
    LimitOutOfRange {
        limit: usize,
        min: usize,
        max: usize,
    },
    #[error("outbox publisher is not configured")]
    PublisherNotConfigured,
    #[error(transparent)]
    RunTaskFailed(ReportedError),
}

/// Who an [`OutboxApiError`] is attributed to; adapters map it to a status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxApiErrorKind {
    /// The request itself is invalid (`400`).
    Invalid,
    /// The application has not configured delivery (`503`).
    Unavailable,
    /// A sanitized server failure (`500`).
    Internal,
}

impl OutboxApiError {
    pub fn kind(&self) -> OutboxApiErrorKind {
        match self {
            Self::MalformedRequest(_) | Self::LimitOutOfRange { .. } => OutboxApiErrorKind::Invalid,
            Self::PublisherNotConfigured => OutboxApiErrorKind::Unavailable,
            Self::RunTaskFailed(_) => OutboxApiErrorKind::Internal,
        }
    }

    /// Whether the request itself is invalid, as opposed to an unavailable
    /// publisher or a server failure.
    pub fn is_client_error(&self) -> bool {
        self.kind() == OutboxApiErrorKind::Invalid
    }
}

pub(crate) fn err_malformed_request(error: serde_json::Error) -> OutboxApiError {
    OutboxApiError::MalformedRequest(ReportedError::client(QUEUE, error))
}

pub(crate) fn err_limit_out_of_range(limit: usize, min: usize, max: usize) -> OutboxApiError {
    OutboxApiError::LimitOutOfRange { limit, min, max }
}

#[cfg(feature = "api-tokio")]
pub(crate) fn err_publisher_not_configured() -> OutboxApiError {
    OutboxApiError::PublisherNotConfigured
}

#[cfg(feature = "api-tokio")]
pub(crate) fn err_run_task_failed(error: tokio::task::JoinError) -> OutboxApiError {
    OutboxApiError::RunTaskFailed(ReportedError::server(QUEUE, error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_failures_are_client_errors() {
        let malformed = serde_json::from_str::<usize>("x").unwrap_err();
        for error in [
            err_malformed_request(malformed),
            err_limit_out_of_range(0, 1, 1000),
        ] {
            assert_eq!(error.kind(), OutboxApiErrorKind::Invalid, "{error}");
            assert!(error.is_client_error(), "{error}");
        }
        assert_eq!(
            err_limit_out_of_range(0, 1, 1000).to_string(),
            "limit must be between 1 and 1000"
        );
    }

    #[test]
    fn missing_publishers_are_unavailable() {
        let error = OutboxApiError::PublisherNotConfigured;
        assert_eq!(error.kind(), OutboxApiErrorKind::Unavailable);
        assert!(!error.is_client_error());
        assert_eq!(error.to_string(), "outbox publisher is not configured");
    }

    #[test]
    fn task_failures_are_server_errors() {
        use std::error::Error;

        let report = ReportedError::server(QUEUE, "private diagnostics");
        let id = report.id().to_string();
        let error = OutboxApiError::RunTaskFailed(report);
        assert_eq!(error.kind(), OutboxApiErrorKind::Internal);
        assert!(!error.is_client_error());
        assert_eq!(error.to_string(), id);
        assert_eq!(error.source().unwrap().to_string(), "private diagnostics");
    }
}
