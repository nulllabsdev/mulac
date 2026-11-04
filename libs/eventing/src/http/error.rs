pub mod prelude {
    pub use super::EventingApiError;
    pub(crate) use super::QUEUE;
    #[cfg(feature = "api-tokio")]
    pub(crate) use super::err_run_task_failed;
    pub(crate) use super::{
        err_limit_out_of_range,
        err_malformed_request,
        //
    };
}

use mulac_http::prelude::ReportedError;

/// Queue name logged with this crate's reported failures.
pub(crate) const QUEUE: &str = "eventing";

/// Failure of an eventing API operation.
///
/// Display text is safe to return to HTTP clients. Unparsable bodies and server
/// failures display only an error ID: the cause is logged under that ID when the
/// error is created, and stays available as [`std::error::Error::source`].
#[derive(Debug, thiserror::Error)]
pub enum EventingApiError {
    #[error(transparent)]
    MalformedRequest(ReportedError),
    #[error("limit must be between {min} and {max}")]
    LimitOutOfRange {
        limit: usize,
        min: usize,
        max: usize,
    },
    #[error(transparent)]
    RunTaskFailed(ReportedError),
}

impl EventingApiError {
    /// Whether the request itself is invalid, as opposed to a server failure.
    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            Self::MalformedRequest(_) | Self::LimitOutOfRange { .. }
        )
    }
}

pub(crate) fn err_malformed_request(error: serde_json::Error) -> EventingApiError {
    EventingApiError::MalformedRequest(ReportedError::client(QUEUE, error))
}

pub(crate) fn err_limit_out_of_range(limit: usize, min: usize, max: usize) -> EventingApiError {
    EventingApiError::LimitOutOfRange { limit, min, max }
}

#[cfg(feature = "api-tokio")]
pub(crate) fn err_run_task_failed(error: tokio::task::JoinError) -> EventingApiError {
    EventingApiError::RunTaskFailed(ReportedError::server(QUEUE, error))
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
            assert!(error.is_client_error(), "{error}");
        }
        assert_eq!(
            err_limit_out_of_range(0, 1, 1000).to_string(),
            "limit must be between 1 and 1000"
        );
    }

    #[test]
    fn task_failures_are_server_errors() {
        use std::error::Error;

        let report = ReportedError::server(QUEUE, "private diagnostics");
        let id = report.id().to_string();
        let error = EventingApiError::RunTaskFailed(report);
        assert!(!error.is_client_error());
        assert_eq!(error.to_string(), id);
        assert_eq!(error.source().unwrap().to_string(), "private diagnostics");
    }
}
