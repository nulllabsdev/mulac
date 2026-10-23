pub mod io {
    pub use super::EventingApiError;
    #[cfg(feature = "tokio")]
    pub(crate) use super::err_run_task_failed;
    pub(crate) use super::{
        err_limit_out_of_range,
        err_malformed_request,
        //
    };
}

type Cause = Box<dyn std::error::Error + Send + Sync>;

/// Failure of an eventing API operation.
///
/// Display text is safe to return to HTTP clients. Task failures keep their
/// cause as [`std::error::Error::source`] without exposing it. Event
/// failures are not errors; they are reported in `RunEventsResponse`.
#[derive(Debug, thiserror::Error)]
pub enum EventingApiError {
    #[error("{0}")]
    MalformedRequest(String),
    #[error("limit must be between {min} and {max}")]
    LimitOutOfRange {
        limit: usize,
        min: usize,
        max: usize,
    },
    #[error("queue processing task failed")]
    RunTaskFailed(#[source] Cause),
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
    EventingApiError::MalformedRequest(error.to_string())
}

pub(crate) fn err_limit_out_of_range(limit: usize, min: usize, max: usize) -> EventingApiError {
    EventingApiError::LimitOutOfRange { limit, min, max }
}

#[cfg(feature = "tokio")]
pub(crate) fn err_run_task_failed(error: tokio::task::JoinError) -> EventingApiError {
    EventingApiError::RunTaskFailed(Box::new(error))
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
        let error = EventingApiError::RunTaskFailed("private diagnostics".into());
        assert!(!error.is_client_error());
        assert_eq!(error.to_string(), "queue processing task failed");
    }
}
