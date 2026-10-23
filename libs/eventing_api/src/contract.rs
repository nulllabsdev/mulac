pub mod io {
    pub use super::{
        RunEventsRequest,
        RunEventsResponse,
        //
    };
}

use crate::error::io::{
    EventingApiError,
    err_limit_out_of_range,
    err_malformed_request,
    //
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Display;

/// Request to dispatch one batch of due events. Unknown fields are ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEventsRequest {
    /// Maximum number of due events to dispatch in this batch.
    pub limit: usize,
}

impl RunEventsRequest {
    pub const MIN_LIMIT: usize = 1;
    pub const MAX_LIMIT: usize = 1000;

    /// Parse and validate a JSON request body.
    pub fn from_json(value: Value) -> Result<Self, EventingApiError> {
        let request: Self = serde_json::from_value(value).map_err(err_malformed_request)?;
        request.validate()?;
        Ok(request)
    }

    pub fn validate(&self) -> Result<(), EventingApiError> {
        if !(Self::MIN_LIMIT..=Self::MAX_LIMIT).contains(&self.limit) {
            return Err(err_limit_out_of_range(
                self.limit,
                Self::MIN_LIMIT,
                Self::MAX_LIMIT,
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEventsResponse {
    /// Number dispatched on full success; `None` if any event failed.
    pub processed: Option<usize>,
    pub errors: Vec<String>,
}

impl RunEventsResponse {
    /// Convert an event consumer's batch result, keeping each error's text.
    pub fn from_result<E: Display>(result: Result<usize, Vec<E>>) -> Self {
        match result {
            Ok(processed) => Self {
                processed: Some(processed),
                errors: vec![],
            },
            Err(errors) => Self {
                processed: None,
                errors: errors.into_iter().map(|error| error.to_string()).collect(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn direct_callers_get_the_same_validation() {
        assert!(matches!(
            RunEventsRequest { limit: 1001 }.validate(),
            Err(EventingApiError::LimitOutOfRange {
                limit: 1001,
                min: 1,
                max: 1000
            })
        ));
        assert!(RunEventsRequest { limit: 1 }.validate().is_ok());
    }

    #[test]
    fn malformed_bodies_keep_the_serde_message() {
        let error = RunEventsRequest::from_json(json!({"limit":"1"})).unwrap_err();
        assert!(matches!(error, EventingApiError::MalformedRequest(_)));
        assert!(error.to_string().contains("invalid type"), "{error}");
    }

    #[test]
    fn response_reports_counts_or_errors() {
        assert_eq!(
            RunEventsResponse::from_result::<String>(Ok(3)),
            RunEventsResponse {
                processed: Some(3),
                errors: vec![]
            }
        );
        assert_eq!(
            serde_json::to_value(RunEventsResponse::from_result(Err(vec!["a", "b"]))).unwrap(),
            json!({"processed":null,"errors":["a","b"]})
        );
    }
}

#[cfg(all(test, feature = "testing"))]
mod fixture_tests {
    use super::*;
    use crate::io::testing;

    #[test]
    fn shared_run_request_table() {
        for body in testing::valid_run_requests() {
            assert!(RunEventsRequest::from_json(body.clone()).is_ok(), "{body}");
        }
        for body in testing::invalid_run_requests() {
            let error = RunEventsRequest::from_json(body.clone()).unwrap_err();
            assert!(error.is_client_error(), "{body}");
        }
        for (body, message) in testing::run_request_error_messages() {
            let error = RunEventsRequest::from_json(body.clone()).unwrap_err();
            assert_eq!(error.to_string(), message, "{body}");
        }
    }
}
