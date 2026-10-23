pub mod io {
    pub use super::{
        RunCommandsRequest,
        RunCommandsResponse,
        //
    };
}

use crate::error::io::{
    CommandingApiError,
    err_limit_out_of_range,
    err_malformed_request,
    //
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Display;

/// Request to execute one batch of due commands. Unknown fields are ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCommandsRequest {
    /// Maximum number of due commands to execute in this batch.
    pub limit: usize,
}

impl RunCommandsRequest {
    pub const MIN_LIMIT: usize = 1;
    pub const MAX_LIMIT: usize = 1000;

    /// Parse and validate a JSON request body.
    pub fn from_json(value: Value) -> Result<Self, CommandingApiError> {
        let request: Self = serde_json::from_value(value).map_err(err_malformed_request)?;
        request.validate()?;
        Ok(request)
    }

    pub fn validate(&self) -> Result<(), CommandingApiError> {
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
pub struct RunCommandsResponse {
    /// Number executed on full success; `None` if any command failed.
    pub processed: Option<usize>,
    pub errors: Vec<String>,
}

impl RunCommandsResponse {
    /// Convert a command consumer's batch result, keeping each error's text.
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
            RunCommandsRequest { limit: 1001 }.validate(),
            Err(CommandingApiError::LimitOutOfRange {
                limit: 1001,
                min: 1,
                max: 1000
            })
        ));
        assert!(RunCommandsRequest { limit: 1 }.validate().is_ok());
    }

    #[test]
    fn malformed_bodies_keep_the_serde_message() {
        let error = RunCommandsRequest::from_json(json!({"limit":"1"})).unwrap_err();
        assert!(matches!(error, CommandingApiError::MalformedRequest(_)));
        assert!(error.to_string().contains("invalid type"), "{error}");
    }

    #[test]
    fn response_reports_counts_or_errors() {
        assert_eq!(
            RunCommandsResponse::from_result::<String>(Ok(3)),
            RunCommandsResponse {
                processed: Some(3),
                errors: vec![]
            }
        );
        assert_eq!(
            serde_json::to_value(RunCommandsResponse::from_result(Err(vec!["a", "b"]))).unwrap(),
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
            assert!(
                RunCommandsRequest::from_json(body.clone()).is_ok(),
                "{body}"
            );
        }
        for body in testing::invalid_run_requests() {
            let error = RunCommandsRequest::from_json(body.clone()).unwrap_err();
            assert!(error.is_client_error(), "{body}");
        }
        for (body, message) in testing::run_request_error_messages() {
            let error = RunCommandsRequest::from_json(body.clone()).unwrap_err();
            assert_eq!(error.to_string(), message, "{body}");
        }
    }
}
