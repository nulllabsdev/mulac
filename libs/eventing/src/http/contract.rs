pub mod prelude {
    pub use super::{
        RunEventsRequest,
        RunEventsResponse,
        //
    };
}

use crate::http::error::prelude::{
    EventingApiError,
    QUEUE,
    err_limit_out_of_range,
    err_malformed_request,
    //
};
use mulac_http::prelude::report_batch_error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Display;

/// Request to dispatch one batch of due events. Unknown fields are ignored.
///
/// Every instance holds a limit within `MIN_LIMIT..=MAX_LIMIT`: construction,
/// including deserialization, goes through validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawRunEventsRequest")]
pub struct RunEventsRequest {
    /// Maximum number of due events to dispatch in this batch.
    limit: usize,
}

impl RunEventsRequest {
    pub const MIN_LIMIT: usize = 1;
    pub const MAX_LIMIT: usize = 1000;

    /// Build a request, rejecting a limit outside `MIN_LIMIT..=MAX_LIMIT`.
    pub fn new(limit: usize) -> Result<Self, EventingApiError> {
        Self::validate(limit)?;
        Ok(Self { limit })
    }

    /// Parse and validate a JSON request body.
    pub fn from_json(value: Value) -> Result<Self, EventingApiError> {
        let raw: RawRunEventsRequest =
            serde_json::from_value(value).map_err(err_malformed_request)?;
        Self::new(raw.limit)
    }

    /// Maximum number of due events to dispatch in this batch.
    pub fn limit(&self) -> usize {
        self.limit
    }

    fn validate(limit: usize) -> Result<(), EventingApiError> {
        if !(Self::MIN_LIMIT..=Self::MAX_LIMIT).contains(&limit) {
            return Err(err_limit_out_of_range(
                limit,
                Self::MIN_LIMIT,
                Self::MAX_LIMIT,
            ));
        }
        Ok(())
    }
}

/// Unvalidated wire shape; only ever converted through [`RunEventsRequest::new`].
#[derive(Deserialize)]
struct RawRunEventsRequest {
    limit: usize,
}

impl TryFrom<RawRunEventsRequest> for RunEventsRequest {
    type Error = EventingApiError;

    fn try_from(raw: RawRunEventsRequest) -> Result<Self, Self::Error> {
        Self::new(raw.limit)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEventsResponse {
    /// Number dispatched on full success; `None` if any event failed.
    pub processed: Option<usize>,
    /// One error ID per failure; each ID is logged with the full error.
    pub errors: Vec<String>,
}

impl RunEventsResponse {
    /// Convert an event consumer's batch result, logging each error under the ID returned for it.
    pub fn from_result<E: Display>(result: Result<usize, Vec<E>>) -> Self {
        match result {
            Ok(processed) => Self {
                processed: Some(processed),
                errors: vec![],
            },
            Err(errors) => Self {
                processed: None,
                errors: errors
                    .into_iter()
                    .map(|error| report_batch_error(QUEUE, error))
                    .collect(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn new_rejects_limits_out_of_range() {
        assert!(matches!(
            RunEventsRequest::new(1001),
            Err(EventingApiError::LimitOutOfRange {
                limit: 1001,
                min: 1,
                max: 1000
            })
        ));
        assert!(matches!(
            RunEventsRequest::new(0),
            Err(EventingApiError::LimitOutOfRange { limit: 0, .. })
        ));
    }

    #[test]
    fn new_accepts_limits_in_range() {
        for limit in [RunEventsRequest::MIN_LIMIT, RunEventsRequest::MAX_LIMIT] {
            assert_eq!(RunEventsRequest::new(limit).unwrap().limit(), limit);
        }
    }

    #[test]
    fn deserializing_cannot_bypass_validation() {
        for limit in [0, 1001] {
            let result = serde_json::from_value::<RunEventsRequest>(json!({"limit":limit}));
            assert!(result.is_err(), "limit {limit} should be rejected");
        }
        let request: RunEventsRequest = serde_json::from_value(json!({"limit":5})).unwrap();
        assert_eq!(request.limit(), 5);
    }

    #[test]
    fn requests_serialize_to_the_wire_shape() {
        let request = RunEventsRequest::new(5).unwrap();
        assert_eq!(serde_json::to_value(request).unwrap(), json!({"limit":5}));
    }

    #[test]
    fn malformed_bodies_report_only_an_id() {
        let error = RunEventsRequest::from_json(json!({"limit":"1"})).unwrap_err();
        assert!(matches!(error, EventingApiError::MalformedRequest(_)));
        assert!(uuid::Uuid::parse_str(&error.to_string()).is_ok(), "{error}");
        let cause = std::error::Error::source(&error).unwrap().to_string();
        assert!(cause.contains("invalid type"), "{cause}");
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
        let response = RunEventsResponse::from_result(Err(vec!["a", "b"]));
        assert_eq!(response.errors.len(), 2);
        for id in &response.errors {
            assert!(uuid::Uuid::parse_str(id).is_ok(), "{id}");
        }
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            json!({"processed":null,"errors":response.errors})
        );
    }
}

#[cfg(all(test, feature = "api-testing"))]
mod fixture_tests {
    use super::*;
    use crate::prelude::testing;

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
        for body in testing::malformed_run_requests() {
            let error = RunEventsRequest::from_json(body.clone()).unwrap_err();
            assert!(error.is_client_error(), "{body}");
            assert!(
                uuid::Uuid::parse_str(&error.to_string()).is_ok(),
                "{body}: {error}"
            );
        }
    }
}
