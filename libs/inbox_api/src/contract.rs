pub mod io {
    pub use super::{
        RunRequest,
        RunResponse,
        SubmitInboxRequest,
        SubmitInboxResponse,
        //
    };
}

use crate::error::io::{
    InboxApiError,
    err_correlation_id_not_uuid,
    err_limit_out_of_range,
    err_malformed_request,
    err_message_id_not_uuid,
    err_payload_empty,
    err_payload_required,
    err_routing_key_blank,
    //
    err_source_blank,
};
use inbox::io::{
    InboundMessageEnvelope,
    NewInboxMessageEnvelope,
    //
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::fmt::Display;
use uuid::Uuid;

/// Request to store one inbox message without processing it.
///
/// Unknown fields are ignored. String fields are strict: numbers and booleans
/// are rejected rather than coerced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubmitInboxRequest {
    /// Nonempty JSON value. Null, blank strings, empty arrays and objects are rejected.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub payload: Option<Value>,
    /// Command type used during inbox processing; must contain non-whitespace text.
    pub routing_key: String,
    /// UUID used for deduplication. Generated when omitted.
    #[serde(default)]
    pub message_id: Option<String>,
    /// Optional correlation UUID, preserved with the message.
    #[serde(default)]
    pub correlation_id: Option<String>,
    /// Message origin; required and must contain non-whitespace text.
    #[serde(default)]
    pub source: Option<String>,
}

// Preserve explicit null so validation can distinguish it from a missing payload.
fn present_value<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

impl SubmitInboxRequest {
    /// Parse and validate a JSON request body.
    pub fn from_json(value: Value) -> Result<Self, InboxApiError> {
        let request = Self::parse_json(value)?;
        request.validate()?;
        Ok(request)
    }

    /// Parse field types only, leaving [`validate`](Self::validate) to
    /// `InboxService::submit`. Adapters use this so validation messages reach
    /// clients without framework parse-error wording.
    pub fn parse_json(value: Value) -> Result<Self, InboxApiError> {
        serde_json::from_value(value).map_err(err_malformed_request)
    }

    pub fn validate(&self) -> Result<(), InboxApiError> {
        self.parsed_ids().map(|_| ())
    }

    fn guard(request: &Self) -> Result<(), InboxApiError> {
        let payload = request.payload.as_ref().ok_or_else(err_payload_required)?;
        let empty = match payload {
            Value::Null => true,
            Value::String(value) => value.trim().is_empty(),
            Value::Array(value) => value.is_empty(),
            Value::Object(value) => value.is_empty(),
            Value::Bool(_) | Value::Number(_) => false,
        };
        if empty {
            return Err(err_payload_empty());
        }
        if request.routing_key.trim().is_empty() {
            return Err(err_routing_key_blank());
        }
        if request
            .source
            .as_deref()
            .is_none_or(|source| source.trim().is_empty())
        {
            return Err(err_source_blank());
        }
        Ok(())
    }

    fn parsed_ids(&self) -> Result<(Option<Uuid>, Option<Uuid>), InboxApiError> {
        Self::guard(self)?;
        let message_id = parse_uuid(&self.message_id).map_err(err_message_id_not_uuid)?;
        let correlation_id =
            parse_uuid(&self.correlation_id).map_err(err_correlation_id_not_uuid)?;
        Ok((message_id, correlation_id))
    }
}

fn parse_uuid(value: &Option<String>) -> Result<Option<Uuid>, uuid::Error> {
    value.as_deref().map(Uuid::parse_str).transpose()
}

impl TryFrom<SubmitInboxRequest> for NewInboxMessageEnvelope {
    type Error = InboxApiError;

    /// Validate and convert; a missing `message_id` becomes a new UUID v7.
    fn try_from(request: SubmitInboxRequest) -> Result<Self, InboxApiError> {
        let (message_id, correlation_id) = request.parsed_ids()?;
        let payload = request.payload.unwrap_or(Value::Null);
        let envelope = InboundMessageEnvelope {
            payload: payload.to_string(),
            message_id,
            correlation_id,
            source: request.source,
            routing_key: Some(request.routing_key),
        };
        Ok(envelope.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitInboxResponse {
    pub message_id: String,
}

/// Request to process one batch from a queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRequest {
    /// Maximum number of eligible items to process in this queue only.
    pub limit: usize,
}

impl RunRequest {
    pub const MIN_LIMIT: usize = 1;
    pub const MAX_LIMIT: usize = 1000;

    /// Parse and validate a JSON request body.
    pub fn from_json(value: Value) -> Result<Self, InboxApiError> {
        let request: Self = serde_json::from_value(value).map_err(err_malformed_request)?;
        request.validate()?;
        Ok(request)
    }

    pub fn validate(&self) -> Result<(), InboxApiError> {
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
pub struct RunResponse {
    /// Number processed on full success; `None` if any item failed.
    pub processed: Option<usize>,
    pub errors: Vec<String>,
}

impl RunResponse {
    /// Convert any queue consumer's batch result.
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

    fn submit(body: Value) -> Result<SubmitInboxRequest, InboxApiError> {
        SubmitInboxRequest::from_json(body)
    }

    #[cfg(feature = "testing")]
    #[test]
    fn rejects_empty_fields_for_parsed_and_direct_requests() {
        for body in crate::testing::io::invalid_submissions() {
            assert!(
                SubmitInboxRequest::from_json(body.clone()).is_err(),
                "{body}"
            );
            if let Ok(request) = SubmitInboxRequest::parse_json(body.clone()) {
                assert!(request.validate().is_err(), "{body}");
                assert!(
                    NewInboxMessageEnvelope::try_from(request).is_err(),
                    "{body}"
                );
            }
        }
    }

    #[test]
    fn direct_callers_get_the_same_validation() {
        let request = SubmitInboxRequest {
            payload: None,
            routing_key: "x".into(),
            message_id: None,
            correlation_id: None,
            source: None,
        };
        assert!(matches!(
            NewInboxMessageEnvelope::try_from(request),
            Err(InboxApiError::PayloadRequired)
        ));
        assert!(matches!(
            RunRequest { limit: 1001 }.validate(),
            Err(InboxApiError::LimitOutOfRange { .. })
        ));
    }

    #[test]
    fn strings_are_strict_and_whitespace_is_preserved() {
        for body in [
            json!({"payload":1,"routing_key":42}),
            json!({"payload":1,"routing_key":"x","source":true}),
            json!({"payload":1,"routing_key":"x","source":"crawler","message_id":42}),
        ] {
            assert!(
                matches!(
                    submit(body.clone()),
                    Err(InboxApiError::MalformedRequest(_))
                ),
                "{body}"
            );
        }
        let request =
            submit(json!({"payload":1,"routing_key":" x ","source":" crawler "})).unwrap();
        assert_eq!(request.routing_key, " x ");
        assert_eq!(request.source.as_deref(), Some(" crawler "));
    }

    #[test]
    fn uuids_are_validated_and_preserved() {
        let id = "0191d6c4-8f5e-7a3b-9c1d-2e3f4a5b6c7d";
        let envelope = NewInboxMessageEnvelope::try_from(
            submit(json!({"payload":{"a":1},"routing_key":"x","source":"crawler","message_id":id}))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(envelope.id().to_string(), id);
        assert_eq!(envelope.payload(), r#"{"a":1}"#);
        assert!(matches!(
            submit(json!({"payload":1,"routing_key":"x","source":"crawler","message_id":""})),
            Err(InboxApiError::MessageIdNotUuid(_))
        ));
        assert!(matches!(
            submit(json!({"payload":1,"routing_key":"x","source":"crawler","correlation_id":"no"})),
            Err(InboxApiError::CorrelationIdNotUuid(_))
        ));
    }

    #[test]
    fn missing_message_id_generates_uuid_v7() {
        let envelope = NewInboxMessageEnvelope::try_from(
            submit(json!({"payload":1,"routing_key":"x","source":"crawler"})).unwrap(),
        )
        .unwrap();
        assert_eq!(envelope.id().get_version_num(), 7);
    }

    #[test]
    fn run_limit_bounds() {
        for limit in [1, 1000] {
            assert!(RunRequest::from_json(json!({"limit":limit})).is_ok());
        }
        for body in [
            json!({"limit":0}),
            json!({"limit":1001}),
            json!({"limit":-1}),
            json!({"limit":1.5}),
            json!({"limit":"1"}),
            json!({"limit":null}),
            json!({}),
        ] {
            let error = RunRequest::from_json(body.clone()).unwrap_err();
            assert!(error.is_client_error(), "{body}");
        }
    }

    #[test]
    fn run_response_reports_counts_or_errors() {
        assert_eq!(
            RunResponse::from_result::<String>(Ok(3)),
            RunResponse {
                processed: Some(3),
                errors: vec![]
            }
        );
        assert_eq!(
            serde_json::to_value(RunResponse::from_result(Err(vec!["boom"]))).unwrap(),
            json!({"processed":null,"errors":["boom"]})
        );
    }
}
