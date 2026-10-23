pub mod io {
    pub use super::{
        RunRequest,
        RunResponse,
        SubmitInboxRequest,
        SubmitInboxResponse,
        //
    };
}

use inbox_api::io as api;
use poem_openapi::registry::{MetaSchemaRef, Registry};
use poem_openapi::types::{ParseError, ParseFromJSON, ParseResult, ToJSON, Type};
use serde_json::Value;
use std::borrow::Cow;

/// Implements poem-openapi traits for a wrapper around a shared contract type.
///
/// The OpenAPI schema comes from a `#[derive(Object)]` twin that is never
/// parsed; parsing and serialization go through the shared Serde contract.
macro_rules! openapi_wrapper {
    ($wrapper:ident, $inner:ty, $schema:ty, $parse:path) => {
        impl Type for $wrapper {
            const IS_REQUIRED: bool = true;
            type RawValueType = Self;
            type RawElementValueType = Self;

            fn name() -> Cow<'static, str> {
                <$schema as Type>::name()
            }

            fn schema_ref() -> MetaSchemaRef {
                <$schema as Type>::schema_ref()
            }

            fn register(registry: &mut Registry) {
                <$schema as Type>::register(registry)
            }

            fn as_raw_value(&self) -> Option<&Self::RawValueType> {
                Some(self)
            }

            fn raw_element_iter<'a>(
                &'a self,
            ) -> Box<dyn Iterator<Item = &'a Self::RawElementValueType> + 'a> {
                Box::new(self.as_raw_value().into_iter())
            }
        }

        impl ParseFromJSON for $wrapper {
            fn parse_from_json(value: Option<Value>) -> ParseResult<Self> {
                $parse(value.unwrap_or_default())
                    .map($wrapper)
                    .map_err(err_unparsable)
            }
        }

        impl ToJSON for $wrapper {
            fn to_json(&self) -> Option<Value> {
                serde_json::to_value(&self.0).ok()
            }
        }

        impl From<$inner> for $wrapper {
            fn from(inner: $inner) -> Self {
                Self(inner)
            }
        }

        impl From<$wrapper> for $inner {
            fn from(wrapper: $wrapper) -> Self {
                wrapper.0
            }
        }
    };
}

/// Poem request body for `inbox_api::io::SubmitInboxRequest`.
///
/// Parsing checks field types only; `InboxAdapter::submit` validates values so
/// their messages are returned verbatim.
#[derive(Debug, Clone)]
pub struct SubmitInboxRequest(pub api::SubmitInboxRequest);

/// Poem response body for `inbox_api::io::SubmitInboxResponse`.
#[derive(Debug, Clone)]
pub struct SubmitInboxResponse(pub api::SubmitInboxResponse);

/// Poem request body for `inbox_api::io::RunRequest`. Parsing validates
/// the limit, so any queue endpoint can reuse it.
#[derive(Debug, Clone, Copy)]
pub struct RunRequest(pub api::RunRequest);

/// Poem response body for `inbox_api::io::RunResponse`.
#[derive(Debug, Clone)]
pub struct RunResponse(pub api::RunResponse);

openapi_wrapper!(
    SubmitInboxRequest,
    api::SubmitInboxRequest,
    schema::SubmitInboxRequest,
    api::SubmitInboxRequest::parse_json
);
openapi_wrapper!(
    SubmitInboxResponse,
    api::SubmitInboxResponse,
    schema::SubmitInboxResponse,
    parse_serde
);
openapi_wrapper!(
    RunRequest,
    api::RunRequest,
    schema::RunRequest,
    parse_run_request
);
openapi_wrapper!(
    RunResponse,
    api::RunResponse,
    schema::RunResponse,
    parse_serde
);

fn parse_run_request(value: Value) -> Result<api::RunRequest, api::InboxApiError> {
    let request: api::RunRequest = parse_serde(value)?;
    request.validate()?;
    Ok(request)
}

fn parse_serde<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, api::InboxApiError> {
    serde_json::from_value(value).map_err(err_malformed)
}

impl std::ops::Deref for RunRequest {
    type Target = api::RunRequest;

    fn deref(&self) -> &api::RunRequest {
        &self.0
    }
}

impl RunResponse {
    /// Convert any queue consumer's batch result.
    pub fn from_result<E: std::fmt::Display>(result: Result<usize, Vec<E>>) -> Self {
        Self(api::RunResponse::from_result(result))
    }
}

fn err_unparsable<T: Type>(error: api::InboxApiError) -> ParseError<T> {
    ParseError::custom(error)
}

fn err_malformed(error: serde_json::Error) -> api::InboxApiError {
    api::InboxApiError::MalformedRequest(error.to_string())
}

/// OpenAPI schema twins; their names and fields define the published schema.
#[allow(dead_code)]
mod schema {
    use poem_openapi::Object;
    use serde_json::Value;

    #[derive(Object)]
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

    #[derive(Object)]
    pub struct SubmitInboxResponse {
        pub message_id: String,
    }

    #[derive(Object)]
    pub struct RunRequest {
        /// Maximum number of eligible items to process in this queue only.
        #[oai(validator(minimum(value = "1"), maximum(value = "1000")))]
        pub limit: usize,
    }

    #[derive(Object)]
    pub struct RunResponse {
        /// Number processed on full success; null if any item failed.
        pub processed: Option<usize>,
        pub errors: Vec<String>,
    }
}
