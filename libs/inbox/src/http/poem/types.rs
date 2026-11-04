pub mod prelude {
    pub use super::{
        SubmitInboxRequest,
        SubmitInboxResponse,
        //
    };
}

use crate::http::error::prelude::err_malformed_request;
use crate::http::prelude as contract;
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

/// Poem request body for `crate::prelude::SubmitInboxRequest`.
///
/// Parsing checks field types only; `InboxAdapter::submit` validates values so
/// their messages are returned verbatim.
#[derive(Debug, Clone)]
pub struct SubmitInboxRequest(pub contract::SubmitInboxRequest);

/// Poem response body for `crate::prelude::SubmitInboxResponse`.
#[derive(Debug, Clone)]
pub struct SubmitInboxResponse(pub contract::SubmitInboxResponse);

openapi_wrapper!(
    SubmitInboxRequest,
    contract::SubmitInboxRequest,
    schema::SubmitInboxRequest,
    contract::SubmitInboxRequest::parse_json
);
openapi_wrapper!(
    SubmitInboxResponse,
    contract::SubmitInboxResponse,
    schema::SubmitInboxResponse,
    parse_serde
);
fn parse_serde<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, contract::InboxApiError> {
    serde_json::from_value(value).map_err(err_malformed_request)
}

fn err_unparsable<T: Type>(error: contract::InboxApiError) -> ParseError<T> {
    ParseError::custom(error)
}

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
}
