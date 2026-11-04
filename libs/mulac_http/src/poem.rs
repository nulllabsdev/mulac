use crate::error_report::prelude::{ReportedError, report_batch_error};
use poem_openapi::registry::{MetaSchemaRef, Registry};
use poem_openapi::types::{ParseError, ParseFromJSON, ParseResult, ToJSON, Type};
use serde_json::Value;
use std::borrow::Cow;
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

pub mod prelude {
    pub use super::{RunRequest, RunRequestData, RunResponse, RunResponseData};
}

/// These types are shared by every queue, so failures here log this name instead.
const SHARED_QUEUE: &str = "http";

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct RunRequestData {
    pub limit: usize,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RunResponseData {
    pub processed: Option<usize>,
    pub errors: Vec<String>,
}
#[derive(Debug, Clone, Copy)]
pub struct RunRequest(pub RunRequestData);
#[derive(Debug, Clone)]
pub struct RunResponse(pub RunResponseData);
impl std::ops::Deref for RunRequest {
    type Target = RunRequestData;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl RunResponse {
    pub fn from_result<E: std::fmt::Display>(result: Result<usize, Vec<E>>) -> Self {
        Self(match result {
            Ok(n) => RunResponseData {
                processed: Some(n),
                errors: vec![],
            },
            Err(errors) => RunResponseData {
                processed: None,
                errors: errors
                    .into_iter()
                    .map(|error| report_batch_error(SHARED_QUEUE, error))
                    .collect(),
            },
        })
    }
}
openapi_wrapper!(
    RunRequest,
    RunRequestData,
    schema::RunRequest,
    parse_run_request
);
openapi_wrapper!(
    RunResponse,
    RunResponseData,
    schema::RunResponse,
    parse_serde
);
fn parse_serde<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, String> {
    serde_json::from_value(value).map_err(|e| e.to_string())
}
fn parse_run_request(value: Value) -> Result<RunRequestData, String> {
    let r: RunRequestData = serde_json::from_value(value)
        .map_err(|error| ReportedError::client(SHARED_QUEUE, error).to_string())?;
    if !(1..=1000).contains(&r.limit) {
        return Err("limit must be between 1 and 1000".into());
    }
    Ok(r)
}
fn err_unparsable<T: Type>(error: String) -> ParseError<T> {
    ParseError::custom(error)
}
mod schema {
    use poem_openapi::Object;
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
        /// One error ID per failure; each ID is logged with the full error.
        pub errors: Vec<String>,
    }
}
