//! JSON body reading shared by every queue extractor.
pub mod io {
    pub(crate) use super::{
        json_body,
        rejection_response,
        //
    };
}

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::Value;

/// Read the body as a JSON value.
///
/// Extracting a `Value` keeps Axum's syntax, content-type and body-limit
/// rejections; field errors then come from the shared `*_api` contract.
pub(crate) async fn json_body<S: Send + Sync>(
    req: Request,
    state: &S,
) -> Result<Value, JsonRejection> {
    let Json(value) = Json::<Value>::from_request(req, state).await?;
    Ok(value)
}

/// Render a body rejection; Axum answers some invalid JSON with `422`, the
/// Mulac contracts use `400`.
pub(crate) fn rejection_response(rejection: JsonRejection) -> Response {
    match rejection {
        JsonRejection::JsonDataError(rejection) => {
            (StatusCode::BAD_REQUEST, rejection.body_text()).into_response()
        }
        rejection => rejection.into_response(),
    }
}
