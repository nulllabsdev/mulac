use super::prelude::*;
use mulac_http::prelude::schema as twins;
use std::borrow::Cow;
use utoipa::openapi::{RefOr, schema::Schema};
use utoipa::{PartialSchema, ToSchema};

macro_rules! schema_as {
    ($adapter:ty, $schema:ty) => {
        impl PartialSchema for $adapter {
            fn schema() -> RefOr<Schema> {
                <$schema as PartialSchema>::schema()
            }
        }

        impl ToSchema for $adapter {
            fn name() -> Cow<'static, str> {
                <$schema as ToSchema>::name()
            }

            fn schemas(schemas: &mut Vec<(String, RefOr<Schema>)>) {
                <$schema as ToSchema>::schemas(schemas)
            }
        }
    };
}

schema_as!(RunOutbox, twins::RunRequest);
schema_as!(OutboxRunResponse, twins::RunResponse);
