mod infra_diesel {
    use crate::assembly::io::{AppError, DbPool, TodoDto, fetch_todo};
    use uuid::Uuid;

    pub fn get(pool: &DbPool, id: Uuid) -> Result<TodoDto, AppError> {
        fetch_todo(pool, id)
    }
}

mod http {
    use crate::{
        AppState,
        assembly::io::{ApiError, TodoDto, run_blocking},
        //
    };
    use poem::web::Data;
    use poem_openapi::{OpenApi, param::Path, payload::Json};
    use uuid::Uuid;

    pub struct Api;

    #[OpenApi]
    impl Api {
        #[oai(path = "/todos/:id", method = "get")]
        async fn get_todo(
            &self,
            state: Data<&AppState>,
            id: Path<Uuid>,
        ) -> Result<Json<TodoDto>, ApiError> {
            let pool = state.pool.clone();
            let id = id.0;
            let todo = run_blocking(move || super::infra_diesel::get(&pool, id)).await?;
            Ok(Json(todo))
        }
    }
}

pub mod io {
    pub use super::http::Api;
}
