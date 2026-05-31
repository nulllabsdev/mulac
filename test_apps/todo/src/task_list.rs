use crate::io::TodoStatus;
use poem_openapi::Enum;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Enum)]
#[serde(rename_all = "snake_case")]
#[oai(rename_all = "snake_case")]
pub enum FilterStatus {
    Active,
    Completed,
    Archived,
    All,
}

impl FilterStatus {
    pub fn as_todo_status(self) -> Option<TodoStatus> {
        match self {
            Self::Active => Some(TodoStatus::Active),
            Self::Completed => Some(TodoStatus::Completed),
            Self::Archived => Some(TodoStatus::Archived),
            Self::All => None,
        }
    }
}

mod infra_diesel {
    use crate::assembly::io::{AppError, DbPool, TodoDto, TodoList, TodoRow};
    use crate::schema::todos;
    use crate::task_list::FilterStatus;
    use diesel::prelude::*;

    pub fn list(pool: &DbPool, status: Option<FilterStatus>) -> Result<TodoList, AppError> {
        let mut conn = pool.get().map_err(|e| AppError::Storage(e.into()))?;

        let rows = match status.and_then(FilterStatus::as_todo_status) {
            Some(status) => todos::table
                .filter(todos::status.eq(status.as_str()))
                .order(todos::created_at.asc())
                .load::<TodoRow>(&mut conn),
            None => todos::table
                .order(todos::created_at.asc())
                .load::<TodoRow>(&mut conn),
        }
        .map_err(|e| AppError::Storage(e.into()))?;

        let items = rows
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<TodoDto>, AppError>>()?;
        Ok(TodoList { items })
    }
}

mod http {
    use crate::AppState;
    use crate::assembly::io::{ApiError, TodoList, run_blocking};
    use crate::task_list::FilterStatus;
    use poem::web::Data;
    use poem_openapi::{OpenApi, param::Query, payload::Json};

    pub struct Api;

    #[OpenApi]
    impl Api {
        #[oai(path = "/todos", method = "get")]
        async fn list_todos(
            &self,
            state: Data<&AppState>,
            status: Query<Option<FilterStatus>>,
        ) -> Result<Json<TodoList>, ApiError> {
            let pool = state.pool.clone();
            let status = status.0;
            let list = run_blocking(move || super::infra_diesel::list(&pool, status)).await?;
            Ok(Json(list))
        }
    }
}

pub mod io {
    pub use super::FilterStatus;
    pub use super::http::Api;
}
