pub const UPDATE_DUE_DATE_COMMAND: &str = "UpdateDueDate";
pub const TODO_DUE_DATE_CHANGED_EVENT: &str = "TodoDueDateChanged";

mod models {
    use crate::assembly::io::TodoDto;
    use chrono::{DateTime, Utc};
    use kernel::ApplicationEvent;
    use poem_openapi::Object;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[derive(Debug, Clone, Serialize, Deserialize, Object)]
    pub struct UpdateDueDateCommand {
        pub todo_id: Uuid,
        pub due_at: Option<DateTime<Utc>>,
    }

    impl kernel::ApplicationCommand for UpdateDueDateCommand {
        fn command_type(&self) -> &'static str {
            super::UPDATE_DUE_DATE_COMMAND
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Object)]
    pub struct TodoDueDateChanged {
        pub todo: TodoDto,
    }

    impl ApplicationEvent for TodoDueDateChanged {
        fn event_type(&self) -> &'static str {
            super::TODO_DUE_DATE_CHANGED_EVENT
        }
    }
}

mod handler {
    use super::models::{TodoDueDateChanged, UpdateDueDateCommand};
    use crate::assembly::io::{DbPool, TodoEvent};
    use kernel::{CommandError, CommandHandlerPort};

    pub struct UpdateDueDateHandler {
        pool: DbPool,
    }

    impl UpdateDueDateHandler {
        pub fn new(pool: DbPool) -> Self {
            Self { pool }
        }
    }

    impl CommandHandlerPort<UpdateDueDateCommand, TodoEvent> for UpdateDueDateHandler {
        fn execute(&self, command: UpdateDueDateCommand) -> Result<Vec<TodoEvent>, CommandError> {
            let todo =
                super::infra_diesel::set_due_date(&self.pool, command.todo_id, command.due_at)
                    .map_err(|e| CommandError::HandlerExecution(e.to_string()))?;

            Ok(vec![TodoEvent::TodoDueDateChanged(TodoDueDateChanged {
                todo,
            })])
        }
    }
}

mod infra_diesel {
    use crate::assembly::io::{AppError, Clock, DbPool, TodoDto, TodoRow};
    use crate::schema::todos;
    use chrono::{DateTime, Utc};
    use diesel::prelude::*;
    use uuid::Uuid;

    pub fn set_due_date(
        pool: &DbPool,
        id: Uuid,
        due_at: Option<DateTime<Utc>>,
    ) -> Result<TodoDto, AppError> {
        let mut conn = pool.get().map_err(|e| AppError::Storage(e.into()))?;
        let row = diesel::update(todos::table.find(id))
            .set((todos::due_at.eq(due_at), todos::updated_at.eq(Clock::now())))
            .get_result::<TodoRow>(&mut conn)
            .optional()
            .map_err(|e| AppError::Storage(e.into()))?
            .ok_or(AppError::NotFound)?;
        row.try_into()
    }
}

mod http {
    use super::models::UpdateDueDateCommand;
    use crate::{
        AppState,
        assembly::io::{
            ApiError, AppCommand, MulacState, NewCommandEnvelope, TodoDto, fetch_todo,
            interpret_dispatch_error, run_blocking,
        },
        //
    };
    use chrono::{DateTime, Utc};
    use poem::web::Data;
    use poem_openapi::{Object, OpenApi, param::Path, payload::Json};
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[derive(Debug, Clone, Serialize, Deserialize, Object)]
    pub struct UpdateDueDateRequest {
        pub due_at: Option<DateTime<Utc>>,
    }

    fn dispatch_update_due_date(
        mulac: &MulacState,
        id: Uuid,
        due_at: Option<DateTime<Utc>>,
    ) -> Result<(), ApiError> {
        let command_id = Uuid::now_v7();
        let envelope = NewCommandEnvelope {
            command: AppCommand::UpdateDueDate(UpdateDueDateCommand {
                todo_id: id,
                due_at,
            }),
            metadata: kernel::NewCommandMetadata {
                command_id,
                correlation_id: Some(command_id),
                causation_id: None,
                source: Some("test_app_todo.http".to_string()),
            },
        };
        mulac
            .dispatch_command(envelope)
            .map_err(|e| ApiError::from(interpret_dispatch_error(e)))
    }

    pub struct Api;

    #[OpenApi]
    impl Api {
        #[oai(path = "/todos/:id/due-date", method = "put")]
        async fn update_due_date(
            &self,
            state: Data<&AppState>,
            id: Path<Uuid>,
            Json(request): Json<UpdateDueDateRequest>,
        ) -> Result<Json<TodoDto>, ApiError> {
            dispatch_update_due_date(&state.mulac, id.0, request.due_at)?;
            let pool = state.pool.clone();
            let id = id.0;
            let todo = run_blocking(move || fetch_todo(&pool, id)).await?;
            Ok(Json(todo))
        }
    }
}

pub mod io {
    pub use super::TODO_DUE_DATE_CHANGED_EVENT;
    pub use super::UPDATE_DUE_DATE_COMMAND;
    pub use super::handler::UpdateDueDateHandler;
    pub use super::http::Api;
    pub use super::models::{TodoDueDateChanged, UpdateDueDateCommand};
}
