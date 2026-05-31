pub const COMPLETE_TODO_COMMAND: &str = "CompleteTodo";
pub const TODO_COMPLETED_EVENT: &str = "TodoCompleted";

mod models {
    use crate::assembly::io::TodoDto;
    use kernel::ApplicationEvent;
    use poem_openapi::Object;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[derive(Debug, Clone, Serialize, Deserialize, Object)]
    pub struct CompleteTodoCommand {
        pub todo_id: Uuid,
    }

    impl kernel::ApplicationCommand for CompleteTodoCommand {
        fn command_type(&self) -> &'static str {
            super::COMPLETE_TODO_COMMAND
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Object)]
    pub struct TodoCompleted {
        pub todo: TodoDto,
    }

    impl ApplicationEvent for TodoCompleted {
        fn event_type(&self) -> &'static str {
            super::TODO_COMPLETED_EVENT
        }
    }
}

mod handler {
    use super::models::{CompleteTodoCommand, TodoCompleted};
    use crate::assembly::io::{DbPool, TodoEvent};
    use kernel::{CommandError, CommandHandlerPort};

    pub struct CompleteTodoHandler {
        pool: DbPool,
    }

    impl CompleteTodoHandler {
        pub fn new(pool: DbPool) -> Self {
            Self { pool }
        }
    }

    impl CommandHandlerPort<CompleteTodoCommand, TodoEvent> for CompleteTodoHandler {
        fn execute(&self, command: CompleteTodoCommand) -> Result<Vec<TodoEvent>, CommandError> {
            let todo = super::infra_diesel::complete(&self.pool, command.todo_id)
                .map_err(|e| CommandError::HandlerExecution(e.to_string()))?;

            Ok(vec![TodoEvent::TodoCompleted(TodoCompleted { todo })])
        }
    }
}

mod infra_diesel {
    use crate::assembly::io::{AppError, Clock, DbPool, TodoDto, TodoRow, TodoStatus};
    use crate::schema::todos;
    use diesel::prelude::*;
    use uuid::Uuid;

    pub fn complete(pool: &DbPool, id: Uuid) -> Result<TodoDto, AppError> {
        let mut conn = pool.get().map_err(|e| AppError::Storage(e.into()))?;
        let row = diesel::update(todos::table.find(id))
            .set((
                todos::status.eq(TodoStatus::Completed.as_str()),
                todos::updated_at.eq(Clock::now()),
            ))
            .get_result::<TodoRow>(&mut conn)
            .optional()
            .map_err(|e| AppError::Storage(e.into()))?
            .ok_or(AppError::NotFound)?;
        row.try_into()
    }
}

mod http {
    use super::models::CompleteTodoCommand;
    use crate::{
        AppState,
        assembly::io::{
            ApiError, AppCommand, MulacState, NewCommandEnvelope, TodoDto, fetch_todo,
            interpret_dispatch_error, run_blocking,
        },
        //
    };
    use poem::web::Data;
    use poem_openapi::{OpenApi, param::Path, payload::Json};
    use uuid::Uuid;

    fn dispatch_complete_todo(mulac: &MulacState, id: Uuid) -> Result<(), ApiError> {
        let command_id = Uuid::now_v7();
        let envelope = NewCommandEnvelope {
            command: AppCommand::CompleteTodo(CompleteTodoCommand { todo_id: id }),
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
        #[oai(path = "/todos/:id/complete", method = "post")]
        async fn complete_todo(
            &self,
            state: Data<&AppState>,
            id: Path<Uuid>,
        ) -> Result<Json<TodoDto>, ApiError> {
            dispatch_complete_todo(&state.mulac, id.0)?;
            let pool = state.pool.clone();
            let id = id.0;
            let todo = run_blocking(move || fetch_todo(&pool, id)).await?;
            Ok(Json(todo))
        }
    }
}

pub mod io {
    pub use super::COMPLETE_TODO_COMMAND;
    pub use super::TODO_COMPLETED_EVENT;
    pub use super::handler::CompleteTodoHandler;
    pub use super::http::Api;
    pub use super::models::{CompleteTodoCommand, TodoCompleted};
}
