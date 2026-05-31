pub const DELETE_TODO_COMMAND: &str = "DeleteTodo";
pub const TODO_DELETED_EVENT: &str = "TodoDeleted";

mod models {
    use crate::assembly::io::TodoDto;
    use kernel::ApplicationEvent;
    use poem_openapi::Object;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[derive(Debug, Clone, Serialize, Deserialize, Object)]
    pub struct DeleteTodoCommand {
        pub todo_id: Uuid,
    }

    impl kernel::ApplicationCommand for DeleteTodoCommand {
        fn command_type(&self) -> &'static str {
            super::DELETE_TODO_COMMAND
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Object)]
    pub struct TodoDeleted {
        pub todo: TodoDto,
    }

    impl ApplicationEvent for TodoDeleted {
        fn event_type(&self) -> &'static str {
            super::TODO_DELETED_EVENT
        }
    }
}

mod handler {
    use super::models::{DeleteTodoCommand, TodoDeleted};
    use crate::assembly::io::{DbPool, TodoEvent};
    use kernel::{CommandError, CommandHandlerPort};

    pub struct DeleteTodoHandler {
        pool: DbPool,
    }

    impl DeleteTodoHandler {
        pub fn new(pool: DbPool) -> Self {
            Self { pool }
        }
    }

    impl CommandHandlerPort<DeleteTodoCommand, TodoEvent> for DeleteTodoHandler {
        fn execute(&self, command: DeleteTodoCommand) -> Result<Vec<TodoEvent>, CommandError> {
            let todo = super::infra_diesel::delete_from_command(&self.pool, command)
                .map_err(|e| CommandError::HandlerExecution(e.to_string()))?;

            Ok(vec![TodoEvent::TodoDeleted(TodoDeleted { todo })])
        }
    }
}

mod infra_diesel {
    use super::models::DeleteTodoCommand;
    use crate::assembly::io::{AppError, DbPool, TodoDto, TodoRow};
    use crate::schema::todos;
    use diesel::prelude::*;

    pub fn delete_from_command(
        pool: &DbPool,
        command: DeleteTodoCommand,
    ) -> Result<TodoDto, AppError> {
        let mut conn = pool.get().map_err(|e| AppError::Storage(e.into()))?;
        let row = diesel::delete(todos::table.find(command.todo_id))
            .get_result::<TodoRow>(&mut conn)
            .optional()
            .map_err(|e| AppError::Storage(e.into()))?
            .ok_or(AppError::NotFound)?;
        row.try_into()
    }
}

mod http {
    use super::models::DeleteTodoCommand;
    use crate::{
        AppState,
        assembly::io::{
            ApiError, AppCommand, AppError, MulacState, NewCommandEnvelope,
            interpret_dispatch_error,
        },
        //
    };
    use poem::web::Data;
    use poem_openapi::{ApiResponse, OpenApi, param::Path};
    use uuid::Uuid;

    #[derive(ApiResponse)]
    enum DeleteResponse {
        #[oai(status = 204)]
        NoContent,
    }

    fn dispatch_delete_todo(mulac: &MulacState, id: Uuid) -> Result<(), AppError> {
        let command_id = Uuid::now_v7();
        let envelope = NewCommandEnvelope {
            command: AppCommand::DeleteTodo(DeleteTodoCommand { todo_id: id }),
            metadata: kernel::NewCommandMetadata {
                command_id,
                correlation_id: Some(command_id),
                causation_id: None,
                source: Some("test_app_todo.http".to_string()),
            },
        };
        mulac
            .dispatch_command(envelope)
            .map_err(interpret_dispatch_error)
    }

    pub struct Api;

    #[OpenApi]
    impl Api {
        #[oai(path = "/todos/:id", method = "delete")]
        async fn delete_todo(
            &self,
            state: Data<&AppState>,
            id: Path<Uuid>,
        ) -> Result<DeleteResponse, ApiError> {
            dispatch_delete_todo(&state.mulac, id.0)?;
            Ok(DeleteResponse::NoContent)
        }
    }
}

pub mod io {
    pub use super::DELETE_TODO_COMMAND;
    pub use super::TODO_DELETED_EVENT;
    pub use super::handler::DeleteTodoHandler;
    pub use super::http::Api;
    pub use super::models::{DeleteTodoCommand, TodoDeleted};
}
