pub const CREATE_TODO_COMMAND: &str = "CreateTodo";
pub const TODO_CREATED_EVENT: &str = "TodoCreated";

mod models {
    use crate::assembly::io::TodoDto;
    use chrono::{DateTime, Utc};
    use kernel::ApplicationEvent;
    use poem_openapi::Object;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[derive(Debug, Clone, Serialize, Deserialize, Object)]
    pub struct CreateTodoCommand {
        pub todo_id: Uuid,
        pub title: String,
        pub description: Option<String>,
        pub due_at: Option<DateTime<Utc>>,
    }

    impl kernel::ApplicationCommand for CreateTodoCommand {
        fn command_type(&self) -> &'static str {
            super::CREATE_TODO_COMMAND
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Object)]
    pub struct TodoCreated {
        pub todo: TodoDto,
    }

    impl ApplicationEvent for TodoCreated {
        fn event_type(&self) -> &'static str {
            super::TODO_CREATED_EVENT
        }
    }
}

mod handler {
    use super::models::{CreateTodoCommand, TodoCreated};
    use crate::assembly::io::{DbPool, TodoEvent};
    use kernel::{CommandError, CommandHandlerPort};

    pub struct CreateTodoHandler {
        pool: DbPool,
    }

    impl CreateTodoHandler {
        pub fn new(pool: DbPool) -> Self {
            Self { pool }
        }
    }

    impl CommandHandlerPort<CreateTodoCommand, TodoEvent> for CreateTodoHandler {
        fn execute(&self, command: CreateTodoCommand) -> Result<Vec<TodoEvent>, CommandError> {
            let todo = super::infra_diesel::create_from_command(&self.pool, command)
                .map_err(|e| CommandError::HandlerExecution(e.to_string()))?;

            Ok(vec![TodoEvent::TodoCreated(TodoCreated { todo })])
        }
    }
}

mod infra_diesel {
    use super::models::CreateTodoCommand;
    use crate::assembly::io::{
        AppError,
        Clock,
        DbPool,
        TodoDto,
        TodoRow,
        TodoStatus,
        validate_title,
        //
    };
    use crate::schema::todos;
    use diesel::prelude::*;

    pub fn create_from_command(
        pool: &DbPool,
        command: CreateTodoCommand,
    ) -> Result<TodoDto, AppError> {
        validate_title(&command.title)?;
        let now = Clock::now();
        let mut conn = pool.get().map_err(|e| AppError::Storage(e.into()))?;
        let row = diesel::insert_into(todos::table)
            .values((
                todos::id.eq(command.todo_id),
                todos::title.eq(command.title.trim()),
                todos::description.eq(command.description.as_deref()),
                todos::status.eq(TodoStatus::Active.as_str()),
                todos::created_at.eq(now),
                todos::updated_at.eq(now),
                todos::due_at.eq(command.due_at),
            ))
            .get_result::<TodoRow>(&mut conn)
            .map_err(|e| AppError::Storage(e.into()))?;
        row.try_into()
    }
}

mod http {
    use super::models::CreateTodoCommand;
    use crate::assembly::io::{AppCommand, AppError, NewCommandEnvelope};
    use crate::{
        AppState,
        assembly::io::{
            ApiError, Command, TodoDto, fetch_todo, interpret_dispatch_error, run_blocking,
        },
        //
    };
    use chrono::{DateTime, Utc};
    use kernel::NewCommandMetadata;
    use poem::web::Data;
    use poem_openapi::{Object, OpenApi, payload::Json};
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[derive(Debug, Clone, Serialize, Deserialize, Object)]
    pub struct CreateTodoRequest {
        pub title: String,
        pub description: Option<String>,
        pub due_at: Option<DateTime<Utc>>,
    }

    impl TryFrom<CreateTodoRequest> for NewCommandEnvelope {
        type Error = AppError;

        fn try_from(request: CreateTodoRequest) -> Result<Self, Self::Error> {
            let todo_id = Uuid::now_v7();
            let command_id = Uuid::now_v7();
            Ok(NewCommandEnvelope {
                command: AppCommand::CreateTodo(CreateTodoCommand {
                    todo_id,
                    title: request.title,
                    description: request.description,
                    due_at: request.due_at,
                }),
                metadata: NewCommandMetadata {
                    command_id,
                    correlation_id: Some(command_id),
                    causation_id: None,
                    source: Some("test_app_todo.http".to_string()),
                },
            })
        }
    }

    pub struct Api;

    #[OpenApi]
    impl Api {
        #[oai(path = "/todos", method = "post")]
        async fn create_todo(
            &self,
            state: Data<&AppState>,
            Json(request): Json<CreateTodoRequest>,
        ) -> Result<Json<TodoDto>, ApiError> {
            let envelope: NewCommandEnvelope = request.try_into()?;
            let todo_id = envelope.command.todo_id();
            state
                .mulac
                .dispatch_command(envelope)
                .map_err(interpret_dispatch_error)?;
            let pool = state.pool.clone();
            let todo = run_blocking(move || fetch_todo(&pool, todo_id)).await?;
            Ok(Json(todo))
        }
    }
}

pub mod io {
    pub use super::CREATE_TODO_COMMAND;
    pub use super::TODO_CREATED_EVENT;
    pub use super::handler::CreateTodoHandler;
    pub use super::http::Api;
    pub use super::models::{CreateTodoCommand, TodoCreated};
}
