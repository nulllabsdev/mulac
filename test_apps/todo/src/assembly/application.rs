use super::domain::{TodoDto, TodoStatus};
use super::infra_diesel::DbPool;
use super::infra_diesel::entity::TodoRow;
use crate::task_complete::io::{COMPLETE_TODO_COMMAND, CompleteTodoCommand};
use crate::task_create::io::{CREATE_TODO_COMMAND, CreateTodoCommand};
use crate::task_delete::io::{DELETE_TODO_COMMAND, DeleteTodoCommand};
use crate::task_reopen::io::{REOPEN_TODO_COMMAND, ReopenTodoCommand};
use crate::task_schedule_due_dates::io::{
    UPDATE_DUE_DATE_COMMAND,
    UpdateDueDateCommand, //
};
use crate::task_update::io::{UPDATE_TODO_COMMAND, UpdateTodoCommand};
use kernel::io::CommandError as MulacCommandError;
use poem::{IntoResponse, Response, http::StatusCode};
use poem_openapi::Object;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Object)]
pub struct ErrorBody {
    pub error: String,
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("todo not found")]
    NotFound,
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("{0}")]
    Conflict(String),
    #[error("storage error: {0}")]
    Storage(#[from] anyhow::Error),
}

pub type ApiError = poem::Error;

impl From<AppError> for poem::Error {
    fn from(error: AppError) -> Self {
        let status = match error {
            AppError::NotFound => StatusCode::NOT_FOUND,
            AppError::Validation(_) => StatusCode::BAD_REQUEST,
            AppError::Conflict(_) => StatusCode::CONFLICT,
            AppError::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        poem::Error::from_response(error_response(status, error.to_string()))
    }
}

fn error_response(status: StatusCode, message: String) -> Response {
    (status, poem::web::Json(ErrorBody { error: message })).into_response()
}

impl TryFrom<TodoRow> for TodoDto {
    type Error = AppError;

    fn try_from(row: TodoRow) -> Result<Self, Self::Error> {
        let status = match row.status.as_str() {
            "active" => TodoStatus::Active,
            "completed" => TodoStatus::Completed,
            "archived" => TodoStatus::Archived,
            other => {
                return Err(AppError::Storage(anyhow::anyhow!(
                    "unknown todo status `{other}`"
                )));
            }
        };
        Ok(TodoDto {
            id: row.id,
            title: row.title,
            description: row.description,
            status,
            created_at: row.created_at,
            updated_at: row.updated_at,
            due_at: row.due_at,
        })
    }
}

pub fn validate_title(title: &str) -> Result<(), AppError> {
    if title.trim().is_empty() {
        return Err(AppError::Validation("title must not be blank".to_string()));
    }
    Ok(())
}

pub fn interpret_dispatch_error(error: kernel::KernelError) -> AppError {
    if let kernel::KernelError::Command(MulacCommandError::HandlerExecution(message)) = &error {
        if message.contains("todo not found") {
            return AppError::NotFound;
        }

        if let Some(message) = message.strip_prefix("validation failed: ") {
            return AppError::Validation(message.to_string());
        }
    }

    AppError::Storage(anyhow::anyhow!("command dispatch failed: {error}"))
}

pub trait Command: kernel::ApplicationCommand {
    fn todo_id(&self) -> Uuid;
}

#[derive(Debug, Clone)]
pub enum AppCommand {
    CreateTodo(CreateTodoCommand),
    CompleteTodo(CompleteTodoCommand),
    ReopenTodo(ReopenTodoCommand),
    UpdateTodo(UpdateTodoCommand),
    DeleteTodo(DeleteTodoCommand),
    UpdateDueDate(UpdateDueDateCommand),
}

impl serde::Serialize for AppCommand {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::CreateTodo(c) => c.serialize(serializer),
            Self::CompleteTodo(c) => c.serialize(serializer),
            Self::ReopenTodo(c) => c.serialize(serializer),
            Self::UpdateTodo(c) => c.serialize(serializer),
            Self::DeleteTodo(c) => c.serialize(serializer),
            Self::UpdateDueDate(c) => c.serialize(serializer),
        }
    }
}

impl kernel::ApplicationCommand for AppCommand {
    fn command_type(&self) -> &'static str {
        match self {
            Self::CreateTodo(_) => CREATE_TODO_COMMAND,
            Self::CompleteTodo(_) => COMPLETE_TODO_COMMAND,
            Self::ReopenTodo(_) => REOPEN_TODO_COMMAND,
            Self::UpdateTodo(_) => UPDATE_TODO_COMMAND,
            Self::DeleteTodo(_) => DELETE_TODO_COMMAND,
            Self::UpdateDueDate(_) => UPDATE_DUE_DATE_COMMAND,
        }
    }
}

impl Command for AppCommand {
    fn todo_id(&self) -> Uuid {
        match self {
            Self::CreateTodo(c) => c.todo_id,
            Self::CompleteTodo(c) => c.todo_id,
            Self::ReopenTodo(c) => c.todo_id,
            Self::UpdateTodo(c) => c.todo_id,
            Self::DeleteTodo(c) => c.todo_id,
            Self::UpdateDueDate(c) => c.todo_id,
        }
    }
}

pub type NewCommandEnvelope = kernel::NewCommandEnvelope<AppCommand>;
pub type MulacState = kernel::PersistentKernelState;
pub type MulacHandle = kernel::PersistentKernelHandle;

pub async fn start_mulac(db_pool: DbPool) -> Result<MulacHandle, kernel::KernelError> {
    use crate::assembly::io::OutboxSubscriber;
    use crate::task_complete::io::{CompleteTodoHandler, TODO_COMPLETED_EVENT};
    use crate::task_create::io::{CreateTodoHandler, TODO_CREATED_EVENT};
    use crate::task_delete::io::{DeleteTodoHandler, TODO_DELETED_EVENT};
    use crate::task_reopen::io::{ReopenTodoHandler, TODO_REOPENED_EVENT};
    use crate::task_schedule_due_dates::io::{TODO_DUE_DATE_CHANGED_EVENT, UpdateDueDateHandler};
    use crate::task_update::io::{TODO_UPDATED_EVENT, UpdateTodoHandler};

    kernel::boot(kernel::KernelConfig::default())
        .command_handler(
            CREATE_TODO_COMMAND,
            Arc::new(CreateTodoHandler::new(db_pool.clone())),
        )
        .command_handler(
            COMPLETE_TODO_COMMAND,
            Arc::new(CompleteTodoHandler::new(db_pool.clone())),
        )
        .command_handler(
            REOPEN_TODO_COMMAND,
            Arc::new(ReopenTodoHandler::new(db_pool.clone())),
        )
        .command_handler(
            UPDATE_TODO_COMMAND,
            Arc::new(UpdateTodoHandler::new(db_pool.clone())),
        )
        .command_handler(
            DELETE_TODO_COMMAND,
            Arc::new(DeleteTodoHandler::new(db_pool.clone())),
        )
        .command_handler(
            UPDATE_DUE_DATE_COMMAND,
            Arc::new(UpdateDueDateHandler::new(db_pool.clone())),
        )
        .event_subscriber(
            TODO_CREATED_EVENT,
            "todo-created-outbox",
            Arc::new(OutboxSubscriber::new(db_pool.clone()))
                as Arc<dyn kernel::EventSubscriberPort>,
        )
        .event_subscriber(
            TODO_COMPLETED_EVENT,
            "todo-completed-outbox",
            Arc::new(OutboxSubscriber::new(db_pool.clone()))
                as Arc<dyn kernel::EventSubscriberPort>,
        )
        .event_subscriber(
            TODO_REOPENED_EVENT,
            "todo-reopened-outbox",
            Arc::new(OutboxSubscriber::new(db_pool.clone()))
                as Arc<dyn kernel::EventSubscriberPort>,
        )
        .event_subscriber(
            TODO_UPDATED_EVENT,
            "todo-updated-outbox",
            Arc::new(OutboxSubscriber::new(db_pool.clone()))
                as Arc<dyn kernel::EventSubscriberPort>,
        )
        .event_subscriber(
            TODO_DUE_DATE_CHANGED_EVENT,
            "todo-due-date-changed-outbox",
            Arc::new(OutboxSubscriber::new(db_pool.clone()))
                as Arc<dyn kernel::EventSubscriberPort>,
        )
        .event_subscriber(
            TODO_DELETED_EVENT,
            "todo-deleted-outbox",
            Arc::new(OutboxSubscriber::new(db_pool.clone()))
                as Arc<dyn kernel::EventSubscriberPort>,
        )
        .start_persistent(db_pool, 1)
}

pub use kernel::io::{block_on_blocking, run_command_worker, run_event_worker};

/// Run a blocking (diesel) closure on Tokio's blocking thread pool so it does
/// not stall the async runtime. Used by the async HTTP handlers.
pub async fn run_blocking<F, T>(f: F) -> Result<T, AppError>
where
    F: FnOnce() -> Result<T, AppError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| AppError::Storage(anyhow::anyhow!("blocking task join failed: {e}")))?
}
