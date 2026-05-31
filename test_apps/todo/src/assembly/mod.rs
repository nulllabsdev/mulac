mod application;
mod domain;
mod infra_diesel;

pub mod io {
    pub use super::application::{
        ApiError,
        AppCommand,
        AppError,
        Command,
        ErrorBody,
        MulacHandle,
        MulacState,
        NewCommandEnvelope,
        block_on_blocking,
        interpret_dispatch_error,
        run_blocking,
        run_command_worker,
        run_event_worker,
        start_mulac,
        validate_title,
        //
    };
    pub use super::domain::{Clock, TodoDto, TodoList, TodoStatus};
    pub use super::infra_diesel::entity::TodoRow;
    pub use super::infra_diesel::{
        DbPool,
        OutboxSubscriber,
        build_pool,
        fetch_todo,
        record_event_payload,
        run_migrations,
        //
    };
    pub use crate::TodoEvent;
}
