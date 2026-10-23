//! Shared contract fixtures and test doubles for adapter tests.
//!
//! Doubles implement the existing `mulac-commanding` ports, so no database is
//! required. Every `*_api` crate exposes the same run-request table, so drift
//! between their copied run types fails a test.
pub mod io {
    pub use super::{
        HANDLER_FAILURE,
        PROCESS_FAILURE,
        RESERVATION_FAILURE,
        empty_service,
        failing_handler_and_process_service,
        failing_handler_service,
        failing_reservation_service,
        invalid_run_requests,
        panicking_service,
        queued_service,
        run_request_error_messages,
        valid_run_requests,
        //
    };
}

use crate::service::io::CommandingService;
use commanding::io::{
    Command,
    CommandConsumer,
    CommandConsumerRepository,
    CommandDispatcher,
    CommandEnvelope,
    CommandError,
    CommandProcessPort,
    CommandReservePort,
    ErasedCommandHandler,
    NewCommandEnvelope,
    ReservableCommandSpec,
    //
};
use eventing::io::{
    EventDispatchPort,
    EventError,
    NewEventEnvelope,
    //
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Error text reported by [`failing_reservation_service`].
pub const RESERVATION_FAILURE: &str = "reservation error: reservation unavailable";
/// Error text reported when a command handler fails.
pub const HANDLER_FAILURE: &str = "handler execution error: handler failed";
/// Error text reported when a failed command cannot be marked failed.
pub const PROCESS_FAILURE: &str = "storage error: failure could not be recorded";

/// Bodies every adapter must accept.
pub fn valid_run_requests() -> Vec<Value> {
    vec![
        json!({"limit":1}),
        json!({"limit":1000}),
        json!({"limit":1,"extra":true}),
    ]
}

/// Bodies every adapter must reject with `400`.
pub fn invalid_run_requests() -> Vec<Value> {
    vec![
        json!({}),
        json!({"limit":0}),
        json!({"limit":1001}),
        json!({"limit":-1}),
        json!({"limit":1.0}),
        json!({"limit":1.5}),
        json!({"limit":"1"}),
        json!({"limit":null}),
        json!([]),
    ]
}

/// Invalid bodies with their exact Mulac error text.
///
/// Axum returns the text verbatim. Poem's `RunRequest` wrapper validates while
/// parsing, so Poem includes it in its parse-error message.
pub fn run_request_error_messages() -> Vec<(Value, &'static str)> {
    vec![
        (json!({"limit":0}), "limit must be between 1 and 1000"),
        (json!({"limit":1001}), "limit must be between 1 and 1000"),
        (json!({}), "missing field `limit`"),
    ]
}

/// Holds `remaining` due commands and reserves up to the batch limit.
struct QueuedCommands {
    remaining: Mutex<usize>,
}

impl CommandReservePort for QueuedCommands {
    fn reserve(&self, spec: &ReservableCommandSpec) -> Result<Vec<CommandEnvelope>, CommandError> {
        let mut remaining = self.remaining.lock().unwrap();
        let count = (*remaining).min(spec.limit);
        *remaining -= count;
        Ok((0..count).map(|_| reserved_command()).collect())
    }
}

fn reserved_command() -> CommandEnvelope {
    CommandEnvelope {
        command: Command {
            id: Uuid::now_v7(),
            reservation_id: Uuid::now_v7(),
            command_type: "test".into(),
            payload: "{}".into(),
            attempts: 1,
            extra_info: None,
        },
        metadata: None,
    }
}

struct FailingReservation;

impl CommandReservePort for FailingReservation {
    fn reserve(&self, _: &ReservableCommandSpec) -> Result<Vec<CommandEnvelope>, CommandError> {
        Err(CommandError::Reservation("reservation unavailable".into()))
    }
}

struct PanickingReservation;

impl CommandReservePort for PanickingReservation {
    fn reserve(&self, _: &ReservableCommandSpec) -> Result<Vec<CommandEnvelope>, CommandError> {
        panic!("private reservation diagnostics")
    }
}

/// Accepts completion; optionally fails to mark commands failed.
struct Process {
    fail_marking: bool,
}

impl CommandProcessPort for Process {
    fn completed(&self, _: Uuid, _: Uuid) -> Result<(), CommandError> {
        Ok(())
    }

    fn failed(&self, _: Uuid, _: Uuid, _: i32, _: Option<String>) -> Result<(), CommandError> {
        if self.fail_marking {
            return Err(CommandError::Storage(
                "failure could not be recorded".into(),
            ));
        }
        Ok(())
    }
}

/// Succeeds without emitting events, or fails every command.
struct Handler {
    fail: bool,
}

impl ErasedCommandHandler for Handler {
    fn execute(&self, _: &NewCommandEnvelope) -> Result<Vec<NewEventEnvelope>, CommandError> {
        if self.fail {
            return Err(CommandError::HandlerExecution("handler failed".into()));
        }
        Ok(vec![])
    }
}

/// Never called: the handler doubles emit no events.
struct UnusedEvents;

impl EventDispatchPort for UnusedEvents {
    fn dispatch(&self, _: NewEventEnvelope) -> Result<(), EventError> {
        unreachable!("the test handler emits no events")
    }
}

fn service(
    reserve: Arc<dyn CommandReservePort>,
    fail_handler: bool,
    fail_marking: bool,
) -> CommandingService {
    let dispatcher = Arc::new(CommandDispatcher::new(
        Arc::new(Handler { fail: fail_handler }),
        Arc::new(UnusedEvents),
    ));
    let repository = CommandConsumerRepository::new(reserve, Arc::new(Process { fail_marking }));
    CommandingService::new(Arc::new(CommandConsumer::new(repository, dispatcher)))
}

fn queue(remaining: usize) -> Arc<dyn CommandReservePort> {
    Arc::new(QueuedCommands {
        remaining: Mutex::new(remaining),
    })
}

/// Has no due commands.
pub fn empty_service() -> CommandingService {
    queued_service(0)
}

/// Executes `remaining` due commands successfully, at most `limit` per batch.
pub fn queued_service(remaining: usize) -> CommandingService {
    service(queue(remaining), false, false)
}

/// Fails reservation, reporting [`RESERVATION_FAILURE`].
pub fn failing_reservation_service() -> CommandingService {
    service(Arc::new(FailingReservation), false, false)
}

/// Fails the handler of its one due command, reporting [`HANDLER_FAILURE`].
pub fn failing_handler_service() -> CommandingService {
    service(queue(1), true, false)
}

/// Fails the handler and then marking the command failed, reporting
/// [`PROCESS_FAILURE`] followed by [`HANDLER_FAILURE`].
pub fn failing_handler_and_process_service() -> CommandingService {
    service(queue(1), true, true)
}

/// Panics during reservation.
pub fn panicking_service() -> CommandingService {
    service(Arc::new(PanickingReservation), false, false)
}
