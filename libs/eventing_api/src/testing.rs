//! Shared contract fixtures and test doubles for adapter tests.
//!
//! Doubles implement the existing `mulac-eventing` ports, so no database is
//! required. Every `*_api` crate exposes the same run-request table, so drift
//! between their copied run types fails a test.
pub mod io {
    pub use super::{
        PROCESS_FAILURE,
        RESERVATION_FAILURE,
        SUBSCRIBER_FAILURE,
        empty_service,
        failing_reservation_service,
        failing_subscriber_and_process_service,
        failing_subscriber_service,
        invalid_run_requests,
        panicking_service,
        partially_failing_service,
        queued_service,
        recording_service,
        run_request_error_messages,
        valid_run_requests,
        //
    };
}

use crate::service::io::EventingService;
use eventing::io::{
    EventConsumer,
    EventConsumerRepository,
    EventDispatcher,
    EventEnvelope,
    EventError,
    EventProcessPort,
    EventReservePort,
    EventSubscriberPort,
    NewEventEnvelope,
    ReservableEventSpec,
    //
};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Error text reported by [`failing_reservation_service`].
pub const RESERVATION_FAILURE: &str = "storage error: reservation unavailable";
/// Error text reported when a subscriber fails.
pub const SUBSCRIBER_FAILURE: &str = "subscriber execution error: subscriber failed";
/// Error text reported when a failed event cannot be marked failed.
pub const PROCESS_FAILURE: &str = "storage error: failure could not be recorded";

/// Event type the subscriber double rejects.
const FAILING_EVENT: &str = "failing";

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

/// Holds due events and reserves up to the batch limit, oldest first.
struct QueuedEvents {
    remaining: Mutex<VecDeque<EventEnvelope>>,
}

impl EventReservePort for QueuedEvents {
    fn reserve(&self, spec: &ReservableEventSpec) -> Result<Vec<EventEnvelope>, EventError> {
        let mut remaining = self.remaining.lock().unwrap();
        let count = remaining.len().min(spec.limit);
        Ok(remaining.drain(..count).collect())
    }
}

fn reserved_event(event_type: &str) -> EventEnvelope {
    EventEnvelope {
        id: Uuid::now_v7(),
        reservation_id: Uuid::now_v7(),
        event_type: event_type.into(),
        payload: "{}".into(),
        attempts: 1,
        metadata: None,
        extra_info: None,
    }
}

struct FailingReservation;

impl EventReservePort for FailingReservation {
    fn reserve(&self, _: &ReservableEventSpec) -> Result<Vec<EventEnvelope>, EventError> {
        Err(EventError::Storage("reservation unavailable".into()))
    }
}

struct PanickingReservation;

impl EventReservePort for PanickingReservation {
    fn reserve(&self, _: &ReservableEventSpec) -> Result<Vec<EventEnvelope>, EventError> {
        panic!("private reservation diagnostics")
    }
}

/// Counts completions; optionally fails to mark events failed.
struct Process {
    completed: Arc<Mutex<usize>>,
    fail_marking: bool,
}

impl EventProcessPort for Process {
    fn completed(&self, _: Uuid, _: Uuid) -> Result<(), EventError> {
        *self.completed.lock().unwrap() += 1;
        Ok(())
    }

    fn failed(
        &self,
        _: Uuid,
        _: Uuid,
        max_attempts: i32,
        _: Option<String>,
    ) -> Result<(), EventError> {
        // The HTTP contract always uses the default attempt limit.
        assert_eq!(max_attempts, ReservableEventSpec::DEFAULT_MAX_ATTEMPTS);
        if self.fail_marking {
            return Err(EventError::Storage("failure could not be recorded".into()));
        }
        Ok(())
    }
}

/// Records the type of every event it handles and fails [`FAILING_EVENT`].
struct Subscriber {
    handled: Arc<Mutex<Vec<String>>>,
}

impl EventSubscriberPort for Subscriber {
    fn handle(&self, envelope: &NewEventEnvelope) -> Result<(), EventError> {
        self.handled
            .lock()
            .unwrap()
            .push(envelope.event_type.clone());
        if envelope.event_type == FAILING_EVENT {
            return Err(EventError::SubscriberExecution("subscriber failed".into()));
        }
        Ok(())
    }
}

struct Doubles {
    handled: Arc<Mutex<Vec<String>>>,
    completed: Arc<Mutex<usize>>,
}

fn service(reserve: Arc<dyn EventReservePort>, fail_marking: bool) -> (EventingService, Doubles) {
    let doubles = Doubles {
        handled: Arc::default(),
        completed: Arc::default(),
    };
    let dispatcher = Arc::new(EventDispatcher::new(Arc::new(Subscriber {
        handled: doubles.handled.clone(),
    })));
    let process = Arc::new(Process {
        completed: doubles.completed.clone(),
        fail_marking,
    });
    let repository = EventConsumerRepository::new(reserve, process);
    let consumer = Arc::new(EventConsumer::new(repository, dispatcher));
    (EventingService::new(consumer), doubles)
}

fn queue(event_types: &[&str]) -> Arc<dyn EventReservePort> {
    Arc::new(QueuedEvents {
        remaining: Mutex::new(event_types.iter().map(|t| reserved_event(t)).collect()),
    })
}

/// Has no due events.
pub fn empty_service() -> EventingService {
    queued_service(0)
}

/// Dispatches `remaining` due events successfully, at most `limit` per batch.
pub fn queued_service(remaining: usize) -> EventingService {
    service(queue(&vec!["test"; remaining]), false).0
}

/// Dispatches the given event types in order; the returned list records the
/// type of every event the subscriber handled.
pub fn recording_service(event_types: &[&str]) -> (EventingService, Arc<Mutex<Vec<String>>>) {
    let (service, doubles) = service(queue(event_types), false);
    (service, doubles.handled)
}

/// Fails reservation, reporting [`RESERVATION_FAILURE`].
pub fn failing_reservation_service() -> EventingService {
    service(Arc::new(FailingReservation), false).0
}

/// Fails the subscriber of its one due event, reporting [`SUBSCRIBER_FAILURE`].
/// The event is marked failed with the default six attempts.
pub fn failing_subscriber_service() -> EventingService {
    service(queue(&[FAILING_EVENT]), false).0
}

/// Fails the subscriber and then marking the event failed, reporting
/// [`PROCESS_FAILURE`] followed by [`SUBSCRIBER_FAILURE`].
pub fn failing_subscriber_and_process_service() -> EventingService {
    service(queue(&[FAILING_EVENT]), true).0
}

/// Holds three due events whose middle one fails, reporting
/// [`SUBSCRIBER_FAILURE`]; the returned counter records completed events.
pub fn partially_failing_service() -> (EventingService, Arc<Mutex<usize>>) {
    let (service, doubles) = service(queue(&["test", FAILING_EVENT, "test"]), false);
    (service, doubles.completed)
}

/// Panics during reservation.
pub fn panicking_service() -> EventingService {
    service(Arc::new(PanickingReservation), false).0
}
