//! Shared contract fixtures and test doubles for adapter tests.
//!
//! Doubles implement the existing `mulac-eventing` ports, so no database is
//! required. Queue HTTP boundaries expose the same run-request table, so drift
//! between their run types fails a test.
pub mod prelude {
    pub use super::Repository;
    pub use super::{
        PROCESS_FAILURE,
        RESERVATION_FAILURE,
        SUBSCRIBER_FAILURE,
        empty_consumer,
        failing_reservation_consumer,
        failing_subscriber_and_process_consumer,
        failing_subscriber_consumer,
        invalid_run_requests,
        malformed_run_requests,
        panicking_consumer,
        partially_failing_consumer,
        queued_consumer,
        recording_consumer,
        run_request_error_messages,
        valid_run_requests,
        //
    };
}

use crate::prelude::{
    EventConsumer,
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

/// Error text reported by [`failing_reservation_consumer`].
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

/// Bodies every adapter must reject with `400`, showing only an error ID.
pub fn malformed_run_requests() -> Vec<Value> {
    vec![
        json!({"limit":"1"}),
        json!({}),
        json!(5),
        json!(null),
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

fn consumer(
    reserve: Arc<dyn EventReservePort>,
    fail_marking: bool,
) -> (Arc<EventConsumer<Repository>>, Doubles) {
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
    let repository = Arc::new(Repository { reserve, process });
    (
        Arc::new(EventConsumer::new(repository, dispatcher)),
        doubles,
    )
}

fn queue(event_types: &[&str]) -> Arc<dyn EventReservePort> {
    Arc::new(QueuedEvents {
        remaining: Mutex::new(event_types.iter().map(|t| reserved_event(t)).collect()),
    })
}

/// Has no due events.
pub fn empty_consumer() -> Arc<EventConsumer<Repository>> {
    queued_consumer(0)
}

/// Dispatches `remaining` due events successfully, at most `limit` per batch.
pub fn queued_consumer(remaining: usize) -> Arc<EventConsumer<Repository>> {
    consumer(queue(&vec!["test"; remaining]), false).0
}

/// Dispatches the given event types in order; the returned list records the
/// type of every event the subscriber handled.
pub fn recording_consumer(
    event_types: &[&str],
) -> (Arc<EventConsumer<Repository>>, Arc<Mutex<Vec<String>>>) {
    let (consumer, doubles) = consumer(queue(event_types), false);
    (consumer, doubles.handled)
}

/// Fails reservation, reporting [`RESERVATION_FAILURE`].
pub fn failing_reservation_consumer() -> Arc<EventConsumer<Repository>> {
    consumer(Arc::new(FailingReservation), false).0
}

/// Fails the subscriber of its one due event, reporting [`SUBSCRIBER_FAILURE`].
/// The event is marked failed with the default six attempts.
pub fn failing_subscriber_consumer() -> Arc<EventConsumer<Repository>> {
    consumer(queue(&[FAILING_EVENT]), false).0
}

/// Fails the subscriber and then marking the event failed, reporting
/// [`PROCESS_FAILURE`] followed by [`SUBSCRIBER_FAILURE`].
pub fn failing_subscriber_and_process_consumer() -> Arc<EventConsumer<Repository>> {
    consumer(queue(&[FAILING_EVENT]), true).0
}

/// Holds three due events whose middle one fails, reporting
/// [`SUBSCRIBER_FAILURE`]; the returned counter records completed events.
pub fn partially_failing_consumer() -> (Arc<EventConsumer<Repository>>, Arc<Mutex<usize>>) {
    let (consumer, doubles) = consumer(queue(&["test", FAILING_EVENT, "test"]), false);
    (consumer, doubles.completed)
}

/// Panics during reservation.
pub fn panicking_consumer() -> Arc<EventConsumer<Repository>> {
    consumer(Arc::new(PanickingReservation), false).0
}

/// Combined reserve/process double used by HTTP fixtures.
pub struct Repository {
    reserve: Arc<dyn EventReservePort>,
    process: Arc<dyn EventProcessPort>,
}
impl EventReservePort for Repository {
    fn reserve(&self, spec: &ReservableEventSpec) -> Result<Vec<EventEnvelope>, EventError> {
        self.reserve.reserve(spec)
    }
}
impl EventProcessPort for Repository {
    fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), EventError> {
        self.process.completed(id, reservation_id)
    }
    fn failed(
        &self,
        id: Uuid,
        reservation_id: Uuid,
        max_attempts: i32,
        reason: Option<String>,
    ) -> Result<(), EventError> {
        self.process
            .failed(id, reservation_id, max_attempts, reason)
    }
}

#[cfg(test)]
mod repository_tests {
    use super::*;
    struct Borrowed<'a>(&'a Repository);
    impl EventReservePort for Borrowed<'_> {
        fn reserve(&self, spec: &ReservableEventSpec) -> Result<Vec<EventEnvelope>, EventError> {
            self.0.reserve(spec)
        }
    }
    impl EventProcessPort for Borrowed<'_> {
        fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), EventError> {
            self.0.completed(id, reservation_id)
        }
        fn failed(
            &self,
            id: Uuid,
            reservation_id: Uuid,
            max: i32,
            reason: Option<String>,
        ) -> Result<(), EventError> {
            self.0.failed(id, reservation_id, max, reason)
        }
    }
    #[test]
    fn synchronous_consumer_accepts_borrowed_repositories() {
        let repository = Repository {
            reserve: queue(&["test"]),
            process: Arc::new(Process {
                completed: Arc::default(),
                fail_marking: false,
            }),
        };
        let dispatcher = Arc::new(EventDispatcher::new(Arc::new(Subscriber {
            handled: Arc::default(),
        })));
        let consumer = EventConsumer::new(Arc::new(Borrowed(&repository)), dispatcher);
        assert_eq!(consumer.consume(&ReservableEventSpec::new(1)).unwrap(), 1);
    }
}
