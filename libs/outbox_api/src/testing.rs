//! Shared contract fixtures and test doubles for adapter tests.
//!
//! Doubles implement the existing `mulac-outbox` ports, so no database or
//! broker is required. Every `*_api` crate exposes the same run-request table,
//! so drift between their copied run types fails a test.
pub mod io {
    pub use super::{
        CONVERSION_FAILURE,
        Outcomes,
        PROCESS_FAILURE,
        PUBLISH_FAILURE,
        RESERVATION_FAILURE,
        empty_service,
        failing_publisher_and_process_service,
        failing_publisher_service,
        failing_reservation_service,
        invalid_run_requests,
        missing_reservation_service,
        panicking_service,
        partially_failing_service,
        publisher_service,
        queued_service,
        recording_service,
        run_request_error_messages,
        unroutable_service,
        valid_run_requests,
        //
    };
}

use crate::service::io::OutboxService;
use chrono::Utc;
use outbox::io::{
    OutboundMessageEnvelope,
    OutboxConsumer,
    OutboxConsumerRepository,
    OutboxEntry,
    OutboxEntryEnvelope,
    OutboxEntryMetadata,
    OutboxError,
    OutboxProcessPort,
    OutboxPublisherPort,
    OutboxReservePort,
    OutboxStatus,
    ReservableOutboxSpec,
    //
};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Error text reported by [`failing_reservation_service`].
pub const RESERVATION_FAILURE: &str = "storage error: reservation unavailable";
/// Error text reported when the publisher rejects an entry.
pub const PUBLISH_FAILURE: &str = "transport error: broker unavailable";
/// Error text reported when a failed entry cannot be marked failed.
pub const PROCESS_FAILURE: &str = "storage error: failure could not be recorded";
/// Error text reported for an entry without a routing key, which becomes dead.
pub const CONVERSION_FAILURE: &str = "conversion error: missing routing_key";

/// Lifecycle calls in order: `"completed"`, `"failed"` or `"dead"`.
pub type Outcomes = Arc<Mutex<Vec<&'static str>>>;

/// Routing key the publisher double rejects.
const FAILING_ROUTE: &str = "failing";

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

/// Holds due entries and reserves up to the batch limit, oldest first.
struct QueuedEntries {
    remaining: Mutex<VecDeque<OutboxEntryEnvelope>>,
}

impl OutboxReservePort for QueuedEntries {
    fn reserve(
        &self,
        spec: &ReservableOutboxSpec,
    ) -> Result<Vec<OutboxEntryEnvelope>, OutboxError> {
        let mut remaining = self.remaining.lock().unwrap();
        let count = remaining.len().min(spec.limit);
        Ok(remaining.drain(..count).collect())
    }
}

fn reserved_entry(routing_key: &str, reservation_id: Option<Uuid>) -> OutboxEntryEnvelope {
    let now = Utc::now();
    let id = Uuid::now_v7();
    let metadata = OutboxEntryMetadata {
        event_id: id,
        message_id: id,
        correlation_id: None,
        causation_id: None,
        event_type: "test".into(),
        routing_key: routing_key.into(),
        source: None,
        content_type: None,
    };
    OutboxEntryEnvelope {
        message: OutboxEntry {
            id,
            status: OutboxStatus::Reserved,
            payload: "{}".into(),
            meta: metadata.clone(),
            scheduled_at: now,
            attempts: 1,
            reservation_id,
            reserved_at: reservation_id.map(|_| now),
            received_at: now,
            updated_at: now,
            processed_at: None,
            last_error: None,
            extra_info: None,
        },
        metadata,
    }
}

struct FailingReservation;

impl OutboxReservePort for FailingReservation {
    fn reserve(&self, _: &ReservableOutboxSpec) -> Result<Vec<OutboxEntryEnvelope>, OutboxError> {
        Err(OutboxError::Storage("reservation unavailable".into()))
    }
}

struct PanickingReservation;

impl OutboxReservePort for PanickingReservation {
    fn reserve(&self, _: &ReservableOutboxSpec) -> Result<Vec<OutboxEntryEnvelope>, OutboxError> {
        panic!("private reservation diagnostics")
    }
}

/// Records lifecycle calls; optionally fails to mark entries failed.
struct Process {
    outcomes: Outcomes,
    fail_marking: bool,
}

impl OutboxProcessPort for Process {
    fn completed(&self, _: Uuid, _: Uuid) -> Result<(), OutboxError> {
        self.outcomes.lock().unwrap().push("completed");
        Ok(())
    }

    fn failed(
        &self,
        _: Uuid,
        _: Uuid,
        max_attempts: i32,
        _: Option<String>,
    ) -> Result<(), OutboxError> {
        // The HTTP contract always uses the default attempt limit.
        assert_eq!(max_attempts, ReservableOutboxSpec::DEFAULT_MAX_ATTEMPTS);
        self.outcomes.lock().unwrap().push("failed");
        if self.fail_marking {
            return Err(OutboxError::Storage("failure could not be recorded".into()));
        }
        Ok(())
    }

    fn dead(&self, _: Uuid, _: Uuid, _: Option<String>) -> Result<(), OutboxError> {
        self.outcomes.lock().unwrap().push("dead");
        Ok(())
    }
}

/// Records the routing key of every published entry and rejects
/// [`FAILING_ROUTE`].
struct RecordingPublisher {
    published: Arc<Mutex<Vec<String>>>,
}

impl OutboxPublisherPort for RecordingPublisher {
    fn publish(&self, envelope: OutboundMessageEnvelope) -> Result<(), OutboxError> {
        if envelope.metadata.routing_key == FAILING_ROUTE {
            return Err(OutboxError::Transport("broker unavailable".into()));
        }
        self.published
            .lock()
            .unwrap()
            .push(envelope.metadata.routing_key);
        Ok(())
    }
}

fn service(
    reserve: Arc<dyn OutboxReservePort>,
    publisher: Arc<dyn OutboxPublisherPort>,
    fail_marking: bool,
) -> (OutboxService, Outcomes) {
    let outcomes = Outcomes::default();
    let process = Arc::new(Process {
        outcomes: outcomes.clone(),
        fail_marking,
    });
    let repository = OutboxConsumerRepository::new(reserve, process);
    let consumer = Arc::new(OutboxConsumer::new(repository, publisher));
    (OutboxService::new(consumer), outcomes)
}

fn recording(
    reserve: Arc<dyn OutboxReservePort>,
    fail_marking: bool,
) -> (OutboxService, Outcomes, Arc<Mutex<Vec<String>>>) {
    let published = Arc::<Mutex<Vec<String>>>::default();
    let publisher = Arc::new(RecordingPublisher {
        published: published.clone(),
    });
    let (service, outcomes) = service(reserve, publisher, fail_marking);
    (service, outcomes, published)
}

fn queue(routing_keys: &[&str]) -> Arc<dyn OutboxReservePort> {
    entries(
        routing_keys
            .iter()
            .map(|key| reserved_entry(key, Some(Uuid::now_v7())))
            .collect(),
    )
}

fn entries(entries: VecDeque<OutboxEntryEnvelope>) -> Arc<dyn OutboxReservePort> {
    Arc::new(QueuedEntries {
        remaining: Mutex::new(entries),
    })
}

/// Has no due entries.
pub fn empty_service() -> OutboxService {
    queued_service(0)
}

/// Publishes `remaining` due entries successfully, at most `limit` per batch.
pub fn queued_service(remaining: usize) -> OutboxService {
    recording(queue(&vec!["test"; remaining]), false).0
}

/// Publishes `remaining` due entries through the given publisher.
pub fn publisher_service(
    remaining: usize,
    publisher: Arc<dyn OutboxPublisherPort>,
) -> OutboxService {
    service(queue(&vec!["test"; remaining]), publisher, false).0
}

/// Publishes entries with the given routing keys in order; the returned list
/// records the routing key of every published entry.
pub fn recording_service(routing_keys: &[&str]) -> (OutboxService, Arc<Mutex<Vec<String>>>) {
    let (service, _, published) = recording(queue(routing_keys), false);
    (service, published)
}

/// Fails reservation, reporting [`RESERVATION_FAILURE`].
pub fn failing_reservation_service() -> OutboxService {
    recording(Arc::new(FailingReservation), false).0
}

/// Fails publishing its one due entry, reporting [`PUBLISH_FAILURE`]. The
/// entry is marked failed with the default six attempts.
pub fn failing_publisher_service() -> OutboxService {
    recording(queue(&[FAILING_ROUTE]), false).0
}

/// Fails publishing and then marking the entry failed, reporting
/// [`PROCESS_FAILURE`] followed by [`PUBLISH_FAILURE`].
pub fn failing_publisher_and_process_service() -> OutboxService {
    recording(queue(&[FAILING_ROUTE]), true).0
}

/// Holds three due entries whose middle one fails to publish, reporting
/// [`PUBLISH_FAILURE`]; the returned outcomes record the lifecycle calls.
pub fn partially_failing_service() -> (OutboxService, Outcomes) {
    let (service, outcomes, _) = recording(queue(&["test", FAILING_ROUTE, "test"]), false);
    (service, outcomes)
}

/// Holds one entry without a routing key, which is marked dead and reported
/// as [`CONVERSION_FAILURE`] without publishing.
pub fn unroutable_service() -> (OutboxService, Outcomes) {
    let (service, outcomes, _) = recording(queue(&["   "]), false);
    (service, outcomes)
}

/// Holds one entry without a reservation ID, reported as
/// `missing reservation for entry <id>` without any lifecycle call.
pub fn missing_reservation_service() -> OutboxService {
    let entry = reserved_entry("test", None);
    recording(entries(VecDeque::from([entry])), false).0
}

/// Panics during reservation.
pub fn panicking_service() -> OutboxService {
    recording(Arc::new(PanickingReservation), false).0
}
