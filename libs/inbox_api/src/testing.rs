//! Shared contract fixtures and test doubles for adapter tests.
//!
//! Doubles implement the existing `mulac-inbox` and `mulac-commanding` ports,
//! so no database is required.
pub mod io {
    pub use super::{
        RESERVATION_FAILURE,
        RecordingStore,
        failing_storage_service,
        invalid_run_requests,
        invalid_submissions,
        panicking_service,
        recording_service,
        submission_error_messages,
        valid_run_requests,
        valid_submissions,
        //
    };
}

use crate::service::io::InboxService;
use commanding::io::{
    CommandError,
    CommandGateway,
    CommandRecorder,
    CommandRecorderRepository,
    CommandStorePort,
    NewCommandEnvelope,
    //
};
use inbox::io::{
    InboxConsumer,
    InboxConsumerRepository,
    InboxError,
    InboxMessageEnvelope,
    InboxProcessPort,
    InboxRecorder,
    InboxRecorderRepository,
    InboxReservePort,
    InboxStorePort,
    NewInboxMessageEnvelope,
    ReservableInboxSpec,
    //
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Error text reported by [`failing_storage_service`] when running a batch.
pub const RESERVATION_FAILURE: &str = "storage error: reservation unavailable";

/// Bodies every adapter must accept (they reach storage).
pub fn valid_submissions() -> Vec<Value> {
    let mut bodies: Vec<Value> = [
        json!(0),
        json!(false),
        json!({"nested":[null, true, 1]}),
        json!([1, "two"]),
        json!("text"),
        json!(42),
        json!(true),
    ]
    .into_iter()
    .map(|payload| json!({"payload":payload,"routing_key":"unknown"}))
    .collect();
    bodies.extend([
        json!({"payload":1,"routing_key":"x","extra":1}),
        json!({"payload":1,"routing_key":" x "}),
        json!({"payload":1,"routing_key":"x","message_id":null,"correlation_id":null}),
        json!({"payload":1,"routing_key":"x","message_id":"0191d6c4-8f5e-7a3b-9c1d-2e3f4a5b6c7d"}),
        json!({"payload":1,"routing_key":"x","message_id":"0191D6C4-8F5E-7A3B-9C1D-2E3F4A5B6C7D"}),
        json!({"payload":1,"routing_key":"x","correlation_id":"0191d6c48f5e7a3b9c1d2e3f4a5b6c7d"}),
        json!({"payload":1,"routing_key":"x","source":"crawler"}),
    ]);
    for body in &mut bodies {
        body["source"] = json!(" crawler ");
    }
    bodies
}

/// Bodies every adapter must reject with `400`.
pub fn invalid_submissions() -> Vec<Value> {
    let valid = json!({"payload":1,"routing_key":"x","source":"crawler"});
    let mut bodies = vec![json!([]), json!(null)];
    for field in ["payload", "routing_key", "source"] {
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove(field);
        bodies.push(missing);
        for value in [
            json!(null),
            json!(""),
            json!(" \t\n\u{2003}"),
            json!([]),
            json!({}),
        ] {
            let mut body = valid.clone();
            body[field] = value;
            bodies.push(body);
        }
    }
    for field in ["routing_key", "source", "message_id", "correlation_id"] {
        for value in [json!(42), json!(true)] {
            let mut body = valid.clone();
            body[field] = value;
            bodies.push(body);
        }
    }
    for field in ["message_id", "correlation_id"] {
        for value in [json!(""), json!("invalid")] {
            let mut body = valid.clone();
            body[field] = value;
            bodies.push(body);
        }
    }
    bodies
}

/// Invalid bodies whose response text is defined by Mulac, not the framework.
pub fn submission_error_messages() -> Vec<(Value, &'static str)> {
    vec![
        (
            json!({"routing_key":"x","source":"crawler"}),
            "payload is required",
        ),
        (
            json!({"payload":null,"routing_key":"x","source":"crawler"}),
            "payload must not be empty",
        ),
        (
            json!({"payload":1,"routing_key":" ","source":"crawler"}),
            "routing_key must contain non-whitespace text",
        ),
        (
            json!({"payload":1,"routing_key":"x"}),
            "source must contain non-whitespace text",
        ),
        (
            json!({"payload":1,"routing_key":"x","source":" \t\n\u{2003}"}),
            "source must contain non-whitespace text",
        ),
        (
            json!({"payload":1,"routing_key":"x","source":"crawler","message_id":"no"}),
            "message_id must be a UUID",
        ),
        (
            json!({"payload":1,"routing_key":"x","source":"crawler","correlation_id":"no"}),
            "correlation_id must be a UUID",
        ),
    ]
}

pub fn valid_run_requests() -> Vec<Value> {
    vec![
        json!({"limit":1}),
        json!({"limit":1000}),
        json!({"limit":1,"extra":true}),
    ]
}

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

/// Records the IDs of stored messages.
#[derive(Default)]
pub struct RecordingStore {
    ids: Mutex<Vec<String>>,
}

impl RecordingStore {
    pub fn stored_ids(&self) -> Vec<String> {
        self.ids.lock().unwrap().clone()
    }
}

impl InboxStorePort for RecordingStore {
    fn store(&self, msg: NewInboxMessageEnvelope) -> Result<(), InboxError> {
        self.ids.lock().unwrap().push(msg.id().to_string());
        Ok(())
    }
}

struct FailingStore;

impl InboxStorePort for FailingStore {
    fn store(&self, _: NewInboxMessageEnvelope) -> Result<(), InboxError> {
        Err(InboxError::Storage("private storage diagnostics".into()))
    }
}

struct PanickingStore;

impl InboxStorePort for PanickingStore {
    fn store(&self, _: NewInboxMessageEnvelope) -> Result<(), InboxError> {
        panic!("private storage diagnostics")
    }
}

/// Reserves nothing.
struct EmptyQueue;

impl InboxReservePort for EmptyQueue {
    fn reserve(&self, _: &ReservableInboxSpec) -> Result<Vec<InboxMessageEnvelope>, InboxError> {
        Ok(vec![])
    }
}

struct FailingQueue;

impl InboxReservePort for FailingQueue {
    fn reserve(&self, _: &ReservableInboxSpec) -> Result<Vec<InboxMessageEnvelope>, InboxError> {
        Err(InboxError::Storage("reservation unavailable".into()))
    }
}

struct PanickingQueue;

impl InboxReservePort for PanickingQueue {
    fn reserve(&self, _: &ReservableInboxSpec) -> Result<Vec<InboxMessageEnvelope>, InboxError> {
        panic!("private reservation diagnostics")
    }
}

/// Never called: the queue doubles reserve no messages.
struct UnusedProcess;

impl InboxProcessPort for UnusedProcess {
    fn completed(&self, _: Uuid, _: Uuid) -> Result<(), InboxError> {
        unreachable!("no message was reserved")
    }

    fn failed(&self, _: Uuid, _: Uuid, _: i32, _: Option<String>) -> Result<(), InboxError> {
        unreachable!("no message was reserved")
    }
}

struct UnusedCommandStore;

impl CommandStorePort for UnusedCommandStore {
    fn record(&self, _: &NewCommandEnvelope) -> Result<(), CommandError> {
        unreachable!("no message was reserved")
    }
}

fn service(store: Arc<dyn InboxStorePort>, queue: Arc<dyn InboxReservePort>) -> InboxService {
    let recorder = Arc::new(InboxRecorder::new(Arc::new(InboxRecorderRepository::new(
        store,
    ))));
    let commands = Arc::new(CommandRecorder::new(Arc::new(
        CommandRecorderRepository::new(Arc::new(UnusedCommandStore)),
    )));
    let consumer = Arc::new(InboxConsumer::new(
        InboxConsumerRepository::new(queue, Arc::new(UnusedProcess)),
        CommandGateway::two_phased(commands),
    ));
    InboxService::new(recorder, consumer)
}

/// Stores successfully and runs empty batches.
pub fn recording_service() -> (InboxService, Arc<RecordingStore>) {
    let store = Arc::new(RecordingStore::default());
    (service(store.clone(), Arc::new(EmptyQueue)), store)
}

/// Fails storage and reservation with private diagnostics.
pub fn failing_storage_service() -> InboxService {
    service(Arc::new(FailingStore), Arc::new(FailingQueue))
}

/// Panics during storage and reservation.
pub fn panicking_service() -> InboxService {
    service(Arc::new(PanickingStore), Arc::new(PanickingQueue))
}
