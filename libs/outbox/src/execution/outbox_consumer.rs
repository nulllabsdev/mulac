//! Outbox consumer use case.
//!
//! This module coordinates reservation of persisted outbox entries, publication
//! through a transport adapter, and lifecycle updates on the backing store.

pub mod prelude {
    pub use super::OutboxConsumer;
}

use crate::application::prelude::{
    OutboundMessageEnvelope,
    OutboxError,
    OutboxPublisherPort,
    //
};
use crate::application::prelude::{OutboxProcessPort, OutboxReservePort};
use crate::execution::prelude::ReservableOutboxSpec;
use std::sync::Arc;

pub struct OutboxConsumer<R: OutboxReservePort + OutboxProcessPort> {
    repository: Arc<R>,
    publisher: Arc<dyn OutboxPublisherPort>,
}

impl<R: OutboxReservePort + OutboxProcessPort> OutboxConsumer<R> {
    pub fn new(repository: Arc<R>, publisher: Arc<dyn OutboxPublisherPort>) -> Self {
        Self {
            repository,
            publisher,
        }
    }

    /// Publish all currently reservable entries, collecting per-entry errors.
    ///
    /// Blocks on storage and the publisher. Do not call it from async code:
    /// publishers such as an AMQP bridge may block on the application runtime.
    /// Use `AsyncOutboxService` (feature `api-tokio`) from async handlers.
    pub fn publish_batch(&self, spec: &ReservableOutboxSpec) -> Result<usize, Vec<OutboxError>> {
        let entries = match self.repository.reserve(spec) {
            Ok(entries) => entries,
            Err(e) => return Err(vec![e]),
        };

        let count = entries.len();
        let mut errors: Vec<OutboxError> = vec![];

        for entry in entries {
            let id = entry.message.id;
            let Some(reservation_id) = entry.message.reservation_id else {
                errors.push(OutboxError::MissingReservation { id });
                continue;
            };

            crate::execution::prelude::run_item(
                (id, reservation_id),
                || {
                    let outbound = OutboundMessageEnvelope::try_from(&entry)?;
                    self.publisher.publish(outbound)
                },
                || self.repository.completed(id, reservation_id),
                |reason| {
                    self.repository
                        .failed(id, reservation_id, spec.max_attempts, Some(reason))
                },
                OutboxError::Publish,
                &mut errors,
            );
        }

        finish_batch(count, errors)
    }
}

fn finish_batch(count: usize, errors: Vec<OutboxError>) -> Result<usize, Vec<OutboxError>> {
    if errors.is_empty() {
        Ok(count)
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use crate::application::prelude::{
        OutboundMessageEnvelope,
        OutboxEntry,
        OutboxEntryEnvelope,
        OutboxEntryMetadata,
        OutboxError,
        OutboxProcessPort,
        OutboxPublisherPort,
        OutboxReservePort,
        OutboxStatus,
        //
    };
    use crate::execution::prelude::{
        OutboxConsumer,
        ReservableOutboxSpec,
        //
    };
    use chrono::Utc;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    struct TestRepository {
        reserve: Arc<dyn OutboxReservePort>,
        process: Arc<dyn OutboxProcessPort>,
    }
    impl OutboxReservePort for TestRepository {
        fn reserve(
            &self,
            s: &ReservableOutboxSpec,
        ) -> Result<Vec<OutboxEntryEnvelope>, OutboxError> {
            self.reserve.reserve(s)
        }
    }
    impl OutboxProcessPort for TestRepository {
        fn completed(&self, id: Uuid, r: Uuid) -> Result<(), OutboxError> {
            self.process.completed(id, r)
        }
        fn failed(&self, id: Uuid, r: Uuid, m: i32, e: Option<String>) -> Result<(), OutboxError> {
            self.process.failed(id, r, m, e)
        }
        fn dead(&self, id: Uuid, r: Uuid, e: Option<String>) -> Result<(), OutboxError> {
            self.process.dead(id, r, e)
        }
    }
    #[derive(Default)]
    struct FakeReserve {
        entries: Mutex<Vec<OutboxEntryEnvelope>>,
    }

    impl OutboxReservePort for FakeReserve {
        fn reserve(
            &self,
            _spec: &ReservableOutboxSpec,
        ) -> Result<Vec<OutboxEntryEnvelope>, OutboxError> {
            Ok(self.entries.lock().unwrap().clone())
        }
    }

    #[derive(Default)]
    struct FakeProcess {
        completed: Mutex<Vec<(Uuid, Uuid)>>,
        failed: Mutex<Vec<(Uuid, Uuid, i32, Option<String>)>>,
        dead: Mutex<Vec<(Uuid, Uuid, Option<String>)>>,
    }

    impl OutboxProcessPort for FakeProcess {
        fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), OutboxError> {
            self.completed.lock().unwrap().push((id, reservation_id));
            Ok(())
        }

        fn failed(
            &self,
            id: Uuid,
            reservation_id: Uuid,
            max_attempts: i32,
            reason: Option<String>,
        ) -> Result<(), OutboxError> {
            self.failed
                .lock()
                .unwrap()
                .push((id, reservation_id, max_attempts, reason));
            Ok(())
        }

        fn dead(
            &self,
            id: Uuid,
            reservation_id: Uuid,
            reason: Option<String>,
        ) -> Result<(), OutboxError> {
            self.dead.lock().unwrap().push((id, reservation_id, reason));
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakePublisher {
        published: Mutex<Vec<OutboundMessageEnvelope>>,
        persistent_error: Mutex<Option<String>>,
        transient_error: Mutex<Option<String>>,
        transient_failures: Mutex<usize>,
    }

    impl OutboxPublisherPort for FakePublisher {
        fn publish(&self, envelope: OutboundMessageEnvelope) -> Result<(), OutboxError> {
            let mut transient_failures = self.transient_failures.lock().unwrap();
            if *transient_failures > 0 {
                *transient_failures -= 1;
                let message = self
                    .transient_error
                    .lock()
                    .unwrap()
                    .clone()
                    .or_else(|| self.persistent_error.lock().unwrap().clone())
                    .unwrap_or_else(|| "transient publish failure".into());
                return Err(OutboxError::Transport(message));
            }

            if let Some(message) = self.persistent_error.lock().unwrap().clone() {
                return Err(OutboxError::Transport(message));
            }
            self.published.lock().unwrap().push(envelope);
            Ok(())
        }
    }

    fn metadata(event_id: Uuid) -> OutboxEntryMetadata {
        OutboxEntryMetadata {
            sequence: None,
            event_id,
            message_id: event_id,
            correlation_id: None,
            causation_id: None,
            event_type: "UserRegistered".into(),
            routing_key: "users.registered".into(),
            source: None,
            content_type: Some("application/json".into()),
        }
    }

    fn entry(id: Uuid, reservation_id: Option<Uuid>) -> OutboxEntryEnvelope {
        let now = Utc::now();
        let meta = metadata(id);
        OutboxEntryEnvelope {
            message: OutboxEntry {
                id,
                status: OutboxStatus::Reserved,
                payload: "{}".into(),
                meta: meta.clone(),
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
            metadata: meta,
        }
    }

    fn consumer(
        reserve: Arc<FakeReserve>,
        process: Arc<FakeProcess>,
        publisher: Arc<FakePublisher>,
    ) -> OutboxConsumer<TestRepository> {
        let repo = Arc::new(TestRepository { reserve, process });
        OutboxConsumer::new(repo, publisher)
    }

    #[test]
    fn publish_batch_publishes_and_completes_reserved_entry() {
        let entry_id = Uuid::now_v7();
        let reservation_id = Uuid::now_v7();
        let reserve = Arc::new(FakeReserve {
            entries: Mutex::new(vec![entry(entry_id, Some(reservation_id))]),
        });
        let process = Arc::new(FakeProcess::default());
        let publisher = Arc::new(FakePublisher::default());
        let consumer = consumer(reserve, process.clone(), publisher.clone());

        consumer
            .publish_batch(&ReservableOutboxSpec::new(10))
            .expect("batch succeeds");

        assert_eq!(publisher.published.lock().unwrap().len(), 1);
        assert_eq!(
            process.completed.lock().unwrap().as_slice(),
            &[(entry_id, reservation_id)]
        );
    }

    #[test]
    fn publish_batch_marks_entry_failed_on_publish_failure() {
        let entry_id = Uuid::now_v7();
        let reservation_id = Uuid::now_v7();
        let reserve = Arc::new(FakeReserve {
            entries: Mutex::new(vec![entry(entry_id, Some(reservation_id))]),
        });
        let process = Arc::new(FakeProcess::default());
        let publisher = Arc::new(FakePublisher {
            published: Mutex::new(vec![]),
            persistent_error: Mutex::new(Some("broker unavailable".into())),
            transient_error: Mutex::new(None),
            transient_failures: Mutex::new(0),
        });
        let consumer = consumer(reserve, process.clone(), publisher);

        let errors = consumer
            .publish_batch(&ReservableOutboxSpec::new(10).with_max_attempts(3))
            .expect_err("batch reports publish error");

        assert_eq!(errors.len(), 1);
        assert_eq!(process.completed.lock().unwrap().len(), 0);
        assert_eq!(
            process.failed.lock().unwrap().as_slice(),
            &[(
                entry_id,
                reservation_id,
                3,
                Some(format!(
                    "item {entry_id}, reservation {reservation_id}, stage processing: transport error: broker unavailable"
                ))
            )]
        );
    }

    #[test]
    fn publish_batch_continues_after_entry_failure() {
        let failed_entry_id = Uuid::now_v7();
        let failed_reservation_id = Uuid::now_v7();
        let completed_entry_id = Uuid::now_v7();
        let completed_reservation_id = Uuid::now_v7();
        let reserve = Arc::new(FakeReserve {
            entries: Mutex::new(vec![
                entry(failed_entry_id, Some(failed_reservation_id)),
                entry(completed_entry_id, Some(completed_reservation_id)),
            ]),
        });
        let process = Arc::new(FakeProcess::default());
        let publisher = Arc::new(FakePublisher {
            published: Mutex::new(vec![]),
            persistent_error: Mutex::new(None),
            transient_error: Mutex::new(Some("broker unavailable".into())),
            transient_failures: Mutex::new(1),
        });
        let consumer = consumer(reserve, process.clone(), publisher.clone());

        let errors = consumer
            .publish_batch(&ReservableOutboxSpec::new(10).with_max_attempts(4))
            .expect_err("batch reports the failed publish");

        assert_eq!(errors.len(), 1);
        assert_eq!(publisher.published.lock().unwrap().len(), 1);
        assert_eq!(
            process.completed.lock().unwrap().as_slice(),
            &[(completed_entry_id, completed_reservation_id)]
        );
        assert_eq!(
            process.failed.lock().unwrap().as_slice(),
            &[(
                failed_entry_id,
                failed_reservation_id,
                4,
                Some(format!(
                    "item {failed_entry_id}, reservation {failed_reservation_id}, stage processing: transport error: broker unavailable"
                ))
            )]
        );
        assert!(process.dead.lock().unwrap().is_empty());
    }

    #[test]
    fn publish_batch_marks_entry_failed_on_conversion_failure() {
        let entry_id = Uuid::now_v7();
        let reservation_id = Uuid::now_v7();
        let reserve = Arc::new(FakeReserve {
            entries: Mutex::new(vec![entry(entry_id, Some(reservation_id))]),
        });
        {
            let mut entries = reserve.entries.lock().unwrap();
            entries[0].metadata.routing_key = "   ".into();
            entries[0].message.meta.routing_key = "   ".into();
        }
        let process = Arc::new(FakeProcess::default());
        let publisher = Arc::new(FakePublisher::default());
        let consumer = consumer(reserve, process.clone(), publisher.clone());

        let errors = consumer
            .publish_batch(&ReservableOutboxSpec::new(10))
            .expect_err("batch reports conversion error");

        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            OutboxError::Conversion(message) if message == "missing routing_key"
        ));
        assert!(publisher.published.lock().unwrap().is_empty());
        assert!(process.completed.lock().unwrap().is_empty());
        assert!(process.dead.lock().unwrap().is_empty());
        assert_eq!(
            process.failed.lock().unwrap().as_slice(),
            &[(
                entry_id,
                reservation_id,
                6,
                Some(format!(
                    "item {entry_id}, reservation {reservation_id}, stage processing: conversion error: missing routing_key"
                ))
            )],
        );
    }

    #[test]
    fn publish_batch_reports_missing_reservation_without_side_effects() {
        let entry_id = Uuid::now_v7();
        let reserve = Arc::new(FakeReserve {
            entries: Mutex::new(vec![entry(entry_id, None)]),
        });
        let process = Arc::new(FakeProcess::default());
        let publisher = Arc::new(FakePublisher::default());
        let consumer = consumer(reserve, process.clone(), publisher.clone());

        let errors = consumer
            .publish_batch(&ReservableOutboxSpec::new(10))
            .expect_err("batch reports missing reservation");

        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            OutboxError::MissingReservation { id } if *id == entry_id
        ));
        assert!(publisher.published.lock().unwrap().is_empty());
        assert!(process.completed.lock().unwrap().is_empty());
        assert!(process.failed.lock().unwrap().is_empty());
        assert!(process.dead.lock().unwrap().is_empty());
    }
}

#[cfg(test)]
mod isolation_tests {
    use super::*;
    use crate::prelude::*;
    use std::sync::Mutex;
    use uuid::Uuid;

    #[derive(Clone, Copy, PartialEq, Debug)]
    enum Fault {
        None,
        ProcessingError,
        ProcessingPanic,
        CompletionError,
        CompletionPanic,
        FailureError,
        FailurePanic,
    }
    struct Repository {
        entries: Mutex<Vec<OutboxEntryEnvelope>>,
        bad: Uuid,
        fault: Fault,
        completed: Mutex<Vec<Uuid>>,
        failed: Mutex<Vec<(Uuid, String)>>,
    }
    impl OutboxReservePort for Repository {
        fn reserve(
            &self,
            _: &ReservableOutboxSpec,
        ) -> Result<Vec<OutboxEntryEnvelope>, OutboxError> {
            Ok(std::mem::take(&mut *self.entries.lock().unwrap()))
        }
    }
    impl OutboxProcessPort for Repository {
        fn completed(&self, id: Uuid, _: Uuid) -> Result<(), OutboxError> {
            if id == self.bad {
                match self.fault {
                    Fault::CompletionError => {
                        return Err(OutboxError::Storage("completion failed".into()));
                    }
                    Fault::CompletionPanic => panic!("private panic details"),
                    _ => {}
                }
            }
            self.completed.lock().unwrap().push(id);
            Ok(())
        }
        fn failed(
            &self,
            id: Uuid,
            _: Uuid,
            _: i32,
            reason: Option<String>,
        ) -> Result<(), OutboxError> {
            if id == self.bad {
                match self.fault {
                    Fault::FailureError => {
                        return Err(OutboxError::Storage("failure finalization failed".into()));
                    }
                    Fault::FailurePanic => panic!("private panic details"),
                    _ => {}
                }
            }
            self.failed.lock().unwrap().push((id, reason.unwrap()));
            Ok(())
        }
        fn dead(&self, _: Uuid, _: Uuid, _: Option<String>) -> Result<(), OutboxError> {
            panic!("automatic processing must never mark an item dead")
        }
    }
    struct Work {
        bad: Uuid,
        fault: Fault,
    }
    impl OutboxPublisherPort for Work {
        fn publish(&self, envelope: OutboundMessageEnvelope) -> Result<(), OutboxError> {
            check_fault(self.fault, envelope.metadata.event_id == self.bad)
                .map_err(OutboxError::Publish)
        }
    }
    fn check_fault(fault: Fault, bad: bool) -> Result<(), String> {
        if bad {
            match fault {
                Fault::ProcessingPanic => panic!("private panic details"),
                Fault::ProcessingError | Fault::FailureError | Fault::FailurePanic => {
                    return Err("processing failed".into());
                }
                _ => {}
            }
        }
        Ok(())
    }
    fn entry(id: Uuid, reservation_id: Uuid) -> OutboxEntryEnvelope {
        let meta = OutboxEntryMetadata {
            sequence: None,
            event_id: id,
            message_id: id,
            correlation_id: None,
            causation_id: None,
            event_type: "test".into(),
            routing_key: "test".into(),
            source: None,
            content_type: None,
        };
        let now = chrono::Utc::now();
        OutboxEntryEnvelope {
            message: OutboxEntry {
                id,
                status: OutboxStatus::Reserved,
                payload: "{}".into(),
                meta: meta.clone(),
                scheduled_at: now,
                attempts: 1,
                reservation_id: Some(reservation_id),
                reserved_at: Some(now),
                received_at: now,
                updated_at: now,
                processed_at: None,
                last_error: None,
                extra_info: None,
            },
            metadata: meta,
        }
    }
    #[test]
    fn each_item_recovers_independently_including_finalization_panics() {
        for fault in [
            Fault::None,
            Fault::ProcessingError,
            Fault::ProcessingPanic,
            Fault::CompletionError,
            Fault::CompletionPanic,
            Fault::FailureError,
            Fault::FailurePanic,
        ] {
            let bad = Uuid::now_v7();
            let good = Uuid::now_v7();
            let reservation_id = Uuid::now_v7();
            let repository = Arc::new(Repository {
                entries: Mutex::new(vec![
                    entry(bad, reservation_id),
                    entry(good, reservation_id),
                ]),
                bad,
                fault,
                completed: Mutex::new(vec![]),
                failed: Mutex::new(vec![]),
            });
            let work = Arc::new(Work { bad, fault });
            let consumer = OutboxConsumer::new(repository.clone(), work);
            let result = consumer.publish_batch(&ReservableOutboxSpec::new(2).with_max_attempts(1));
            let completed = repository.completed.lock().unwrap();
            let failed = repository.failed.lock().unwrap();
            assert!(completed.contains(&good), "{fault:?}");
            if fault == Fault::None {
                assert_eq!(result.unwrap(), 2);
                assert!(completed.contains(&bad));
                assert!(failed.is_empty());
            } else {
                let errors = result.unwrap_err();
                assert!(
                    !errors
                        .iter()
                        .any(|e| e.to_string().contains("private panic details"))
                );
                assert!(!completed.contains(&bad));
                if matches!(fault, Fault::FailureError | Fault::FailurePanic) {
                    assert_eq!(errors.len(), 2);
                    assert!(failed.is_empty());
                } else {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(failed.len(), 1);
                    assert_eq!(failed[0].0, bad);
                    assert!(failed[0].1.contains(&bad.to_string()));
                    assert!(failed[0].1.contains(&reservation_id.to_string()));
                    assert!(failed[0].1.contains("stage "));
                }
            }
        }
    }
}
