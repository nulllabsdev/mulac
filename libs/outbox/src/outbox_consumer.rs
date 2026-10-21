//! Outbox consumer use case.
//!
//! This module coordinates reservation of persisted outbox entries, publication
//! through a transport adapter, and lifecycle updates on the backing store.

pub mod io {
    pub use super::consumer::OutboxConsumer;
    pub use super::repository::OutboxConsumerRepository;
    pub use super::reservable::ReservableOutboxSpec;
    #[cfg(feature = "diesel")]
    use crate::assembly::infra_diesel::io::{DbPool, OutboxConsumerStorage};
    #[cfg(feature = "diesel")]
    use std::sync::Arc;
    #[cfg(feature = "diesel")]
    pub fn repository(pool: DbPool) -> OutboxConsumerRepository {
        let storage = Arc::new(OutboxConsumerStorage::new(pool));
        OutboxConsumerRepository::new(storage.clone(), storage)
    }
}

mod reservable {
    /// Parameters for selecting outbox entries eligible for publication.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ReservableOutboxSpec {
        /// Maximum number of entries to reserve in a single call.
        pub limit: usize,
        /// Entries with `attempts >= max_attempts` are excluded from reservation.
        pub max_attempts: i32,
    }

    impl ReservableOutboxSpec {
        /// Default number of publication attempts before an entry is no longer reservable.
        pub const DEFAULT_MAX_ATTEMPTS: i32 = 6;

        /// Create a spec with the given limit and [`DEFAULT_MAX_ATTEMPTS`].
        ///
        /// [`DEFAULT_MAX_ATTEMPTS`]: Self::DEFAULT_MAX_ATTEMPTS
        pub fn new(limit: usize) -> Self {
            Self {
                limit,
                max_attempts: Self::DEFAULT_MAX_ATTEMPTS,
            }
        }

        /// Override the maximum number of attempts.
        pub fn with_max_attempts(mut self, max_attempts: i32) -> Self {
            self.max_attempts = max_attempts;
            self
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn new_uses_default_max_attempts() {
            let spec = ReservableOutboxSpec::new(10);
            assert_eq!(spec.limit, 10);
            assert_eq!(
                spec.max_attempts,
                ReservableOutboxSpec::DEFAULT_MAX_ATTEMPTS
            );
        }

        #[test]
        fn with_max_attempts_overrides_default() {
            let spec = ReservableOutboxSpec::new(5).with_max_attempts(3);
            assert_eq!(spec.limit, 5);
            assert_eq!(spec.max_attempts, 3);
        }
    }
}

mod conversions {
    use crate::assembly::io::{
        OutboundMessageEnvelope,
        OutboxEntryEnvelope,
        OutboxError,
        //
    };

    impl TryFrom<&OutboxEntryEnvelope> for OutboundMessageEnvelope {
        type Error = OutboxError;

        fn try_from(entry: &OutboxEntryEnvelope) -> Result<Self, Self::Error> {
            if entry.metadata.routing_key.trim().is_empty() {
                return Err(OutboxError::Conversion("missing routing_key".into()));
            }

            Ok(Self {
                payload: entry.message.payload.clone().into_bytes(),
                metadata: entry.metadata.clone(),
            })
        }
    }
}

mod repository {
    use super::reservable::ReservableOutboxSpec;
    use crate::assembly::io::{
        OutboxEntryEnvelope,
        OutboxError,
        OutboxProcessPort,
        OutboxReservePort,
        //
    };
    use std::sync::Arc;
    use uuid::Uuid;

    #[derive(Clone)]
    pub struct OutboxConsumerRepository {
        reserve: Arc<dyn OutboxReservePort>,
        process: Arc<dyn OutboxProcessPort>,
    }

    impl OutboxConsumerRepository {
        pub fn new(
            reserve: Arc<dyn OutboxReservePort>,
            process: Arc<dyn OutboxProcessPort>,
        ) -> Self {
            Self { reserve, process }
        }

        pub fn reserve(
            &self,
            spec: &ReservableOutboxSpec,
        ) -> Result<Vec<OutboxEntryEnvelope>, OutboxError> {
            self.reserve.reserve(spec)
        }

        pub fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), OutboxError> {
            self.process.completed(id, reservation_id)
        }

        pub fn failed(
            &self,
            id: Uuid,
            reservation_id: Uuid,
            max_attempts: i32,
            reason: Option<String>,
        ) -> Result<(), OutboxError> {
            self.process
                .failed(id, reservation_id, max_attempts, reason)
        }

        pub fn dead(
            &self,
            id: Uuid,
            reservation_id: Uuid,
            reason: Option<String>,
        ) -> Result<(), OutboxError> {
            self.process.dead(id, reservation_id, reason)
        }
    }
}

mod consumer {
    use super::repository::OutboxConsumerRepository;
    use super::reservable::ReservableOutboxSpec;
    use crate::assembly::io::{
        OutboundMessageEnvelope,
        OutboxError,
        OutboxPublisherPort,
        //
    };
    use std::sync::Arc;

    pub struct OutboxConsumer {
        repository: OutboxConsumerRepository,
        publisher: Arc<dyn OutboxPublisherPort>,
    }

    impl OutboxConsumer {
        pub fn new(
            repository: OutboxConsumerRepository,
            publisher: Arc<dyn OutboxPublisherPort>,
        ) -> Self {
            Self {
                repository,
                publisher,
            }
        }

        /// Publish all currently reservable entries, collecting per-entry errors.
        pub fn publish_batch(
            &self,
            spec: &ReservableOutboxSpec,
        ) -> Result<usize, Vec<OutboxError>> {
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

                let outbound = match OutboundMessageEnvelope::try_from(&entry) {
                    Ok(outbound) => outbound,
                    Err(e) => {
                        let reason = e.to_string();
                        self.repository
                            .dead(id, reservation_id, Some(reason))
                            .unwrap_or_else(|err| errors.push(err));
                        errors.push(e);
                        continue;
                    }
                };

                match self.publisher.publish(outbound) {
                    Ok(()) => self
                        .repository
                        .completed(id, reservation_id)
                        .unwrap_or_else(|e| errors.push(e)),
                    Err(e) => {
                        let reason = e.to_string();
                        self.repository
                            .failed(id, reservation_id, spec.max_attempts, Some(reason))
                            .unwrap_or_else(|err| errors.push(err));
                        errors.push(e);
                    }
                }
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
}

#[cfg(test)]
mod tests {
    use super::io::{
        OutboxConsumer,
        OutboxConsumerRepository,
        ReservableOutboxSpec,
        //
    };
    use crate::assembly::io::{
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
    use chrono::Utc;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

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
    ) -> OutboxConsumer {
        let repo = OutboxConsumerRepository::new(reserve, process);
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
                Some("transport error: broker unavailable".into())
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
                Some("transport error: broker unavailable".into())
            )]
        );
        assert!(process.dead.lock().unwrap().is_empty());
    }

    #[test]
    fn publish_batch_marks_entry_dead_on_conversion_failure() {
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
        assert!(process.failed.lock().unwrap().is_empty());
        assert_eq!(
            process.dead.lock().unwrap().as_slice(),
            &[(
                entry_id,
                reservation_id,
                Some("conversion error: missing routing_key".into())
            )]
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
