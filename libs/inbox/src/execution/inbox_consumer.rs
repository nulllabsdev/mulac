pub mod prelude {
    pub use super::InboxConsumer;
}

use crate::application::prelude::InboxError;
use crate::application::prelude::{InboxProcessPort, InboxReservePort};
use crate::execution::prelude::ReservableInboxSpec;
use commanding::prelude::{CommandGateway, NewCommandEnvelope};
use std::sync::Arc;
use uuid::Uuid;

pub struct InboxConsumer<R: InboxReservePort + InboxProcessPort> {
    repository: Arc<R>,
    next: CommandGateway,
}

impl<R: InboxReservePort + InboxProcessPort> InboxConsumer<R> {
    pub fn new(repository: Arc<R>, next: CommandGateway) -> Self {
        Self { repository, next }
    }

    pub fn process(&self, spec: &ReservableInboxSpec) -> Result<usize, Vec<InboxError>> {
        let messages = match self.repository.reserve(spec) {
            Ok(messages) => messages,
            Err(e) => return Err(vec![e]),
        };

        let count = messages.len();
        let mut errors: Vec<InboxError> = vec![];

        for message in messages {
            let id = message.id().to_owned();

            let Some(reservation_id) = message.reservation_id() else {
                errors.push(InboxError::MissingReservation { id });
                continue;
            };

            crate::execution::prelude::run_item(
                (id, reservation_id),
                || {
                    let envelope: NewCommandEnvelope = message.try_into()?;
                    self.next
                        .dispatch(envelope)
                        .map_err(|e| InboxError::PublishFailed(e.to_string()))
                },
                || self.completed(id, reservation_id),
                |reason| self.failed(id, reservation_id, spec.max_attempts, Some(reason)),
                InboxError::Recording,
                &mut errors,
            );
        }

        if errors.is_empty() {
            return Ok(count);
        }

        Err(errors)
    }

    pub fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), InboxError> {
        self.repository.completed(id, reservation_id)
    }

    pub fn failed(
        &self,
        id: Uuid,
        reservation_id: Uuid,
        max_attempts: i32,
        reason: Option<String>,
    ) -> Result<(), InboxError> {
        self.repository
            .failed(id, reservation_id, max_attempts, reason)
    }
}

#[cfg(test)]
mod isolation_tests {
    use super::*;
    use crate::application::prelude::InboxMessage;
    use crate::domain::prelude::InboxStatus;
    use crate::prelude::*;
    use commanding::prelude::{CommandError, CommandStorePort, NewCommandEnvelope};
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
        entries: Mutex<Vec<InboxMessageEnvelope>>,
        bad: Uuid,
        fault: Fault,
        completed: Mutex<Vec<Uuid>>,
        failed: Mutex<Vec<(Uuid, String)>>,
    }
    impl InboxReservePort for Repository {
        fn reserve(
            &self,
            _: &ReservableInboxSpec,
        ) -> Result<Vec<InboxMessageEnvelope>, InboxError> {
            Ok(std::mem::take(&mut *self.entries.lock().unwrap()))
        }
    }
    impl InboxProcessPort for Repository {
        fn completed(&self, id: Uuid, _: Uuid) -> Result<(), InboxError> {
            if id == self.bad {
                match self.fault {
                    Fault::CompletionError => {
                        return Err(InboxError::Storage("completion failed".into()));
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
        ) -> Result<(), InboxError> {
            if id == self.bad {
                match self.fault {
                    Fault::FailureError => {
                        return Err(InboxError::Storage("failure finalization failed".into()));
                    }
                    Fault::FailurePanic => panic!("private panic details"),
                    _ => {}
                }
            }
            self.failed.lock().unwrap().push((id, reason.unwrap()));
            Ok(())
        }
    }
    struct Work {
        bad: Uuid,
        fault: Fault,
    }
    impl CommandStorePort for Work {
        fn record(&self, envelope: &NewCommandEnvelope) -> Result<(), CommandError> {
            check_fault(
                self.fault,
                envelope.metadata.as_ref().unwrap().causation_id == Some(self.bad),
            )
            .map_err(CommandError::Storage)
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
    fn entry(id: Uuid, reservation_id: Uuid) -> InboxMessageEnvelope {
        InboxMessageEnvelope {
            msg: InboxMessage {
                id,
                payload: "{}".into(),
                status: InboxStatus::Reserved,
                scheduled_at: chrono::Utc::now(),
                attempts: 1,
                reservation_id: Some(reservation_id),
                reserved_at: Some(chrono::Utc::now()),
                extra_info: None,
            },
            meta: InboxMessageMetadata {
                message_id: Some(id),
                correlation_id: None,
                source: None,
                routing_key: Some("test".into()),
            },
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
            let consumer = InboxConsumer::new(repository.clone(), CommandGateway::two_phased(work));
            let result = consumer.process(&ReservableInboxSpec::new(2).with_max_attempts(1));
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
