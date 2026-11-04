pub mod prelude {
    pub use super::EventConsumer;
}

use crate::application::prelude::{EventEnvelope, EventError};
use crate::application::prelude::{EventProcessPort, EventReservePort};
use crate::execution::prelude::{EventDispatcher, ReservableEventSpec};
use std::sync::Arc;

pub struct EventConsumer<R: EventReservePort + EventProcessPort> {
    repository: Arc<R>,
    dispatcher: Arc<EventDispatcher>,
}

impl<R: EventReservePort + EventProcessPort> EventConsumer<R> {
    pub fn new(repository: Arc<R>, dispatcher: Arc<EventDispatcher>) -> Self {
        Self {
            repository,
            dispatcher,
        }
    }

    pub fn consume(&self, spec: &ReservableEventSpec) -> Result<usize, Vec<EventError>> {
        let entries = match self.repository.reserve(spec) {
            Ok(entries) => entries,
            Err(e) => return Err(vec![e]),
        };

        let count = entries.len();
        let mut errors: Vec<EventError> = vec![];

        for entry in entries {
            self.process_entry(&entry, spec, &mut errors);
        }

        if errors.is_empty() {
            Ok(count)
        } else {
            Err(errors)
        }
    }

    fn process_entry(
        &self,
        entry: &EventEnvelope,
        spec: &ReservableEventSpec,
        errors: &mut Vec<EventError>,
    ) {
        let id = entry.id;
        let reservation_id = entry.reservation_id;
        crate::execution::prelude::run_item(
            (id, reservation_id),
            || {
                let envelope = entry.into();
                self.dispatcher.dispatch(&envelope)
            },
            || self.repository.completed(id, reservation_id),
            |reason| {
                self.repository
                    .failed(id, reservation_id, spec.max_attempts, Some(reason))
            },
            EventError::Storage,
            errors,
        );
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
        entries: Mutex<Vec<EventEnvelope>>,
        bad: Uuid,
        fault: Fault,
        completed: Mutex<Vec<Uuid>>,
        failed: Mutex<Vec<(Uuid, String)>>,
    }
    impl EventReservePort for Repository {
        fn reserve(&self, _: &ReservableEventSpec) -> Result<Vec<EventEnvelope>, EventError> {
            Ok(std::mem::take(&mut *self.entries.lock().unwrap()))
        }
    }
    impl EventProcessPort for Repository {
        fn completed(&self, id: Uuid, _: Uuid) -> Result<(), EventError> {
            if id == self.bad {
                match self.fault {
                    Fault::CompletionError => {
                        return Err(EventError::Storage("completion failed".into()));
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
        ) -> Result<(), EventError> {
            if id == self.bad {
                match self.fault {
                    Fault::FailureError => {
                        return Err(EventError::Storage("failure finalization failed".into()));
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
    impl EventSubscriberPort for Work {
        fn handle(&self, envelope: &NewEventEnvelope) -> Result<(), EventError> {
            check_fault(
                self.fault,
                envelope.metadata.as_ref().unwrap().event_id == self.bad,
            )
            .map_err(EventError::SubscriberExecution)
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
    fn entry(id: Uuid, reservation_id: Uuid) -> EventEnvelope {
        EventEnvelope {
            id,
            reservation_id,
            event_type: "test".into(),
            payload: "{}".into(),
            attempts: 1,
            extra_info: None,
            metadata: Some(EventMetadata {
                sequence: None,
                event_id: id,
                correlation_id: None,
                causation_id: None,
                source: None,
            }),
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
            let consumer =
                EventConsumer::new(repository.clone(), Arc::new(EventDispatcher::new(work)));
            let result = consumer.consume(&ReservableEventSpec::new(2).with_max_attempts(1));
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
