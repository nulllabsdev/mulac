pub mod prelude {
    pub use super::EventingService;
}

use crate::contract::prelude::{
    RunEventsRequest,
    RunEventsResponse,
    //
};
use crate::error::prelude::EventingApiError;
use eventing::prelude::{
    EventConsumer,
    EventProcessPort,
    EventReservePort,
    ReservableEventSpec,
    //
};
use std::sync::Arc;

/// Synchronous dispatch of one batch of due events.
///
/// Running blocks on storage and subscribers; use `AsyncEventingService`
/// (feature `tokio`) from async handlers.
pub struct EventingService {
    run: Arc<dyn Fn(RunEventsRequest) -> RunEventsResponse + Send + Sync>,
}

impl EventingService {
    /// Wrap a consumer whose dispatcher the application or kernel assembled,
    /// for example `PersistentKernelHandle::event_consumer()`.
    pub fn new<R: EventReservePort + EventProcessPort + 'static>(
        consumer: Arc<EventConsumer<R>>,
    ) -> Self {
        Self {
            run: Arc::new(move |request| {
                RunEventsResponse::from_result(
                    consumer.consume(&ReservableEventSpec::new(request.limit)),
                )
            }),
        }
    }

    /// Dispatch up to `limit` due events with the default retry limit.
    ///
    /// Event failures are reported in the response, not as an error; events
    /// that were dispatched successfully stay completed.
    pub fn run(&self, request: RunEventsRequest) -> Result<RunEventsResponse, EventingApiError> {
        request.validate()?;
        Ok((self.run)(request))
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use crate::prelude::{EventingApiError, RunEventsRequest, testing};

    fn run(
        service: crate::prelude::EventingService,
        limit: usize,
    ) -> crate::prelude::RunEventsResponse {
        service.run(RunEventsRequest { limit }).unwrap()
    }

    #[test]
    fn empty_batches_process_nothing() {
        let response = run(testing::empty_service(), 5);
        assert_eq!(response.processed, Some(0));
        assert!(response.errors.is_empty());
    }

    #[test]
    fn batches_respect_the_limit() {
        let service = testing::queued_service(3);
        for processed in [2, 1, 0] {
            let response = service.run(RunEventsRequest { limit: 2 }).unwrap();
            assert_eq!(response.processed, Some(processed));
        }
    }

    #[test]
    fn dispatches_each_event_to_the_subscriber() {
        let (service, handled) = testing::recording_service(&["a", "b", "c"]);
        assert_eq!(run(service, 2).processed, Some(2));
        assert_eq!(*handled.lock().unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn consumer_failures_are_reported_in_the_response() {
        for (service, errors) in [
            (
                testing::failing_reservation_service(),
                vec![testing::RESERVATION_FAILURE],
            ),
            (
                testing::failing_subscriber_service(),
                vec![testing::SUBSCRIBER_FAILURE],
            ),
            // Marking the event failed is reported before the subscriber error.
            (
                testing::failing_subscriber_and_process_service(),
                vec![testing::PROCESS_FAILURE, testing::SUBSCRIBER_FAILURE],
            ),
        ] {
            let response = run(service, 5);
            assert_eq!(response.processed, None);
            assert_eq!(response.errors, errors);
        }
    }

    #[test]
    fn successful_events_of_a_failed_batch_stay_completed() {
        let (service, completed) = testing::partially_failing_service();
        let response = run(service, 5);
        assert_eq!(response.processed, None);
        assert_eq!(response.errors, vec![testing::SUBSCRIBER_FAILURE]);
        assert_eq!(*completed.lock().unwrap(), 2);
    }

    #[test]
    fn direct_callers_are_validated() {
        let error = testing::panicking_service()
            .run(RunEventsRequest { limit: 0 })
            .unwrap_err();
        assert!(matches!(error, EventingApiError::LimitOutOfRange { .. }));
    }
}
