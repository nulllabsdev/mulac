pub mod io {
    pub use super::OutboxService;
}

use crate::contract::io::{
    RunOutboxRequest,
    RunOutboxResponse,
    //
};
use crate::error::io::OutboxApiError;
use outbox::io::{
    OutboxConsumer,
    ReservableOutboxSpec,
    //
};
use std::sync::Arc;

/// Synchronous publication of one batch of due outbox entries.
///
/// Running blocks on storage and the publisher. Do not call it from async
/// code: publishers such as an AMQP bridge may block on the application
/// runtime. Use `AsyncOutboxService` (feature `tokio`) from async handlers.
pub struct OutboxService {
    consumer: Arc<OutboxConsumer>,
}

impl OutboxService {
    /// Wrap a consumer whose storage and publisher the application assembled.
    pub fn new(consumer: Arc<OutboxConsumer>) -> Self {
        Self { consumer }
    }

    /// Publish from PostgreSQL through an application-owned publisher.
    #[cfg(feature = "diesel")]
    pub fn from_postgres(
        pool: outbox::io::DbPool,
        publisher: Arc<dyn outbox::io::OutboxPublisherPort>,
    ) -> Self {
        let repository = outbox::io::consumer_repository(pool);
        Self::new(Arc::new(OutboxConsumer::new(repository, publisher)))
    }

    /// Publish up to `limit` due entries with the default retry limit.
    ///
    /// Publication failures are reported in the response, not as an error;
    /// entries that were published stay completed.
    pub fn run(&self, request: RunOutboxRequest) -> Result<RunOutboxResponse, OutboxApiError> {
        request.validate()?;
        let result = self
            .consumer
            .publish_batch(&ReservableOutboxSpec::new(request.limit));
        Ok(RunOutboxResponse::from_result(result))
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use crate::io::{OutboxApiError, RunOutboxRequest, testing};

    fn run(service: crate::io::OutboxService, limit: usize) -> crate::io::RunOutboxResponse {
        service.run(RunOutboxRequest { limit }).unwrap()
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
            let response = service.run(RunOutboxRequest { limit: 2 }).unwrap();
            assert_eq!(response.processed, Some(processed));
        }
    }

    #[test]
    fn publishes_each_entry() {
        let (service, published) = testing::recording_service(&["a", "b", "c"]);
        assert_eq!(run(service, 2).processed, Some(2));
        assert_eq!(*published.lock().unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn consumer_failures_are_reported_in_the_response() {
        for (service, errors) in [
            (
                testing::failing_reservation_service(),
                vec![testing::RESERVATION_FAILURE],
            ),
            (
                testing::failing_publisher_service(),
                vec![testing::PUBLISH_FAILURE],
            ),
            // Marking the entry failed is reported before the publish error.
            (
                testing::failing_publisher_and_process_service(),
                vec![testing::PROCESS_FAILURE, testing::PUBLISH_FAILURE],
            ),
        ] {
            let response = run(service, 5);
            assert_eq!(response.processed, None);
            assert_eq!(response.errors, errors);
        }
    }

    #[test]
    fn published_entries_of_a_failed_batch_stay_completed() {
        let (service, outcomes) = testing::partially_failing_service();
        let response = run(service, 5);
        assert_eq!(response.processed, None);
        assert_eq!(response.errors, vec![testing::PUBLISH_FAILURE]);
        assert_eq!(
            *outcomes.lock().unwrap(),
            vec!["completed", "failed", "completed"]
        );
    }

    #[test]
    fn unroutable_entries_become_dead() {
        let (service, outcomes) = testing::unroutable_service();
        let response = run(service, 5);
        assert_eq!(response.processed, None);
        assert_eq!(response.errors, vec![testing::CONVERSION_FAILURE]);
        assert_eq!(*outcomes.lock().unwrap(), vec!["dead"]);
    }

    #[test]
    fn entries_without_reservation_are_reported() {
        let response = run(testing::missing_reservation_service(), 5);
        assert_eq!(response.processed, None);
        assert_eq!(response.errors.len(), 1);
        assert!(
            response.errors[0].starts_with("missing reservation for entry "),
            "{:?}",
            response.errors
        );
    }

    #[test]
    fn direct_callers_are_validated() {
        let error = testing::panicking_service()
            .run(RunOutboxRequest { limit: 0 })
            .unwrap_err();
        assert!(matches!(error, OutboxApiError::LimitOutOfRange { .. }));
    }
}
