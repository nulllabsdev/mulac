//! Shared HTTP execution bridge; not a second application layer.

pub mod prelude {
    pub use super::AsyncOutboxService;
}

use crate::application::prelude::{OutboxProcessPort, OutboxReservePort};
use crate::execution::prelude::{OutboxConsumer, ReservableOutboxSpec};
use crate::http::contract::prelude::{
    RunOutboxRequest,
    RunOutboxResponse,
    //
};
use crate::http::error::prelude::{
    OutboxApiError,
    err_publisher_not_configured,
    err_run_task_failed,
    //
};
use std::sync::Arc;

/// Bridges the HTTP contract to an [`OutboxConsumer`] on Tokio's blocking pool,
/// or reports that no publisher is configured.
///
/// Response formatting belongs here; [`RunOutboxRequest`] is valid by
/// construction, and publication and lifecycle rules remain in [`OutboxConsumer`].
/// Callers without a web framework or Tokio runtime call the consumer directly.
///
/// Uses the caller's runtime; it creates no runtime, pool, worker or
/// connection. Publishers run on a blocking-pool thread of that runtime, so
/// they may use `Handle::current().block_on`. Repository types are erased here,
/// at the background-task boundary, so HTTP adapters do not need repository type
/// parameters. Dropping the returned future (for example when an HTTP client
/// disconnects) does not stop a batch that has already started; delivery stays
/// at least once.
#[derive(Clone)]
pub struct AsyncOutboxService {
    run: Option<Arc<dyn Fn(RunOutboxRequest) -> RunOutboxResponse + Send + Sync>>,
}

impl AsyncOutboxService {
    /// Wrap a consumer whose storage and publisher the application assembled.
    pub fn new<R: OutboxReservePort + OutboxProcessPort + 'static>(
        consumer: Arc<OutboxConsumer<R>>,
    ) -> Self {
        Self {
            run: Some(Arc::new(move |request| {
                let spec = ReservableOutboxSpec::new(request.limit());
                RunOutboxResponse::from_result(consumer.publish_batch(&spec))
            })),
        }
    }

    /// Assemble PostgreSQL publication with an application-owned publisher.
    #[cfg(feature = "diesel")]
    pub fn from_postgres(
        pool: crate::prelude::DbPool,
        publisher: Arc<dyn crate::prelude::OutboxPublisherPort>,
    ) -> Self {
        let repository = Arc::new(crate::prelude::DieselPgOutboxRepository::new(pool));
        Self::new(Arc::new(OutboxConsumer::new(repository, publisher)))
    }

    /// Delivery is not configured: every request is rejected with
    /// [`OutboxApiError::PublisherNotConfigured`] without touching storage.
    pub fn unconfigured() -> Self {
        Self { run: None }
    }

    /// Publish up to `limit` due entries with the default retry limit.
    ///
    /// Publication failures are reported in the response, not as an error;
    /// entries that were published stay completed. The only errors are a missing
    /// publisher and a sanitized failure of the blocking task.
    pub async fn run(
        &self,
        request: RunOutboxRequest,
    ) -> Result<RunOutboxResponse, OutboxApiError> {
        let run = self.run.clone().ok_or_else(err_publisher_not_configured)?;
        tokio::task::spawn_blocking(move || run(request))
            .await
            .map_err(err_run_task_failed)
    }
}

impl<R: OutboxReservePort + OutboxProcessPort + 'static> From<Arc<OutboxConsumer<R>>>
    for AsyncOutboxService
{
    fn from(consumer: Arc<OutboxConsumer<R>>) -> Self {
        Self::new(consumer)
    }
}

#[cfg(all(test, feature = "api-testing"))]
mod tests {
    use crate::prelude::{
        AsyncOutboxService,
        OutboundMessageEnvelope,
        OutboxApiError,
        OutboxApiErrorKind,
        OutboxConsumer,
        OutboxError,
        OutboxPublisherPort,
        ReservableOutboxSpec,
        RunOutboxRequest,
        RunOutboxResponse,
        testing,
        //
    };
    use std::error::Error;
    use std::sync::Arc;

    /// Blocks on the application runtime, as an AMQP bridge does. That panics
    /// on an async worker thread and fails outside a runtime.
    struct RuntimeBoundPublisher;

    impl OutboxPublisherPort for RuntimeBoundPublisher {
        fn publish(&self, _: OutboundMessageEnvelope) -> Result<(), OutboxError> {
            let handle = tokio::runtime::Handle::try_current()
                .map_err(|_| OutboxError::Transport("no runtime".into()))?;
            handle.block_on(async { Ok(()) })
        }
    }

    fn assert_reports_ids(response: &RunOutboxResponse, count: usize) {
        assert_eq!(response.processed, None);
        assert_eq!(response.errors.len(), count);
        for id in &response.errors {
            assert!(uuid::Uuid::parse_str(id).is_ok(), "{id}");
        }
    }

    async fn run(service: &AsyncOutboxService, limit: usize) -> RunOutboxResponse {
        service
            .run(RunOutboxRequest::new(limit).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn empty_batches_process_nothing() {
        let service = AsyncOutboxService::from(testing::empty_consumer());
        let response = run(&service, 5).await;
        assert_eq!(response.processed, Some(0));
        assert!(response.errors.is_empty());
    }

    #[tokio::test]
    async fn batches_respect_the_limit() {
        let service = AsyncOutboxService::from(testing::queued_consumer(3));
        for processed in [2, 1, 0] {
            let response = run(&service, 2).await;
            assert_eq!(response.processed, Some(processed));
        }
    }

    #[tokio::test]
    async fn publishes_each_entry() {
        let (consumer, published) = testing::recording_consumer(&["a", "b", "c"]);
        let service = AsyncOutboxService::from(consumer);
        assert_eq!(run(&service, 2).await.processed, Some(2));
        assert_eq!(*published.lock().unwrap(), vec!["a", "b"]);
    }

    #[tokio::test]
    async fn consumer_failures_are_reported_by_id() {
        type Consumer = fn() -> std::sync::Arc<OutboxConsumer<testing::Repository>>;
        let cases: [(Consumer, Vec<&str>); 3] = [
            (
                testing::failing_reservation_consumer,
                vec![testing::RESERVATION_FAILURE],
            ),
            (
                testing::failing_publisher_consumer,
                vec![testing::PUBLISH_FAILURE],
            ),
            // Marking the entry failed is reported before the publish error.
            (
                testing::failing_publisher_and_process_consumer,
                vec![testing::PROCESS_FAILURE, testing::PUBLISH_FAILURE],
            ),
        ];
        for (consumer, errors) in cases {
            let failures = consumer()
                .publish_batch(&ReservableOutboxSpec::new(5))
                .unwrap_err();
            let text: Vec<String> = failures.iter().map(ToString::to_string).collect();
            assert_eq!(text, errors);

            let response = run(&AsyncOutboxService::from(consumer()), 5).await;
            assert_reports_ids(&response, errors.len());
        }
    }

    #[tokio::test]
    async fn published_entries_of_a_failed_batch_stay_completed() {
        let (consumer, outcomes) = testing::partially_failing_consumer();
        let response = run(&AsyncOutboxService::from(consumer), 5).await;
        assert_reports_ids(&response, 1);
        assert_eq!(
            *outcomes.lock().unwrap(),
            vec!["completed", "failed", "completed"]
        );
    }

    #[tokio::test]
    async fn unroutable_entries_become_failed() {
        let (consumer, outcomes) = testing::unroutable_consumer();
        let response = run(&AsyncOutboxService::from(consumer), 5).await;
        assert_reports_ids(&response, 1);
        assert_eq!(*outcomes.lock().unwrap(), vec!["failed"]);
    }

    #[tokio::test]
    async fn entries_without_reservation_are_reported() {
        let failures = testing::missing_reservation_consumer()
            .publish_batch(&ReservableOutboxSpec::new(5))
            .unwrap_err();
        assert_eq!(failures.len(), 1);
        let text = failures[0].to_string();
        assert!(text.starts_with("missing reservation for entry "), "{text}");

        let service = AsyncOutboxService::from(testing::missing_reservation_consumer());
        assert_reports_ids(&run(&service, 5).await, 1);
    }

    #[tokio::test]
    async fn runs_batches_on_blocking_pool_of_the_current_runtime() {
        let service = AsyncOutboxService::from(testing::publisher_consumer(
            2,
            Arc::new(RuntimeBoundPublisher),
        ));
        let response = run(&service, 5).await;
        assert_eq!(response.processed, Some(2));
        assert!(response.errors.is_empty());
    }

    #[tokio::test]
    async fn cloned_bridges_share_the_supplied_consumer() {
        let consumer = testing::queued_consumer(2);
        let bridge = AsyncOutboxService::new(consumer.clone());
        let cloned = bridge.clone();
        for bridge in [bridge, cloned] {
            let response = run(&bridge, 1).await;
            assert_eq!(response.processed, Some(1));
        }
        assert_eq!(
            consumer
                .publish_batch(&ReservableOutboxSpec::new(1))
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn sanitizes_task_failures() {
        let service = AsyncOutboxService::from(testing::panicking_consumer());
        let error = service
            .run(RunOutboxRequest::new(1).unwrap())
            .await
            .unwrap_err();
        assert!(uuid::Uuid::parse_str(&error.to_string()).is_ok(), "{error}");
        assert_eq!(error.kind(), OutboxApiErrorKind::Internal);
        assert!(error.source().is_some());
    }

    #[tokio::test]
    async fn unconfigured_services_report_unavailable() {
        let service = AsyncOutboxService::unconfigured();
        let error = service
            .run(RunOutboxRequest::new(1).unwrap())
            .await
            .unwrap_err();
        assert!(matches!(error, OutboxApiError::PublisherNotConfigured));
        assert_eq!(error.kind(), OutboxApiErrorKind::Unavailable);
        assert_eq!(error.to_string(), "outbox publisher is not configured");
    }
}
