//! Shared HTTP execution bridge; not a second application layer.

pub mod prelude {
    pub use super::AsyncEventingService;
}

use crate::application::prelude::{EventProcessPort, EventReservePort};
use crate::execution::prelude::{EventConsumer, ReservableEventSpec};
use crate::http::contract::prelude::{
    RunEventsRequest,
    RunEventsResponse,
    //
};
use crate::http::error::prelude::{
    EventingApiError,
    err_run_task_failed,
    //
};
use std::sync::Arc;

/// Bridges the HTTP contract to an [`EventConsumer`] on Tokio's blocking pool.
///
/// Response formatting belongs here; [`RunEventsRequest`] is valid by
/// construction, and dispatch and lifecycle rules remain in [`EventConsumer`].
/// Callers without a web framework or Tokio runtime call the consumer directly.
///
/// Uses the caller's runtime; it creates no runtime, pool, worker or
/// dispatcher. Repository types are erased here, at the background-task boundary,
/// so HTTP adapters do not need repository type parameters.
/// Dropping the returned future (for example when an HTTP client
/// disconnects) does not stop a batch that has already started.
#[derive(Clone)]
pub struct AsyncEventingService {
    run: Arc<dyn Fn(RunEventsRequest) -> RunEventsResponse + Send + Sync>,
}

impl AsyncEventingService {
    /// Wrap a consumer whose dispatcher the application or kernel assembled,
    /// for example `PersistentKernelHandle::event_consumer()`.
    pub fn new<R: EventReservePort + EventProcessPort + 'static>(
        consumer: Arc<EventConsumer<R>>,
    ) -> Self {
        Self {
            run: Arc::new(move |request| {
                let spec = ReservableEventSpec::new(request.limit());
                RunEventsResponse::from_result(consumer.consume(&spec))
            }),
        }
    }

    /// Dispatch up to `limit` due events with the default retry limit.
    ///
    /// Event failures are reported in the response, not as an error; events
    /// that were dispatched successfully stay completed.
    /// The only error is a sanitized failure of the blocking task.
    pub async fn run(
        &self,
        request: RunEventsRequest,
    ) -> Result<RunEventsResponse, EventingApiError> {
        let run = self.run.clone();
        tokio::task::spawn_blocking(move || run(request))
            .await
            .map_err(err_run_task_failed)
    }
}

impl<R: EventReservePort + EventProcessPort + 'static> From<Arc<EventConsumer<R>>>
    for AsyncEventingService
{
    fn from(consumer: Arc<EventConsumer<R>>) -> Self {
        Self::new(consumer)
    }
}

#[cfg(all(test, feature = "api-testing"))]
mod tests {
    use crate::prelude::{
        AsyncEventingService,
        EventConsumer,
        ReservableEventSpec,
        RunEventsRequest,
        RunEventsResponse,
        testing,
        //
    };
    use std::error::Error;

    fn assert_reports_ids(response: &RunEventsResponse, count: usize) {
        assert_eq!(response.processed, None);
        assert_eq!(response.errors.len(), count);
        for id in &response.errors {
            assert!(uuid::Uuid::parse_str(id).is_ok(), "{id}");
        }
    }

    async fn run(service: &AsyncEventingService, limit: usize) -> RunEventsResponse {
        service
            .run(RunEventsRequest::new(limit).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn empty_batches_process_nothing() {
        let service = AsyncEventingService::from(testing::empty_consumer());
        let response = run(&service, 5).await;
        assert_eq!(response.processed, Some(0));
        assert!(response.errors.is_empty());
    }

    #[tokio::test]
    async fn batches_respect_the_limit() {
        let service = AsyncEventingService::from(testing::queued_consumer(3));
        for processed in [2, 1, 0] {
            let response = run(&service, 2).await;
            assert_eq!(response.processed, Some(processed));
        }
    }

    #[tokio::test]
    async fn dispatches_each_event_to_the_subscriber() {
        let (consumer, handled) = testing::recording_consumer(&["a", "b", "c"]);
        let service = AsyncEventingService::from(consumer);
        assert_eq!(run(&service, 2).await.processed, Some(2));
        assert_eq!(*handled.lock().unwrap(), vec!["a", "b"]);
    }

    #[tokio::test]
    async fn consumer_failures_are_reported_by_id() {
        type Consumer = fn() -> std::sync::Arc<EventConsumer<testing::Repository>>;
        let cases: [(Consumer, Vec<&str>); 3] = [
            (
                testing::failing_reservation_consumer,
                vec![testing::RESERVATION_FAILURE],
            ),
            (
                testing::failing_subscriber_consumer,
                vec![testing::SUBSCRIBER_FAILURE],
            ),
            // Marking the event failed is reported before the subscriber error.
            (
                testing::failing_subscriber_and_process_consumer,
                vec![testing::PROCESS_FAILURE, testing::SUBSCRIBER_FAILURE],
            ),
        ];
        for (consumer, errors) in cases {
            let failures = consumer()
                .consume(&ReservableEventSpec::new(5))
                .unwrap_err();
            let text: Vec<String> = failures.iter().map(ToString::to_string).collect();
            assert_eq!(text, errors);

            let response = run(&AsyncEventingService::from(consumer()), 5).await;
            assert_reports_ids(&response, errors.len());
        }
    }

    #[tokio::test]
    async fn successful_events_of_a_failed_batch_stay_completed() {
        let (consumer, completed) = testing::partially_failing_consumer();
        let response = run(&AsyncEventingService::from(consumer), 5).await;
        assert_reports_ids(&response, 1);
        assert_eq!(*completed.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn runs_batches_on_blocking_pool() {
        let service = AsyncEventingService::from(testing::queued_consumer(2));
        let response = run(&service, 5).await;
        assert_eq!(response.processed, Some(2));
    }

    #[tokio::test]
    async fn cloned_bridges_share_the_supplied_consumer() {
        let consumer = testing::queued_consumer(2);
        let bridge = AsyncEventingService::new(consumer.clone());
        let cloned = bridge.clone();
        for bridge in [bridge, cloned] {
            let response = run(&bridge, 1).await;
            assert_eq!(response.processed, Some(1));
        }
        assert_eq!(consumer.consume(&ReservableEventSpec::new(1)).unwrap(), 0);
    }

    #[tokio::test]
    async fn sanitizes_task_failures() {
        let service = AsyncEventingService::from(testing::panicking_consumer());
        let error = service
            .run(RunEventsRequest::new(1).unwrap())
            .await
            .unwrap_err();
        assert!(uuid::Uuid::parse_str(&error.to_string()).is_ok(), "{error}");
        assert!(!error.is_client_error());
        assert!(error.source().is_some());
    }
}
