//! Shared HTTP execution bridge; not a second application layer.

pub mod prelude {
    pub use super::AsyncCommandingService;
}

use crate::application::prelude::{CommandProcessPort, CommandReservePort};
use crate::execution::prelude::{CommandConsumer, ReservableCommandSpec};
use crate::http::contract::prelude::{
    RunCommandsRequest,
    RunCommandsResponse,
    //
};
use crate::http::error::prelude::{
    CommandingApiError,
    err_run_task_failed,
    //
};
use std::sync::Arc;

/// Bridges the HTTP contract to a [`CommandConsumer`] on Tokio's blocking pool.
///
/// Response formatting belongs here; [`RunCommandsRequest`] is valid by
/// construction, and command execution and lifecycle rules remain in
/// [`CommandConsumer`]. Callers without a web framework or Tokio runtime call
/// the consumer directly.
///
/// Uses the caller's runtime; it creates no runtime, pool, worker or
/// dispatcher. Repository types are erased here, at the background-task boundary,
/// so HTTP adapters do not need repository type parameters.
/// Dropping the returned future (for example when an HTTP client
/// disconnects) does not stop a command that has already started.
#[derive(Clone)]
pub struct AsyncCommandingService {
    run: Arc<dyn Fn(RunCommandsRequest) -> RunCommandsResponse + Send + Sync>,
}

impl AsyncCommandingService {
    /// Wrap a consumer whose dispatcher the application or kernel assembled,
    /// for example `PersistentKernelHandle::command_consumer()`.
    pub fn new<R: CommandReservePort + CommandProcessPort + 'static>(
        consumer: Arc<CommandConsumer<R>>,
    ) -> Self {
        Self {
            run: Arc::new(move |request| {
                let spec = ReservableCommandSpec::new(request.limit());
                RunCommandsResponse::from_result(consumer.consume(&spec))
            }),
        }
    }

    /// Execute up to `limit` due commands with the default retry limit.
    ///
    /// Command failures are reported in the response, not as an error;
    /// commands that succeeded stay completed and their events stay queued.
    /// The only error is a sanitized failure of the blocking task.
    pub async fn run(
        &self,
        request: RunCommandsRequest,
    ) -> Result<RunCommandsResponse, CommandingApiError> {
        let run = self.run.clone();
        tokio::task::spawn_blocking(move || run(request))
            .await
            .map_err(err_run_task_failed)
    }
}

impl<R: CommandReservePort + CommandProcessPort + 'static> From<Arc<CommandConsumer<R>>>
    for AsyncCommandingService
{
    fn from(consumer: Arc<CommandConsumer<R>>) -> Self {
        Self::new(consumer)
    }
}

#[cfg(all(test, feature = "api-testing"))]
mod tests {
    use crate::prelude::{
        AsyncCommandingService,
        CommandConsumer,
        ReservableCommandSpec,
        RunCommandsRequest,
        RunCommandsResponse,
        testing,
        //
    };
    use std::error::Error;

    fn assert_reports_ids(response: &RunCommandsResponse, count: usize) {
        assert_eq!(response.processed, None);
        assert_eq!(response.errors.len(), count);
        for id in &response.errors {
            assert!(uuid::Uuid::parse_str(id).is_ok(), "{id}");
        }
    }

    async fn run(service: &AsyncCommandingService, limit: usize) -> RunCommandsResponse {
        service
            .run(RunCommandsRequest::new(limit).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn empty_batches_process_nothing() {
        let service = AsyncCommandingService::from(testing::empty_consumer());
        let response = run(&service, 5).await;
        assert_eq!(response.processed, Some(0));
        assert!(response.errors.is_empty());
    }

    #[tokio::test]
    async fn batches_respect_the_limit() {
        let service = AsyncCommandingService::from(testing::queued_consumer(3));
        assert_eq!(run(&service, 2).await.processed, Some(2));
        assert_eq!(run(&service, 2).await.processed, Some(1));
        assert_eq!(run(&service, 2).await.processed, Some(0));
    }

    #[tokio::test]
    async fn consumer_failures_are_reported_by_id() {
        type Consumer = fn() -> std::sync::Arc<CommandConsumer<testing::Repository>>;
        let cases: [(Consumer, Vec<&str>); 3] = [
            (
                testing::failing_reservation_consumer,
                vec![testing::RESERVATION_FAILURE],
            ),
            (
                testing::failing_handler_consumer,
                vec![testing::HANDLER_FAILURE],
            ),
            // Marking the command failed is reported before the handler error.
            (
                testing::failing_handler_and_process_consumer,
                vec![testing::PROCESS_FAILURE, testing::HANDLER_FAILURE],
            ),
        ];
        for (consumer, errors) in cases {
            let failures = consumer()
                .consume(&ReservableCommandSpec::new(5))
                .unwrap_err();
            let text: Vec<String> = failures.iter().map(ToString::to_string).collect();
            assert_eq!(text, errors);

            let response = run(&AsyncCommandingService::from(consumer()), 5).await;
            assert_reports_ids(&response, errors.len());
        }
    }

    #[tokio::test]
    async fn runs_batches_on_blocking_pool() {
        let service = AsyncCommandingService::from(testing::queued_consumer(2));
        let response = run(&service, 5).await;
        assert_eq!(response.processed, Some(2));
    }

    #[tokio::test]
    async fn cloned_bridges_share_the_supplied_consumer() {
        let consumer = testing::queued_consumer(2);
        let bridge = AsyncCommandingService::new(consumer.clone());
        let cloned = bridge.clone();
        for bridge in [bridge, cloned] {
            let response = run(&bridge, 1).await;
            assert_eq!(response.processed, Some(1));
        }
        assert_eq!(consumer.consume(&ReservableCommandSpec::new(1)).unwrap(), 0);
    }

    #[tokio::test]
    async fn sanitizes_task_failures() {
        let service = AsyncCommandingService::from(testing::panicking_consumer());
        let error = service
            .run(RunCommandsRequest::new(1).unwrap())
            .await
            .unwrap_err();
        assert!(uuid::Uuid::parse_str(&error.to_string()).is_ok(), "{error}");
        assert!(!error.is_client_error());
        assert!(error.source().is_some());
    }
}
