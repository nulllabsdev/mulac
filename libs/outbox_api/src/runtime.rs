pub mod io {
    pub use super::AsyncOutboxService;
}

use crate::contract::io::{
    RunOutboxRequest,
    RunOutboxResponse,
    //
};
use crate::error::io::{
    OutboxApiError,
    err_publisher_not_configured,
    err_run_task_failed,
    //
};
use crate::service::io::OutboxService;
use std::sync::Arc;

/// Runs [`OutboxService`] on Tokio's blocking thread pool, or reports that
/// no publisher is configured.
///
/// Uses the caller's runtime; it creates no runtime, pool, worker or
/// connection. Publishers run on a blocking-pool thread of that runtime, so
/// they may use `Handle::current().block_on`. Dropping the returned future
/// (for example when an HTTP client disconnects) does not stop a batch that
/// has already started; delivery stays at least once.
#[derive(Clone)]
pub struct AsyncOutboxService {
    service: Option<Arc<OutboxService>>,
}

impl AsyncOutboxService {
    pub fn new(service: Arc<OutboxService>) -> Self {
        Self {
            service: Some(service),
        }
    }

    /// Delivery is not configured: requests are validated, then rejected with
    /// [`OutboxApiError::PublisherNotConfigured`] without touching storage.
    pub fn unconfigured() -> Self {
        Self { service: None }
    }

    pub async fn run(
        &self,
        request: RunOutboxRequest,
    ) -> Result<RunOutboxResponse, OutboxApiError> {
        request.validate()?;
        let service = self
            .service
            .clone()
            .ok_or_else(err_publisher_not_configured)?;
        tokio::task::spawn_blocking(move || service.run(request))
            .await
            .map_err(err_run_task_failed)?
    }
}

impl From<OutboxService> for AsyncOutboxService {
    fn from(service: OutboxService) -> Self {
        Self::new(Arc::new(service))
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use crate::io::{OutboxApiErrorKind, testing};
    use outbox::io::{
        OutboundMessageEnvelope,
        OutboxError,
        OutboxPublisherPort,
        //
    };
    use std::error::Error;

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

    #[tokio::test]
    async fn runs_batches_on_blocking_pool_of_the_current_runtime() {
        let service = AsyncOutboxService::from(testing::publisher_service(
            2,
            Arc::new(RuntimeBoundPublisher),
        ));
        let response = service.run(RunOutboxRequest { limit: 5 }).await.unwrap();
        assert_eq!(response.processed, Some(2));
        assert!(response.errors.is_empty());
    }

    #[tokio::test]
    async fn sanitizes_task_failures() {
        let service = AsyncOutboxService::from(testing::panicking_service());
        let error = service
            .run(RunOutboxRequest { limit: 1 })
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "queue processing task failed");
        assert_eq!(error.kind(), OutboxApiErrorKind::Internal);
        assert!(error.source().is_some());
    }

    #[tokio::test]
    async fn unconfigured_services_validate_then_report_unavailable() {
        let service = AsyncOutboxService::unconfigured();
        let error = service
            .run(RunOutboxRequest { limit: 0 })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), OutboxApiErrorKind::Invalid);
        let error = service
            .run(RunOutboxRequest { limit: 1 })
            .await
            .unwrap_err();
        assert!(matches!(error, OutboxApiError::PublisherNotConfigured));
        assert_eq!(error.kind(), OutboxApiErrorKind::Unavailable);
        assert_eq!(error.to_string(), "outbox publisher is not configured");
    }
}
