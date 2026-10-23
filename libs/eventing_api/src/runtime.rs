pub mod io {
    pub use super::AsyncEventingService;
}

use crate::contract::io::{
    RunEventsRequest,
    RunEventsResponse,
    //
};
use crate::error::io::{
    EventingApiError,
    err_run_task_failed,
    //
};
use crate::service::io::EventingService;
use std::sync::Arc;

/// Runs [`EventingService`] on Tokio's blocking thread pool.
///
/// Uses the caller's runtime; it creates no runtime, pool, worker or
/// dispatcher. Dropping the returned future (for example when an HTTP client
/// disconnects) does not stop a batch that has already started.
#[derive(Clone)]
pub struct AsyncEventingService {
    service: Arc<EventingService>,
}

impl AsyncEventingService {
    pub fn new(service: Arc<EventingService>) -> Self {
        Self { service }
    }

    pub async fn run(
        &self,
        request: RunEventsRequest,
    ) -> Result<RunEventsResponse, EventingApiError> {
        let service = self.service.clone();
        tokio::task::spawn_blocking(move || service.run(request))
            .await
            .map_err(err_run_task_failed)?
    }
}

impl From<EventingService> for AsyncEventingService {
    fn from(service: EventingService) -> Self {
        Self::new(Arc::new(service))
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use crate::io::testing;
    use std::error::Error;

    #[tokio::test]
    async fn runs_batches_on_blocking_pool() {
        let service = AsyncEventingService::from(testing::queued_service(2));
        let response = service.run(RunEventsRequest { limit: 5 }).await.unwrap();
        assert_eq!(response.processed, Some(2));
    }

    #[tokio::test]
    async fn sanitizes_task_failures() {
        let service = AsyncEventingService::from(testing::panicking_service());
        let error = service
            .run(RunEventsRequest { limit: 1 })
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "queue processing task failed");
        assert!(!error.is_client_error());
        assert!(error.source().is_some());
    }
}
