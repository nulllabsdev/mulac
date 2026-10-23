pub mod io {
    pub use super::AsyncCommandingService;
}

use crate::contract::io::{
    RunCommandsRequest,
    RunCommandsResponse,
    //
};
use crate::error::io::{
    CommandingApiError,
    err_run_task_failed,
    //
};
use crate::service::io::CommandingService;
use std::sync::Arc;

/// Runs [`CommandingService`] on Tokio's blocking thread pool.
///
/// Uses the caller's runtime; it creates no runtime, pool, worker or
/// dispatcher. Dropping the returned future (for example when an HTTP client
/// disconnects) does not stop a command that has already started.
#[derive(Clone)]
pub struct AsyncCommandingService {
    service: Arc<CommandingService>,
}

impl AsyncCommandingService {
    pub fn new(service: Arc<CommandingService>) -> Self {
        Self { service }
    }

    pub async fn run(
        &self,
        request: RunCommandsRequest,
    ) -> Result<RunCommandsResponse, CommandingApiError> {
        let service = self.service.clone();
        tokio::task::spawn_blocking(move || service.run(request))
            .await
            .map_err(err_run_task_failed)?
    }
}

impl From<CommandingService> for AsyncCommandingService {
    fn from(service: CommandingService) -> Self {
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
        let service = AsyncCommandingService::from(testing::queued_service(2));
        let response = service.run(RunCommandsRequest { limit: 5 }).await.unwrap();
        assert_eq!(response.processed, Some(2));
    }

    #[tokio::test]
    async fn sanitizes_task_failures() {
        let service = AsyncCommandingService::from(testing::panicking_service());
        let error = service
            .run(RunCommandsRequest { limit: 1 })
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "queue processing task failed");
        assert!(!error.is_client_error());
        assert!(error.source().is_some());
    }
}
