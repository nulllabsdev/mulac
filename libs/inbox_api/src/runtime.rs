pub mod io {
    pub use super::AsyncInboxService;
}

use crate::contract::io::{
    RunRequest,
    RunResponse,
    SubmitInboxRequest,
    SubmitInboxResponse,
    //
};
use crate::error::io::{
    InboxApiError,
    err_run_task_failed,
    err_submission_task_failed,
    //
};
use crate::service::io::InboxService;
use std::sync::Arc;

/// Runs [`InboxService`] operations on Tokio's blocking thread pool.
///
/// Uses the caller's runtime; it creates no runtime, pool or worker. Dropping
/// the returned future (for example when an HTTP client disconnects) does not
/// stop a blocking database operation that has already started.
#[derive(Clone)]
pub struct AsyncInboxService {
    service: Arc<InboxService>,
}

impl AsyncInboxService {
    pub fn new(service: Arc<InboxService>) -> Self {
        Self { service }
    }

    pub async fn submit(
        &self,
        request: SubmitInboxRequest,
    ) -> Result<SubmitInboxResponse, InboxApiError> {
        let service = self.service.clone();
        tokio::task::spawn_blocking(move || service.submit(request))
            .await
            .map_err(err_submission_task_failed)?
    }

    pub async fn run(&self, request: RunRequest) -> Result<RunResponse, InboxApiError> {
        let service = self.service.clone();
        tokio::task::spawn_blocking(move || service.run(request))
            .await
            .map_err(err_run_task_failed)?
    }
}

impl From<InboxService> for AsyncInboxService {
    fn from(service: InboxService) -> Self {
        Self::new(Arc::new(service))
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use crate::io::testing;
    use serde_json::json;
    use std::error::Error;

    #[tokio::test]
    async fn runs_operations_on_blocking_pool() {
        let (service, store) = testing::recording_service();
        let service = AsyncInboxService::from(service);
        let request = SubmitInboxRequest::from_json(
            json!({"payload":1,"routing_key":"x","source":"crawler"}),
        );
        let response = service.submit(request.unwrap()).await.unwrap();
        assert_eq!(store.stored_ids(), vec![response.message_id]);
        let response = service.run(RunRequest { limit: 1 }).await.unwrap();
        assert_eq!(response.processed, Some(0));
    }

    #[tokio::test]
    async fn sanitizes_task_failures() {
        let service = AsyncInboxService::from(testing::panicking_service());
        let request = SubmitInboxRequest::from_json(
            json!({"payload":1,"routing_key":"x","source":"crawler"}),
        );
        let error = service.submit(request.unwrap()).await.unwrap_err();
        assert_eq!(error.to_string(), "inbox submission task failed");
        assert!(!error.is_client_error());
        assert!(error.source().is_some());

        let error = service.run(RunRequest { limit: 1 }).await.unwrap_err();
        assert_eq!(error.to_string(), "queue processing task failed");
    }
}
