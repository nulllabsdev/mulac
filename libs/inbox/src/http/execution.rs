//! Shared HTTP execution bridge; not a second application layer.

pub mod prelude {
    pub use super::AsyncInboxService;
}

use crate::application::prelude::{
    InboxProcessPort,
    InboxReservePort,
    InboxStorePort,
    NewInboxMessageEnvelope,
    //
};
use crate::execution::prelude::{InboxConsumer, ReservableInboxSpec};
use crate::http::contract::prelude::{
    RunRequest,
    RunResponse,
    SubmitInboxRequest,
    SubmitInboxResponse,
    //
};
use crate::http::error::prelude::{
    InboxApiError,
    err_run_task_failed,
    err_storage_failed,
    err_submission_task_failed,
    //
};
use std::sync::Arc;

/// Bridges the HTTP contract to inbox storage and an [`InboxConsumer`] on Tokio's
/// blocking pool.
///
/// Submission validation and response formatting belong here; [`RunRequest`] is
/// valid by construction, and processing and lifecycle rules remain in
/// [`InboxConsumer`]. Callers without a web framework or Tokio runtime use
/// [`InboxStorePort`] and the consumer directly.
///
/// Uses the caller's runtime; it creates no runtime, pool or worker. Repository
/// types are erased here, at the background-task boundary, so HTTP adapters do not
/// need repository type parameters. Dropping the returned future (for example when
/// an HTTP client disconnects) does not stop a blocking database operation that
/// has already started.
#[derive(Clone)]
pub struct AsyncInboxService {
    run: Arc<dyn Fn(RunRequest) -> RunResponse + Send + Sync>,
    submit:
        Arc<dyn Fn(SubmitInboxRequest) -> Result<SubmitInboxResponse, InboxApiError> + Send + Sync>,
}

impl AsyncInboxService {
    /// Wrap the store and consumer the application assembled.
    pub fn new<R: InboxReservePort + InboxProcessPort + 'static>(
        store: Arc<dyn InboxStorePort>,
        consumer: Arc<InboxConsumer<R>>,
    ) -> Self {
        Self {
            run: Arc::new(move |request| {
                let spec = ReservableInboxSpec::new(request.limit());
                RunResponse::from_result(consumer.process(&spec))
            }),
            submit: Arc::new(move |request| {
                let envelope = NewInboxMessageEnvelope::try_from(request)?;
                let message_id = envelope.id().to_string();
                store.store(envelope).map_err(err_storage_failed)?;
                Ok(SubmitInboxResponse { message_id })
            }),
        }
    }

    /// Share one PostgreSQL repository between storage and consumer.
    #[cfg(feature = "diesel")]
    pub fn from_postgres(pool: crate::prelude::DbPool) -> Self {
        use crate::prelude::DieselPgInboxRepository;
        use commanding::prelude::{CommandGateway, DieselPgCommandRepository};
        let repository = Arc::new(DieselPgInboxRepository::new(pool.clone()));
        let commands = Arc::new(DieselPgCommandRepository::new(pool));
        let consumer = Arc::new(InboxConsumer::new(
            repository.clone(),
            CommandGateway::two_phased(commands),
        ));
        Self::new(repository, consumer)
    }

    /// Store one message; the ID is returned only after storage succeeds.
    ///
    /// A duplicate `message_id` is accepted and leaves the stored row unchanged.
    pub async fn submit(
        &self,
        request: SubmitInboxRequest,
    ) -> Result<SubmitInboxResponse, InboxApiError> {
        let submit = self.submit.clone();
        tokio::task::spawn_blocking(move || submit(request))
            .await
            .map_err(err_submission_task_failed)?
    }

    /// Queue commands for one batch of stored messages.
    ///
    /// Consumer failures are reported in the response, not as an error; messages
    /// that succeeded stay committed. The only error is a sanitized failure of
    /// the blocking task.
    pub async fn run(&self, request: RunRequest) -> Result<RunResponse, InboxApiError> {
        let run = self.run.clone();
        tokio::task::spawn_blocking(move || run(request))
            .await
            .map_err(err_run_task_failed)
    }
}

impl<R: InboxReservePort + InboxProcessPort + 'static>
    From<(Arc<dyn InboxStorePort>, Arc<InboxConsumer<R>>)> for AsyncInboxService
{
    fn from((store, consumer): (Arc<dyn InboxStorePort>, Arc<InboxConsumer<R>>)) -> Self {
        Self::new(store, consumer)
    }
}

#[cfg(all(test, feature = "api-testing"))]
mod tests {
    use crate::prelude::{
        AsyncInboxService,
        InboxApiError,
        ReservableInboxSpec,
        RunRequest,
        RunResponse,
        SubmitInboxRequest,
        testing,
        //
    };
    use serde_json::json;
    use std::error::Error;

    fn request(body: serde_json::Value) -> SubmitInboxRequest {
        SubmitInboxRequest::from_json(body).unwrap()
    }

    fn assert_reports_ids(response: &RunResponse, count: usize) {
        assert_eq!(response.processed, None);
        assert_eq!(response.errors.len(), count);
        for id in &response.errors {
            assert!(uuid::Uuid::parse_str(id).is_ok(), "{id}");
        }
    }

    fn valid_submission() -> SubmitInboxRequest {
        request(json!({"payload":1,"routing_key":"x","source":"crawler"}))
    }

    async fn run(service: &AsyncInboxService, limit: usize) -> RunResponse {
        service.run(RunRequest::new(limit).unwrap()).await.unwrap()
    }

    #[tokio::test]
    async fn submit_returns_the_stored_id() {
        let (parts, store) = testing::recording_parts();
        let service = AsyncInboxService::from(parts);
        let id = "0191d6c4-8f5e-7a3b-9c1d-2e3f4a5b6c7d";
        let response = service
            .submit(request(
                json!({"payload":1,"routing_key":"x","source":"crawler","message_id":id}),
            ))
            .await
            .unwrap();
        assert_eq!(response.message_id, id);
        assert_eq!(store.stored_ids(), vec![id.to_owned()]);
    }

    #[tokio::test]
    async fn submit_assigns_and_returns_generated_id() {
        let (parts, store) = testing::recording_parts();
        let service = AsyncInboxService::from(parts);
        let response = service.submit(valid_submission()).await.unwrap();
        assert_eq!(store.stored_ids(), vec![response.message_id]);
    }

    #[tokio::test]
    async fn submit_validates_before_storing() {
        let (parts, store) = testing::recording_parts();
        let service = AsyncInboxService::from(parts);
        let invalid = SubmitInboxRequest::parse_json(
            json!({"payload":null,"routing_key":"x","source":"crawler"}),
        )
        .unwrap();
        let error = service.submit(invalid).await.unwrap_err();
        assert!(matches!(error, InboxApiError::PayloadEmpty));
        assert!(store.stored_ids().is_empty());
    }

    #[tokio::test]
    async fn submit_reports_storage_failure() {
        let service = AsyncInboxService::from(testing::failing_storage_parts());
        let error = service.submit(valid_submission()).await.unwrap_err();
        assert!(matches!(error, InboxApiError::StorageFailed(_)));
    }

    #[tokio::test]
    async fn run_handles_empty_batches_and_consumer_failures() {
        let (parts, _) = testing::recording_parts();
        let response = run(&AsyncInboxService::from(parts), 5).await;
        assert_eq!(response.processed, Some(0));
        assert!(response.errors.is_empty());

        let service = AsyncInboxService::from(testing::failing_storage_parts());
        assert_reports_ids(&run(&service, 5).await, 1);

        let (_, consumer) = testing::failing_storage_parts();
        let failures = consumer.process(&ReservableInboxSpec::new(5)).unwrap_err();
        assert_eq!(failures[0].to_string(), testing::RESERVATION_FAILURE);
    }

    #[tokio::test]
    async fn runs_operations_on_blocking_pool() {
        let (parts, store) = testing::recording_parts();
        let service = AsyncInboxService::from(parts);
        let response = service.submit(valid_submission()).await.unwrap();
        assert_eq!(store.stored_ids(), vec![response.message_id]);
        assert_eq!(run(&service, 1).await.processed, Some(0));
    }

    #[tokio::test]
    async fn cloned_bridges_share_the_supplied_store() {
        let (parts, store) = testing::recording_parts();
        let bridge = AsyncInboxService::from(parts);
        let cloned = bridge.clone();
        for bridge in [bridge, cloned] {
            bridge.submit(valid_submission()).await.unwrap();
        }
        assert_eq!(store.stored_ids().len(), 2);
    }

    #[tokio::test]
    async fn sanitizes_task_failures() {
        let service = AsyncInboxService::from(testing::panicking_parts());
        let error = service.submit(valid_submission()).await.unwrap_err();
        assert!(uuid::Uuid::parse_str(&error.to_string()).is_ok(), "{error}");
        assert!(!error.is_client_error());
        assert!(error.source().is_some());

        let error = service.run(RunRequest::new(1).unwrap()).await.unwrap_err();
        assert!(uuid::Uuid::parse_str(&error.to_string()).is_ok(), "{error}");
    }
}
