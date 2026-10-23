pub mod io {
    pub use super::InboxService;
}

use crate::contract::io::{
    RunRequest,
    RunResponse,
    SubmitInboxRequest,
    SubmitInboxResponse,
    //
};
use crate::error::io::{InboxApiError, err_storage_failed};
use inbox::io::{
    InboxConsumer,
    InboxRecorder,
    NewInboxMessageEnvelope,
    ReservableInboxSpec,
    //
};
use std::sync::Arc;

/// Synchronous inbox submission and batch processing.
///
/// Both operations block on storage; use `AsyncInboxService` (feature
/// `tokio`) from async handlers.
pub struct InboxService {
    recorder: Arc<InboxRecorder>,
    consumer: Arc<InboxConsumer>,
}

impl InboxService {
    pub fn new(recorder: Arc<InboxRecorder>, consumer: Arc<InboxConsumer>) -> Self {
        Self { recorder, consumer }
    }

    /// Assemble PostgreSQL storage from an existing pool.
    ///
    /// The inbox consumer only records commands; running the inbox never
    /// executes them.
    #[cfg(feature = "diesel")]
    pub fn from_postgres(pool: inbox::io::DbPool) -> Self {
        use commanding::io::{
            CommandGateway,
            CommandRecorder,
            CommandRecorderRepository,
            CommandStoreStorage,
            //
        };
        use inbox::io::{
            InboxConsumerRepository,
            InboxConsumerStorage,
            InboxRecorderRepository,
            InboxStoreStorage,
            //
        };

        let store = Arc::new(InboxStoreStorage::new(pool.clone()));
        let recorder = Arc::new(InboxRecorder::new(Arc::new(InboxRecorderRepository::new(
            store,
        ))));
        let storage = Arc::new(InboxConsumerStorage::new(pool.clone()));
        let commands = Arc::new(CommandRecorder::new(Arc::new(
            CommandRecorderRepository::new(Arc::new(CommandStoreStorage::new(pool))),
        )));
        let consumer = Arc::new(InboxConsumer::new(
            InboxConsumerRepository::new(storage.clone(), storage),
            CommandGateway::two_phased(commands),
        ));
        Self::new(recorder, consumer)
    }

    /// Store one message; the ID is returned only after storage succeeds.
    ///
    /// A duplicate `message_id` is accepted and leaves the stored row unchanged.
    pub fn submit(
        &self,
        request: SubmitInboxRequest,
    ) -> Result<SubmitInboxResponse, InboxApiError> {
        let envelope = NewInboxMessageEnvelope::try_from(request)?;
        let message_id = envelope.id().to_string();
        self.recorder
            .publish(envelope)
            .map_err(err_storage_failed)?;
        Ok(SubmitInboxResponse { message_id })
    }

    /// Queue commands for one batch of stored messages.
    ///
    /// Consumer failures are reported in the response, not as an error;
    /// messages that succeeded stay committed.
    pub fn run(&self, request: RunRequest) -> Result<RunResponse, InboxApiError> {
        request.validate()?;
        let result = self
            .consumer
            .process(&ReservableInboxSpec::new(request.limit));
        Ok(RunResponse::from_result(result))
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use crate::io::{InboxApiError, RunRequest, SubmitInboxRequest, testing};
    use serde_json::json;

    fn request(body: serde_json::Value) -> SubmitInboxRequest {
        SubmitInboxRequest::from_json(body).unwrap()
    }

    #[test]
    fn submit_returns_the_stored_id() {
        let (service, store) = testing::recording_service();
        let id = "0191d6c4-8f5e-7a3b-9c1d-2e3f4a5b6c7d";
        let response = service
            .submit(request(
                json!({"payload":1,"routing_key":"x","source":"crawler","message_id":id}),
            ))
            .unwrap();
        assert_eq!(response.message_id, id);
        assert_eq!(store.stored_ids(), vec![id.to_owned()]);
    }

    #[test]
    fn submit_assigns_and_returns_generated_id() {
        let (service, store) = testing::recording_service();
        let response = service
            .submit(request(
                json!({"payload":1,"routing_key":"x","source":"crawler"}),
            ))
            .unwrap();
        assert_eq!(store.stored_ids(), vec![response.message_id]);
    }

    #[test]
    fn submit_reports_storage_failure() {
        let service = testing::failing_storage_service();
        let error = service
            .submit(request(
                json!({"payload":1,"routing_key":"x","source":"crawler"}),
            ))
            .unwrap_err();
        assert!(matches!(error, InboxApiError::StorageFailed(_)));
    }

    #[test]
    fn run_handles_empty_batches_and_consumer_failures() {
        let (service, _) = testing::recording_service();
        let response = service.run(RunRequest { limit: 5 }).unwrap();
        assert_eq!(response.processed, Some(0));
        assert!(response.errors.is_empty());

        let response = testing::failing_storage_service()
            .run(RunRequest { limit: 5 })
            .unwrap();
        assert_eq!(response.processed, None);
        assert_eq!(
            response.errors,
            vec![testing::RESERVATION_FAILURE.to_owned()]
        );
    }

    #[test]
    fn run_validates_limit() {
        let (service, _) = testing::recording_service();
        assert!(service.run(RunRequest { limit: 0 }).is_err());
    }
}
