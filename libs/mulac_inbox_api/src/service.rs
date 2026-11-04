pub mod prelude {
    pub use super::InboxService;
}

use crate::contract::prelude::{
    RunRequest,
    RunResponse,
    SubmitInboxRequest,
    SubmitInboxResponse,
    //
};
use crate::error::prelude::{InboxApiError, err_storage_failed};
use inbox::prelude::{
    InboxConsumer,
    InboxProcessPort,
    InboxReservePort,
    InboxStorePort,
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
    store: Arc<dyn InboxStorePort>,
    run: Arc<dyn Fn(RunRequest) -> RunResponse + Send + Sync>,
}

impl InboxService {
    pub fn new<R: InboxReservePort + InboxProcessPort + 'static>(
        store: Arc<dyn InboxStorePort>,
        consumer: Arc<InboxConsumer<R>>,
    ) -> Self {
        Self {
            store,
            run: Arc::new(move |request| {
                RunResponse::from_result(consumer.process(&ReservableInboxSpec::new(request.limit)))
            }),
        }
    }

    /// Assemble PostgreSQL storage from an existing pool.
    ///
    /// The inbox consumer only records commands; running the inbox never
    /// executes them.
    #[cfg(feature = "diesel")]
    pub fn from_postgres(pool: inbox::prelude::DbPool) -> Self {
        let store = Arc::new(inbox::prelude::DieselPgInboxRepository::new(pool.clone()));
        let commands = Arc::new(commanding::prelude::DieselPgCommandRepository::new(pool));
        let consumer = Arc::new(InboxConsumer::new(
            store.clone(),
            commanding::prelude::CommandGateway::two_phased(commands),
        ));
        Self::new(store, consumer)
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
        self.store.store(envelope).map_err(err_storage_failed)?;
        Ok(SubmitInboxResponse { message_id })
    }

    /// Queue commands for one batch of stored messages.
    ///
    /// Consumer failures are reported in the response, not as an error;
    /// messages that succeeded stay committed.
    pub fn run(&self, request: RunRequest) -> Result<RunResponse, InboxApiError> {
        request.validate()?;
        Ok((self.run)(request))
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use crate::prelude::{InboxApiError, RunRequest, SubmitInboxRequest, testing};
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
