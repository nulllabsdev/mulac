pub mod prelude {
    pub use super::CommandingService;
}

use crate::contract::prelude::{
    RunCommandsRequest,
    RunCommandsResponse,
    //
};
use crate::error::prelude::CommandingApiError;
use commanding::prelude::{
    CommandConsumer,
    CommandProcessPort,
    CommandReservePort,
    ReservableCommandSpec,
    //
};
use std::sync::Arc;

/// Synchronous execution of one batch of due commands.
///
/// Running blocks on storage and command handlers; use
/// `AsyncCommandingService` (feature `tokio`) from async handlers.
pub struct CommandingService {
    run: Arc<dyn Fn(RunCommandsRequest) -> RunCommandsResponse + Send + Sync>,
}

impl CommandingService {
    /// Wrap a consumer whose dispatcher the application or kernel assembled,
    /// for example `PersistentKernelHandle::command_consumer()`.
    pub fn new<R: CommandReservePort + CommandProcessPort + 'static>(
        consumer: Arc<CommandConsumer<R>>,
    ) -> Self {
        Self {
            run: Arc::new(move |request| {
                RunCommandsResponse::from_result(
                    consumer.consume(&ReservableCommandSpec::new(request.limit)),
                )
            }),
        }
    }

    /// Execute up to `limit` due commands with the default retry limit.
    ///
    /// Command failures are reported in the response, not as an error;
    /// commands that succeeded stay completed and their events stay queued.
    pub fn run(
        &self,
        request: RunCommandsRequest,
    ) -> Result<RunCommandsResponse, CommandingApiError> {
        request.validate()?;
        Ok((self.run)(request))
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use crate::prelude::{CommandingApiError, RunCommandsRequest, testing};

    fn run(
        service: crate::prelude::CommandingService,
        limit: usize,
    ) -> crate::prelude::RunCommandsResponse {
        service.run(RunCommandsRequest { limit }).unwrap()
    }

    #[test]
    fn empty_batches_process_nothing() {
        let response = run(testing::empty_service(), 5);
        assert_eq!(response.processed, Some(0));
        assert!(response.errors.is_empty());
    }

    #[test]
    fn batches_respect_the_limit() {
        let service = testing::queued_service(3);
        assert_eq!(
            service
                .run(RunCommandsRequest { limit: 2 })
                .unwrap()
                .processed,
            Some(2)
        );
        assert_eq!(
            service
                .run(RunCommandsRequest { limit: 2 })
                .unwrap()
                .processed,
            Some(1)
        );
        assert_eq!(
            service
                .run(RunCommandsRequest { limit: 2 })
                .unwrap()
                .processed,
            Some(0)
        );
    }

    #[test]
    fn consumer_failures_are_reported_in_the_response() {
        for (service, errors) in [
            (
                testing::failing_reservation_service(),
                vec![testing::RESERVATION_FAILURE],
            ),
            (
                testing::failing_handler_service(),
                vec![testing::HANDLER_FAILURE],
            ),
            // Marking the command failed is reported before the handler error.
            (
                testing::failing_handler_and_process_service(),
                vec![testing::PROCESS_FAILURE, testing::HANDLER_FAILURE],
            ),
        ] {
            let response = run(service, 5);
            assert_eq!(response.processed, None);
            assert_eq!(response.errors, errors);
        }
    }

    #[test]
    fn direct_callers_are_validated() {
        let error = testing::panicking_service()
            .run(RunCommandsRequest { limit: 0 })
            .unwrap_err();
        assert!(matches!(error, CommandingApiError::LimitOutOfRange { .. }));
    }
}
