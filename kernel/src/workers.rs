use commanding::prelude::{
    CommandConsumer, CommandProcessPort, CommandReservePort, ReservableCommandSpec,
};
use eventing::prelude::{EventConsumer, EventProcessPort, EventReservePort, ReservableEventSpec};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const BATCH_SIZE: usize = 10;

pub async fn run_command_worker<R: CommandReservePort + CommandProcessPort + 'static>(
    consumer: Arc<CommandConsumer<R>>,
    token: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
        }
        let c = Arc::clone(&consumer);
        match tokio::task::spawn_blocking(move || {
            c.consume(&ReservableCommandSpec::new(BATCH_SIZE))
        })
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(errs)) => {
                for e in &errs {
                    tracing::error!("command worker: {e}");
                }
            }
            Err(e) => tracing::error!("command worker panicked: {e}"),
        }
    }
}

pub async fn run_event_worker<R: EventReservePort + EventProcessPort + 'static>(
    consumer: Arc<EventConsumer<R>>,
    token: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
        }
        let c = Arc::clone(&consumer);
        match tokio::task::spawn_blocking(move || c.consume(&ReservableEventSpec::new(BATCH_SIZE)))
            .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(errs)) => {
                for e in &errs {
                    tracing::error!("event worker: {e}");
                }
            }
            Err(e) => tracing::error!("event worker panicked: {e}"),
        }
    }
}
