pub mod prelude {
    pub use super::spec::StaleReservationSpec;
    pub use super::sweeper::ReservationSweeper;
}

mod spec {
    /// Stale reservations use the same retry limit as the consumer.
    pub struct StaleReservationSpec {
        pub timeout: chrono::Duration,
        pub max_attempts: i32,
    }
    impl StaleReservationSpec {
        pub const DEFAULT_MAX_ATTEMPTS: i32 = 6;
        pub fn new(timeout: chrono::Duration) -> Self {
            Self {
                timeout,
                max_attempts: Self::DEFAULT_MAX_ATTEMPTS,
            }
        }
        pub fn with_max_attempts(mut self, max_attempts: i32) -> Self {
            self.max_attempts = max_attempts;
            self
        }
    }
    #[cfg(test)]
    mod tests {
        use super::*;
        #[test]
        fn retry_limit_is_configurable() {
            assert_eq!(
                StaleReservationSpec::new(chrono::Duration::minutes(5)).max_attempts,
                6
            );
            assert_eq!(
                StaleReservationSpec::new(chrono::Duration::minutes(5))
                    .with_max_attempts(3)
                    .max_attempts,
                3
            );
        }
    }
}

mod sweeper {
    use super::spec::StaleReservationSpec;
    use crate::application::prelude::{InboxError, InboxSweepPort};
    use std::sync::Arc;

    /// Releases messages that have been stuck in `Reserved` status longer than a
    /// configured timeout, making them eligible for re-reservation below the attempt cap.
    ///
    /// A reservation becomes stale when the worker that claimed it crashes,
    /// is shut down, or fails to call `completed` or `failed` before the timeout
    /// expires. The sweep transitions each stale message to `Failed`, including
    /// exhausted attempts, and applies the standard retry backoff
    /// to `scheduled_at`.
    #[derive(Clone)]
    pub struct ReservationSweeper {
        port: Arc<dyn InboxSweepPort>,
    }

    impl ReservationSweeper {
        pub fn new(port: Arc<dyn InboxSweepPort>) -> Self {
            Self { port }
        }

        /// Release all reservations older than `spec.timeout`.
        ///
        /// Returns the number of messages whose reservations were released.
        pub fn sweep(&self, spec: &StaleReservationSpec) -> Result<usize, InboxError> {
            self.port.sweep(spec)
        }
    }
}
