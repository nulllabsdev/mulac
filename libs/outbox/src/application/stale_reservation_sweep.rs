pub mod prelude {
    pub use super::spec::StaleReservationSpec;
    pub use super::sweeper::ReservationSweeper;
}

mod spec {
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct StaleReservationSpec {
        pub timeout: chrono::Duration,
        /// Use the same retry limit as the consumer.
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
}

mod sweeper {
    use super::spec::StaleReservationSpec;
    use crate::application::prelude::{OutboxError, OutboxSweepPort};
    use std::sync::Arc;

    pub struct ReservationSweeper {
        sweep: Arc<dyn OutboxSweepPort>,
    }

    impl ReservationSweeper {
        pub fn new(sweep: Arc<dyn OutboxSweepPort>) -> Self {
            Self { sweep }
        }

        pub fn sweep(&self, spec: &StaleReservationSpec) -> Result<u64, OutboxError> {
            self.sweep.sweep(spec)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::prelude::{ReservationSweeper, StaleReservationSpec};
    use crate::application::prelude::{OutboxError, OutboxSweepPort};
    use chrono::Duration;
    use std::sync::{Arc, Mutex};

    struct FakeSweepPort {
        result: Mutex<Result<u64, String>>,
        specs: Mutex<Vec<StaleReservationSpec>>,
    }

    impl OutboxSweepPort for FakeSweepPort {
        fn sweep(&self, spec: &StaleReservationSpec) -> Result<u64, OutboxError> {
            self.specs.lock().unwrap().push(spec.clone());
            match &*self.result.lock().unwrap() {
                Ok(count) => Ok(*count),
                Err(message) => Err(OutboxError::Reservation(message.clone())),
            }
        }
    }

    #[test]
    fn new_uses_default_max_attempts() {
        let spec = StaleReservationSpec::new(Duration::minutes(5));

        assert_eq!(spec.timeout, Duration::minutes(5));
        assert_eq!(
            spec.max_attempts,
            StaleReservationSpec::DEFAULT_MAX_ATTEMPTS
        );
    }

    #[test]
    fn with_max_attempts_overrides_default() {
        let spec = StaleReservationSpec::new(Duration::minutes(5)).with_max_attempts(3);

        assert_eq!(spec.timeout, Duration::minutes(5));
        assert_eq!(spec.max_attempts, 3);
    }

    #[test]
    fn sweep_returns_successful_count() {
        let port = Arc::new(FakeSweepPort {
            result: Mutex::new(Ok(4)),
            specs: Mutex::new(vec![]),
        });
        let sweeper = ReservationSweeper::new(port);

        let swept = sweeper
            .sweep(&StaleReservationSpec::new(Duration::minutes(5)))
            .expect("sweep succeeds");

        assert_eq!(swept, 4);
    }

    #[test]
    fn sweep_propagates_port_errors() {
        let port = Arc::new(FakeSweepPort {
            result: Mutex::new(Err("db timeout".into())),
            specs: Mutex::new(vec![]),
        });
        let sweeper = ReservationSweeper::new(port);

        let err = sweeper
            .sweep(&StaleReservationSpec::new(Duration::minutes(5)))
            .expect_err("sweep returns the adapter error");

        assert!(matches!(err, OutboxError::Reservation(message) if message == "db timeout"));
    }

    #[test]
    fn sweep_passes_spec_through_to_port() {
        let port = Arc::new(FakeSweepPort {
            result: Mutex::new(Ok(1)),
            specs: Mutex::new(vec![]),
        });
        let sweeper = ReservationSweeper::new(port.clone());
        let spec = StaleReservationSpec::new(Duration::minutes(15)).with_max_attempts(9);

        sweeper.sweep(&spec).expect("sweep succeeds");

        assert_eq!(port.specs.lock().unwrap().as_slice(), &[spec]);
    }
}
