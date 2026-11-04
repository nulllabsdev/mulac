pub mod prelude {
    pub use super::spec::StaleEventSpec;
    pub use super::sweeper::EventSweeper;
}

mod spec {

    use chrono::Duration;

    pub struct StaleEventSpec {
        pub timeout: Duration,
        /// Use the same retry limit as the consumer.
        pub max_attempts: i32,
    }

    impl StaleEventSpec {
        pub const DEFAULT_MAX_ATTEMPTS: i32 = 6;

        pub fn new(timeout: Duration) -> Self {
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
        fn new_uses_default_max_attempts() {
            let spec = StaleEventSpec::new(Duration::minutes(5));
            assert_eq!(spec.max_attempts, StaleEventSpec::DEFAULT_MAX_ATTEMPTS);
        }

        #[test]
        fn with_max_attempts_overrides_default() {
            let spec = StaleEventSpec::new(Duration::minutes(5)).with_max_attempts(3);
            assert_eq!(spec.max_attempts, 3);
        }
    }
}

mod sweeper {
    use super::spec::StaleEventSpec;
    use crate::application::prelude::{EventError, EventSweepPort};
    use std::sync::Arc;

    #[derive(Clone)]
    pub struct EventSweeper {
        port: Arc<dyn EventSweepPort>,
    }

    impl EventSweeper {
        pub fn new(port: Arc<dyn EventSweepPort>) -> Self {
            Self { port }
        }

        pub fn sweep(&self, spec: &StaleEventSpec) -> Result<u64, EventError> {
            self.port.sweep(spec)
        }
    }
}
