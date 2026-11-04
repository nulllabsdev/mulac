pub mod prelude {
    pub use super::spec::StaleCommandSpec;
    pub use super::sweeper::CommandSweeper;
}

mod spec {
    use crate::application::prelude::Criterion;
    use chrono::Duration;
    use chrono::{DateTime, Utc};

    pub struct StaleCommandSpec {
        pub timeout: Duration,
        /// Retained for compatibility; expired commands always become `Failed`.
        /// Use the same limit as the command consumer.
        pub max_attempts: i32,
    }

    impl StaleCommandSpec {
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

        /// Resolve criteria using the same clock that created the reservations.
        pub fn criteria(&self, now: DateTime<Utc>) -> Vec<Criterion> {
            let cutoff = now - self.timeout;
            vec![Criterion::ReservedBefore(cutoff)]
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn new_uses_default_max_attempts() {
            let spec = StaleCommandSpec::new(Duration::minutes(5));
            assert_eq!(spec.max_attempts, StaleCommandSpec::DEFAULT_MAX_ATTEMPTS);
        }

        #[test]
        fn with_max_attempts_overrides_default() {
            let spec = StaleCommandSpec::new(Duration::minutes(5)).with_max_attempts(3);
            assert_eq!(spec.max_attempts, 3);
        }

        #[test]
        fn criteria_reserved_before_is_in_the_past() {
            use chrono::Utc;
            let spec = StaleCommandSpec::new(Duration::minutes(5));
            let now = DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc);
            let criteria = spec.criteria(now);
            assert_eq!(criteria.len(), 1);
            if let Criterion::ReservedBefore(cutoff) = &criteria[0] {
                assert_eq!(*cutoff, now - Duration::minutes(5));
            } else {
                panic!("first criterion should be ReservedBefore");
            }
        }
    }
}

mod sweeper {
    use super::spec::StaleCommandSpec;
    use crate::application::prelude::CommandError;
    use crate::application::prelude::CommandSweepPort;
    use std::sync::Arc;

    #[derive(Clone)]
    pub struct CommandSweeper {
        port: Arc<dyn CommandSweepPort>,
    }

    impl CommandSweeper {
        pub fn new(port: Arc<dyn CommandSweepPort>) -> Self {
            Self { port }
        }

        pub fn sweep(&self, spec: &StaleCommandSpec) -> Result<u64, CommandError> {
            self.port.sweep(spec)
        }
    }
}
