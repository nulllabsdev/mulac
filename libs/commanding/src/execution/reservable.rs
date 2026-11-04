pub mod prelude {
    pub use super::ReservableCommandSpec;
}

use crate::application::prelude::Criterion;
use crate::domain::prelude::CommandStatus;

/// Parameters for selecting command entries eligible for consumption.
pub struct ReservableCommandSpec {
    /// Maximum number of entries to reserve in a single call.
    pub limit: usize,
    /// Entries with `attempts >= max_attempts` are excluded from reservation.
    pub max_attempts: i32,
}

impl ReservableCommandSpec {
    pub const DEFAULT_MAX_ATTEMPTS: i32 = 6;

    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            max_attempts: Self::DEFAULT_MAX_ATTEMPTS,
        }
    }

    pub fn with_max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    pub fn criteria(&self) -> Vec<Criterion> {
        vec![
            Criterion::StatusIn(vec![CommandStatus::Received, CommandStatus::Failed]),
            Criterion::ScheduledBeforeNow,
            Criterion::MaxAttempts(self.max_attempts),
            Criterion::OrderByScheduledAtAsc,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_uses_default_max_attempts() {
        let spec = ReservableCommandSpec::new(10);
        assert_eq!(spec.limit, 10);
        assert_eq!(
            spec.max_attempts,
            ReservableCommandSpec::DEFAULT_MAX_ATTEMPTS
        );
    }

    #[test]
    fn with_max_attempts_overrides_default() {
        let spec = ReservableCommandSpec::new(5).with_max_attempts(3);
        assert_eq!(spec.limit, 5);
        assert_eq!(spec.max_attempts, 3);
    }

    #[test]
    fn criteria_returns_four_entries_in_order() {
        let spec = ReservableCommandSpec::new(10);
        let criteria = spec.criteria();
        assert_eq!(criteria.len(), 4);
        assert!(matches!(criteria[0], Criterion::StatusIn(_)));
        assert!(matches!(criteria[1], Criterion::ScheduledBeforeNow));
        assert!(matches!(criteria[2], Criterion::MaxAttempts(6)));
        assert!(matches!(criteria[3], Criterion::OrderByScheduledAtAsc));
    }

    #[test]
    fn criteria_max_attempts_reflects_custom_value() {
        let spec = ReservableCommandSpec::new(1).with_max_attempts(3);
        let criteria = spec.criteria();
        assert!(matches!(criteria[2], Criterion::MaxAttempts(3)));
    }

    #[test]
    fn criteria_status_in_includes_received_and_failed() {
        let spec = ReservableCommandSpec::new(1);
        let criteria = spec.criteria();
        if let Criterion::StatusIn(statuses) = &criteria[0] {
            assert!(statuses.contains(&CommandStatus::Received));
            assert!(statuses.contains(&CommandStatus::Failed));
            assert_eq!(statuses.len(), 2);
        } else {
            panic!("first criterion should be StatusIn");
        }
    }
}
