pub mod prelude {
    pub use super::ReservableEventSpec;
}

#[cfg(feature = "diesel")]
use crate::application::prelude::Criterion;
#[cfg(feature = "diesel")]
use crate::domain::prelude::EventStatus;

/// Parameters for selecting event entries eligible for consumption.
pub struct ReservableEventSpec {
    /// Maximum number of entries to reserve in a single call.
    pub limit: usize,
    /// Entries with `attempts >= max_attempts` are excluded from reservation.
    pub max_attempts: i32,
}

impl ReservableEventSpec {
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

    #[cfg(feature = "diesel")]
    pub(crate) fn criteria(&self) -> Vec<Criterion> {
        vec![
            Criterion::StatusIn(vec![EventStatus::Received, EventStatus::Failed]),
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
        let spec = ReservableEventSpec::new(10);
        assert_eq!(spec.limit, 10);
        assert_eq!(spec.max_attempts, ReservableEventSpec::DEFAULT_MAX_ATTEMPTS);
    }

    #[test]
    fn with_max_attempts_overrides_default() {
        let spec = ReservableEventSpec::new(5).with_max_attempts(3);
        assert_eq!(spec.limit, 5);
        assert_eq!(spec.max_attempts, 3);
    }

    #[cfg(feature = "diesel")]
    #[test]
    fn criteria_returns_four_entries_in_order() {
        let spec = ReservableEventSpec::new(10);
        let criteria = spec.criteria();
        assert_eq!(criteria.len(), 4);
        assert!(matches!(criteria[0], Criterion::StatusIn(_)));
        assert!(matches!(criteria[1], Criterion::ScheduledBeforeNow));
        assert!(matches!(criteria[2], Criterion::MaxAttempts(6)));
        assert!(matches!(criteria[3], Criterion::OrderByScheduledAtAsc));
    }

    #[cfg(feature = "diesel")]
    #[test]
    fn criteria_max_attempts_reflects_custom_value() {
        let spec = ReservableEventSpec::new(1).with_max_attempts(3);
        let criteria = spec.criteria();
        assert!(matches!(criteria[2], Criterion::MaxAttempts(3)));
    }

    #[cfg(feature = "diesel")]
    #[test]
    fn criteria_status_in_includes_received_and_failed() {
        let spec = ReservableEventSpec::new(1);
        let criteria = spec.criteria();
        if let Criterion::StatusIn(statuses) = &criteria[0] {
            assert!(statuses.contains(&EventStatus::Received));
            assert!(statuses.contains(&EventStatus::Failed));
            assert_eq!(statuses.len(), 2);
        } else {
            panic!("first criterion should be StatusIn");
        }
    }
}
