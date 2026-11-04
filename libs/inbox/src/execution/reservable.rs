pub mod prelude {
    pub use super::ReservableInboxSpec;
}

#[cfg(feature = "diesel")]
use crate::application::prelude::{Criterion, InboxStatus};

/// Parameters for selecting inbox messages eligible for consumption.
///
/// Passed to [`InboxReservePort::reserve`] to express how many messages to
/// claim and how many prior failures are still acceptable. Build with
/// [`ReservableInboxSpec::new`] and customise via [`with_max_attempts`].
///
/// [`with_max_attempts`]: ReservableInboxSpec::with_max_attempts
pub struct ReservableInboxSpec {
    /// Maximum number of messages to reserve in a single call.
    pub limit: usize,
    /// Messages with `attempts >= max_attempts` are excluded from reservation.
    pub max_attempts: i32,
}

impl ReservableInboxSpec {
    /// Default reservation attempt cap; exhausted failures stay `Failed`.
    pub const DEFAULT_MAX_ATTEMPTS: i32 = 6;

    /// Create a spec with the given limit and [`DEFAULT_MAX_ATTEMPTS`].
    ///
    /// [`DEFAULT_MAX_ATTEMPTS`]: Self::DEFAULT_MAX_ATTEMPTS
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            max_attempts: Self::DEFAULT_MAX_ATTEMPTS,
        }
    }

    /// Override the maximum number of attempts.
    pub fn with_max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Build the ordered list of query criteria used by the storage adapter.
    ///
    /// Always returns, in order:
    /// 1. `StatusIn([Received, Failed])` — both statuses are eligible
    /// 2. `ScheduledBeforeNow` — only messages past their scheduled time
    /// 3. `MaxAttempts(n)` — exclude messages at the retry limit
    /// 4. `OrderByScheduledAtAsc` — oldest-first processing
    #[cfg(feature = "diesel")]
    pub(crate) fn criteria(&self) -> Vec<Criterion> {
        vec![
            Criterion::StatusIn(vec![InboxStatus::Received, InboxStatus::Failed]),
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
        let spec = ReservableInboxSpec::new(10);
        assert_eq!(spec.limit, 10);
        assert_eq!(spec.max_attempts, ReservableInboxSpec::DEFAULT_MAX_ATTEMPTS);
    }

    #[test]
    fn with_max_attempts_overrides_default() {
        let spec = ReservableInboxSpec::new(5).with_max_attempts(3);
        assert_eq!(spec.limit, 5);
        assert_eq!(spec.max_attempts, 3);
    }

    #[cfg(feature = "diesel")]
    #[test]
    fn criteria_returns_four_entries_in_order() {
        let spec = ReservableInboxSpec::new(10);
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
        let spec = ReservableInboxSpec::new(1).with_max_attempts(3);
        let criteria = spec.criteria();
        assert!(matches!(criteria[2], Criterion::MaxAttempts(3)));
    }

    #[cfg(feature = "diesel")]
    #[test]
    fn criteria_status_in_includes_received_and_failed() {
        let spec = ReservableInboxSpec::new(1);
        let criteria = spec.criteria();
        if let Criterion::StatusIn(statuses) = &criteria[0] {
            assert!(statuses.contains(&InboxStatus::Received));
            assert!(statuses.contains(&InboxStatus::Failed));
            assert_eq!(statuses.len(), 2);
        } else {
            panic!("first criterion should be StatusIn");
        }
    }
}
