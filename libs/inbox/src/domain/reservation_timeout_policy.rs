use super::prelude::InboxStatus;
use chrono::{DateTime, Duration, Utc};
/// Pure timeout outcome rules. Consumers and sweepers must use the same retry limit.
pub(crate) struct ReservationTimeoutPolicy {
    pub(crate) reserved_before: DateTime<Utc>,
}
impl ReservationTimeoutPolicy {
    pub(crate) fn new(reserved_before: DateTime<Utc>, _max_attempts: i32) -> Self {
        Self { reserved_before }
    }
    pub(crate) fn outcome(
        &self,
        attempts: i32,
        now: DateTime<Utc>,
        previous: DateTime<Utc>,
    ) -> (InboxStatus, DateTime<Utc>) {
        let status = InboxStatus::Failed;
        let _ = previous;
        let scheduled = now + Duration::seconds(i64::from(attempts) * 30);
        (status, scheduled)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn outcomes_preserve_retry_rules() {
        let now = Utc::now();
        let old = now - Duration::hours(1);
        for limit in [6, 3] {
            let policy = ReservationTimeoutPolicy::new(now, limit);
            for attempts in [0, 1, limit - 1, limit, limit + 1, 10] {
                let (status, scheduled) = policy.outcome(attempts, now, old);
                assert_eq!(status, InboxStatus::Failed);
                assert_eq!(scheduled, now + Duration::seconds(i64::from(attempts) * 30));
            }
        }
    }
}
