use super::prelude::OutboxStatus;
use chrono::{DateTime, Duration, Utc};
/// Pure timeout outcome rules. Consumers and sweepers must use the same retry limit.
pub(crate) struct ReservationTimeoutPolicy {
    pub(crate) reserved_before: DateTime<Utc>,
    max_attempts: i32,
}
impl ReservationTimeoutPolicy {
    pub(crate) fn new(reserved_before: DateTime<Utc>, max_attempts: i32) -> Self {
        Self {
            reserved_before,
            max_attempts,
        }
    }
    pub(crate) fn outcome(
        &self,
        attempts: i32,
        now: DateTime<Utc>,
        previous: DateTime<Utc>,
    ) -> (OutboxStatus, DateTime<Utc>) {
        let exhausted = attempts >= self.max_attempts;
        let status = OutboxStatus::Failed;
        let delay = (i64::from(attempts.max(1)) * 30).min(120);
        let scheduled = if exhausted {
            previous
        } else {
            now + Duration::seconds(delay)
        };
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
                assert_eq!(status, OutboxStatus::Failed);
                assert_eq!(
                    scheduled,
                    if attempts >= limit {
                        old
                    } else {
                        now + Duration::seconds((i64::from(attempts.max(1)) * 30).min(120))
                    }
                );
            }
        }
    }
}
