use super::prelude::CommandStatus;
use chrono::{DateTime, Duration, Utc};

/// Selection and rescheduling rules for expired command reservations.
///
/// Timeouts do not increment attempts. Rows remain Failed even at the retry limit.
/// The scheduling delay is uncapped; reservation eligibility enforces the limit.
#[derive(Debug, Clone)]
pub struct ReservationTimeoutPolicy {
    pub starting_status: CommandStatus,
    /// Select reservations strictly older than this cutoff.
    pub reserved_before: DateTime<Utc>,
    /// Legacy policy setting retained for compatibility; timeout outcomes are always `Failed`.
    pub ending_status: CommandStatus,
    /// Reservation eligibility, not timeout status, enforces this retry limit.
    pub max_attempts: i32,
    /// Delay per attempt, not the total delay for a command.
    pub scheduled_after: Duration,
}

impl ReservationTimeoutPolicy {
    pub fn new(reserved_before: DateTime<Utc>, max_attempts: i32) -> Self {
        Self {
            starting_status: CommandStatus::Reserved,
            reserved_before,
            ending_status: CommandStatus::Failed,
            max_attempts,
            scheduled_after: Duration::seconds(30),
        }
    }

    pub fn status_after_timeout(&self, _attempts: i32) -> CommandStatus {
        CommandStatus::Failed
    }

    pub fn retry_delay(&self, attempts: i32) -> Duration {
        self.scheduled_after * attempts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_describes_selection_and_result() {
        let cutoff = Utc::now();
        let policy = ReservationTimeoutPolicy::new(cutoff, 6);
        assert_eq!(policy.starting_status, CommandStatus::Reserved);
        assert_eq!(policy.reserved_before, cutoff);
        assert_eq!(policy.ending_status, CommandStatus::Failed);
        assert_eq!(policy.scheduled_after, Duration::seconds(30));
    }

    #[test]
    fn timeout_delay_is_uncapped_and_preserves_attempt_arithmetic() {
        let policy = ReservationTimeoutPolicy::new(Utc::now(), 6);
        for attempts in [i32::MIN, -1, 0, 1, 4, 6, 10, i32::MAX] {
            assert_eq!(
                policy.retry_delay(attempts).num_seconds(),
                i64::from(attempts) * 30
            );
        }
    }

    #[test]
    fn delay_uses_the_policy_setting() {
        let policy = ReservationTimeoutPolicy {
            scheduled_after: Duration::seconds(15),
            ..ReservationTimeoutPolicy::new(Utc::now(), 6)
        };
        assert_eq!(policy.retry_delay(3), Duration::seconds(45));
    }

    #[test]
    fn timeout_keeps_exhausted_attempts_failed_for_inspection() {
        for max_attempts in [6, 3] {
            let policy = ReservationTimeoutPolicy::new(Utc::now(), max_attempts);
            assert_eq!(
                policy.status_after_timeout(max_attempts - 1),
                CommandStatus::Failed
            );
            assert_eq!(
                policy.status_after_timeout(max_attempts),
                CommandStatus::Failed
            );
            assert_eq!(
                policy.status_after_timeout(max_attempts + 1),
                CommandStatus::Failed
            );
        }
    }
}
