#[cfg(any(feature = "diesel", test))]
mod reservation_timeout_policy;
pub mod prelude {
    #[cfg(feature = "diesel")]
    pub(crate) use super::reservation_timeout_policy::ReservationTimeoutPolicy;
    pub use super::{ExtraInfo, InboxStatus};
}
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExtraInfo {
    errors: Vec<String>,
}

impl ExtraInfo {
    pub fn with_errors(errors: Vec<String>) -> Self {
        Self { errors }
    }

    pub fn with_error(error: impl Into<String>) -> Self {
        Self::with_errors(vec![error.into()])
    }

    pub fn add_error(&mut self, error: impl Into<String>) {
        self.errors.push(error.into());
    }

    pub fn errors(&self) -> &[String] {
        &self.errors
    }
}

/// Persistent inbox status codes.
///
/// Values are stored as `Int4` in the database and must remain stable. The gaps
/// at `1`, `3`, and `6` are intentionally reserved for compatibility with
/// existing data and must not be reused without a schema/data migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum InboxStatus {
    /// Stored and awaiting a consumer.
    Received = 0,
    /// Claimed by a consumer; blocked from re-reservation until released.
    Reserved = 2,
    /// Attempt failed; eligible for retry only while below the attempt cap.
    Failed = 4,
    /// Processed successfully.
    Completed = 5,
    /// Legacy/manual terminal status; automatic failures remain `Failed`.
    Dead = 7,
    /// Archived.
    Archive = 8,
}

impl TryFrom<i32> for InboxStatus {
    type Error = UnknownInboxStatus;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Received),
            2 => Ok(Self::Reserved),
            4 => Ok(Self::Failed),
            5 => Ok(Self::Completed),
            7 => Ok(Self::Dead),
            8 => Ok(Self::Archive),
            _ => Err(UnknownInboxStatus(value)),
        }
    }
}

impl InboxStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Reserved => "reserved",
            Self::Failed => "failed",
            Self::Completed => "completed",
            Self::Dead => "dead",
            Self::Archive => "archive",
        }
    }
}

impl From<InboxStatus> for i32 {
    fn from(status: InboxStatus) -> i32 {
        status as i32
    }
}

#[derive(Debug, Error)]
#[error("unknown inbox status: {0}")]
pub struct UnknownInboxStatus(pub i32);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_round_trips_through_i32() {
        let cases = [
            (0, InboxStatus::Received),
            (2, InboxStatus::Reserved),
            (4, InboxStatus::Failed),
            (5, InboxStatus::Completed),
            (7, InboxStatus::Dead),
            (8, InboxStatus::Archive),
        ];
        for (value, expected) in cases {
            let status = InboxStatus::try_from(value).expect("valid status");
            assert_eq!(status, expected);
            assert_eq!(i32::from(status), value);
        }
    }

    #[test]
    fn reserved_gaps_are_unknown() {
        for value in [1, 3, 6] {
            assert!(
                InboxStatus::try_from(value).is_err(),
                "value {value} should be unknown"
            );
        }
    }

    #[test]
    fn arbitrary_unknown_value_returns_error() {
        assert!(InboxStatus::try_from(99).is_err());
        assert!(InboxStatus::try_from(-1).is_err());
    }

    #[test]
    fn with_errors_preserves_order() {
        let extra_info = ExtraInfo::with_errors(vec!["first".into(), "second".into()]);

        assert_eq!(
            extra_info.errors(),
            &["first".to_string(), "second".to_string()]
        );
    }

    #[test]
    fn as_str_matches_variant() {
        assert_eq!(InboxStatus::Received.as_str(), "received");
        assert_eq!(InboxStatus::Reserved.as_str(), "reserved");
        assert_eq!(InboxStatus::Failed.as_str(), "failed");
        assert_eq!(InboxStatus::Completed.as_str(), "completed");
        assert_eq!(InboxStatus::Dead.as_str(), "dead");
        assert_eq!(InboxStatus::Archive.as_str(), "archive");
    }
}
