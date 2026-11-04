pub mod prelude {
    pub use super::ReservableOutboxSpec;
}

/// Parameters for selecting outbox entries eligible for publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReservableOutboxSpec {
    /// Maximum number of entries to reserve in a single call.
    pub limit: usize,
    /// Entries with `attempts >= max_attempts` are excluded from reservation.
    pub max_attempts: i32,
}

impl ReservableOutboxSpec {
    /// Default number of publication attempts before an entry is no longer reservable.
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_uses_default_max_attempts() {
        let spec = ReservableOutboxSpec::new(10);
        assert_eq!(spec.limit, 10);
        assert_eq!(
            spec.max_attempts,
            ReservableOutboxSpec::DEFAULT_MAX_ATTEMPTS
        );
    }

    #[test]
    fn with_max_attempts_overrides_default() {
        let spec = ReservableOutboxSpec::new(5).with_max_attempts(3);
        assert_eq!(spec.limit, 5);
        assert_eq!(spec.max_attempts, 3);
    }
}
