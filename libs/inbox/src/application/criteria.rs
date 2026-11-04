use crate::domain::prelude::InboxStatus;

/// Backend-neutral selection rules produced by specs and translated by each
/// storage backend into its own query language.
pub(crate) enum Criterion {
    StatusIn(Vec<InboxStatus>),
    ScheduledBeforeNow,
    MaxAttempts(i32),
    OrderByScheduledAtAsc,
}
