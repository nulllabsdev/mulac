use crate::domain::prelude::EventStatus;

/// Backend-neutral selection rules produced by specs and translated by each
/// storage backend into its own query language.
pub(crate) enum Criterion {
    StatusIn(Vec<EventStatus>),
    ScheduledBeforeNow,
    MaxAttempts(i32),
    OrderByScheduledAtAsc,
}
