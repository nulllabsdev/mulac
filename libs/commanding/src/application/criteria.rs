use crate::domain::prelude::CommandStatus;
use chrono::{DateTime, Utc};

/// Backend-neutral selection rules produced by specs and translated by each
/// storage backend into its own query language.
#[derive(Debug, Clone, PartialEq)]
pub enum Criterion {
    StatusIn(Vec<CommandStatus>),
    ScheduledBeforeNow,
    MaxAttempts(i32),
    ReservedBefore(DateTime<Utc>),
    OrderByScheduledAtAsc,
}
