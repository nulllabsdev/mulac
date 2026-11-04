//! Storage and publisher ports. Core outbox code depends only on these traits;
//! backends (e.g. `infra_diesel`, the AMQP publishers) implement them.
pub mod prelude {
    pub use super::{
        OutboxProcessPort,
        OutboxPublisherPort,
        OutboxReservePort,
        OutboxStorePort,
        OutboxSweepPort,
        //
    };
}

use crate::application::prelude::StaleReservationSpec;
use crate::application::prelude::{
    NewOutboxEntry,
    OutboundMessageEnvelope,
    OutboxEntryEnvelope,
    OutboxError,
    //
};
use crate::execution::prelude::ReservableOutboxSpec;
use uuid::Uuid;

/// Persists newly recorded entries. Validate sequence bounds before writing;
/// direct callers must not bypass metadata validation performed by the recorder.
pub trait OutboxStorePort: Send + Sync {
    fn record(&self, entry: &NewOutboxEntry) -> Result<(), OutboxError>;
}

/// Reserves entries eligible for publication.
///
/// Reserve and validate candidates atomically. Persist malformed candidates as
/// `Failed` and return only healthy entries after commit; loading/failure-write
/// errors roll back the reservation. The limit bounds candidates, so a shorter
/// or empty result does not necessarily mean the queue was empty.
pub trait OutboxReservePort: Send + Sync {
    fn reserve(&self, spec: &ReservableOutboxSpec)
    -> Result<Vec<OutboxEntryEnvelope>, OutboxError>;
}

/// Sends one reserved entry to the broker.
pub trait OutboxPublisherPort: Send + Sync {
    fn publish(&self, envelope: OutboundMessageEnvelope) -> Result<(), OutboxError>;
}

/// Releases reservations that exceeded their timeout.
///
/// Persist `Failed` even for exhausted attempts and clear reservation fields.
/// Applications must schedule this recovery; consumers cannot recover from
/// process termination or unavailable failure persistence on their own.
pub trait OutboxSweepPort: Send + Sync {
    fn sweep(&self, spec: &StaleReservationSpec) -> Result<u64, OutboxError>;
}

/// Records the outcome of publishing a reserved entry.
///
/// Finalization must require the matching ID, reservation token, and `Reserved`
/// status. `failed` records `Failed`, including exhausted attempts, and clears
/// reservation fields; reservation eligibility enforces the attempt cap.
pub trait OutboxProcessPort: Send + Sync {
    fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), OutboxError>;

    fn failed(
        &self,
        id: Uuid,
        reservation_id: Uuid,
        max_attempts: i32,
        reason: Option<String>,
    ) -> Result<(), OutboxError>;

    /// Explicit administrative transition; never used by automatic batch processing.
    fn dead(
        &self,
        id: Uuid,
        reservation_id: Uuid,
        reason: Option<String>,
    ) -> Result<(), OutboxError>;
}
