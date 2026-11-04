//! Storage and dispatch ports. Core eventing code depends only on these traits;
//! backends (e.g. `infra_diesel`) implement them.
pub mod prelude {
    pub use super::{
        EventDispatchPort,
        EventProcessPort,
        EventReservePort,
        EventStorePort,
        EventSweepPort,
        //
    };
}

use crate::application::prelude::StaleEventSpec;
use crate::application::prelude::{EventEnvelope, EventError, NewEventEnvelope};
use crate::execution::prelude::ReservableEventSpec;
use uuid::Uuid;

/// Dispatches events produced by command handlers. The event gateway implements
/// this port; commanding depends on it, and eventing does not depend on commanding.
pub trait EventDispatchPort: Send + Sync {
    fn dispatch(&self, event: NewEventEnvelope) -> Result<(), EventError>;
}

/// Persists newly received events.
pub trait EventStorePort: Send + Sync {
    fn record(&self, envelope: &NewEventEnvelope) -> Result<(), EventError>;
}

/// Reserves events eligible for processing.
///
/// Reserve and validate candidates atomically. Persist malformed candidates as
/// `Failed` and return only healthy entries after commit; loading/failure-write
/// errors roll back the reservation. The limit bounds candidates, so a shorter
/// or empty result does not necessarily mean the queue was empty.
pub trait EventReservePort: Send + Sync {
    fn reserve(&self, spec: &ReservableEventSpec) -> Result<Vec<EventEnvelope>, EventError>;
}

/// Records the outcome of processing a reserved event.
///
/// Finalization must require the matching ID, reservation token, and `Reserved`
/// status. `failed` records `Failed`, including exhausted attempts, and clears
/// reservation fields; reservation eligibility enforces the attempt cap.
pub trait EventProcessPort: Send + Sync {
    fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), EventError>;

    fn failed(
        &self,
        id: Uuid,
        reservation_id: Uuid,
        max_attempts: i32,
        reason: Option<String>,
    ) -> Result<(), EventError>;
}

/// Releases reservations that exceeded their timeout.
///
/// Persist `Failed` even for exhausted attempts and clear reservation fields.
/// Applications must schedule this recovery; consumers cannot recover from
/// process termination or unavailable failure persistence on their own.
pub trait EventSweepPort: Send + Sync {
    fn sweep(&self, spec: &StaleEventSpec) -> Result<u64, EventError>;
}
