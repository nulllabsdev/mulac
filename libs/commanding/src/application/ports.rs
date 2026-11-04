//! Storage ports. Core commanding code depends only on these traits;
//! backends (e.g. `infra_diesel`) implement them.
pub mod prelude {
    pub use super::{
        CommandProcessPort,
        CommandReservePort,
        CommandStorePort,
        CommandSweepPort,
        //
    };
}

use crate::application::prelude::StaleCommandSpec;
use crate::application::prelude::{CommandEnvelope, CommandError, NewCommandEnvelope};
use crate::execution::prelude::ReservableCommandSpec;
use uuid::Uuid;

#[rustfmt::skip]
/// Persists newly received commands.
pub trait CommandStorePort: Send + Sync {
    fn record(&self, envelope: &NewCommandEnvelope) -> Result<(), CommandError>;
}

#[rustfmt::skip]
/// Reserves commands eligible for processing.
///
/// Reserve and validate candidates atomically. Persist malformed candidates as
/// `Failed` and return only healthy entries after commit; loading/failure-write
/// errors roll back the reservation. The limit bounds candidates, so a shorter
/// or empty result does not necessarily mean the queue was empty.
pub trait CommandReservePort: Send + Sync {
    fn reserve(&self, spec: &ReservableCommandSpec) -> Result<Vec<CommandEnvelope>, CommandError>;
}

#[rustfmt::skip]
/// Records the outcome of processing a reserved command.
///
/// Finalization must require the matching ID, reservation token, and `Reserved`
/// status. `failed` records `Failed`, including exhausted attempts, and clears
/// reservation fields; reservation eligibility enforces the attempt cap.
pub trait CommandProcessPort: Send + Sync {
    fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), CommandError>;

    fn failed(&self, id: Uuid, reservation_id: Uuid, max_attempts: i32, reason: Option<String>) -> Result<(), CommandError>;
}

#[rustfmt::skip]
/// Releases reservations that exceeded their timeout.
///
/// Persist `Failed` even for exhausted attempts and clear reservation fields.
/// Applications must schedule this recovery; consumers cannot recover from
/// process termination or unavailable failure persistence on their own.
pub trait CommandSweepPort: Send + Sync {
    fn sweep(&self, spec: &StaleCommandSpec) -> Result<u64, CommandError>;
}
