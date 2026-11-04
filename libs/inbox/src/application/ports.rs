//! Storage and transport ports. Core inbox code depends only on these traits;
//! backends (e.g. `infra_diesel`, the AMQP transport) implement them.
pub mod prelude {
    pub use super::{
        AcknowledgeHandle,
        InboxProcessPort,
        InboxReservePort,
        InboxStorePort,
        InboxSweepPort,
        InboxTransportFuture,
        InboxTransportPort,
        InboxTransportResult,
        //
    };
}

use crate::application::prelude::StaleReservationSpec;
use crate::application::prelude::{
    InboundMessageEnvelope,
    InboxError,
    InboxMessageEnvelope,
    NewInboxMessageEnvelope,
    //
};
use crate::execution::prelude::ReservableInboxSpec;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

/// Persists newly received messages.
pub trait InboxStorePort: Send + Sync {
    fn store(&self, msg: NewInboxMessageEnvelope) -> Result<(), InboxError>;
}

/// Reserves messages eligible for processing.
///
/// Reserve and validate candidates atomically. Persist malformed candidates as
/// `Failed` and return only healthy entries after commit; loading/failure-write
/// errors roll back the reservation. The limit bounds candidates, so a shorter
/// or empty result does not necessarily mean the queue was empty.
pub trait InboxReservePort: Send + Sync {
    fn reserve(&self, spec: &ReservableInboxSpec) -> Result<Vec<InboxMessageEnvelope>, InboxError>;
}

/// Records the outcome of processing a reserved message.
///
/// Finalization must require the matching ID, reservation token, and `Reserved`
/// status. `failed` records `Failed`, including exhausted attempts, and clears
/// reservation fields; reservation eligibility enforces the attempt cap.
pub trait InboxProcessPort: Send + Sync {
    fn completed(&self, id: Uuid, reservation_id: Uuid) -> Result<(), InboxError>;
    fn failed(
        &self,
        id: Uuid,
        reservation_id: Uuid,
        max_attempts: i32,
        reason: Option<String>,
    ) -> Result<(), InboxError>;
}

/// Output port for releasing stale reservations back into the processable pool.
///
/// Persist `Failed` even for exhausted attempts and clear reservation fields.
/// Applications must schedule this recovery; consumers cannot recover from
/// process termination or unavailable failure persistence on their own.
pub trait InboxSweepPort: Send + Sync {
    /// Find all messages with status `Reserved` whose `reserved_at` is older
    /// than `spec.timeout` and transition them to `Failed`, including exhausted
    /// attempts, clearing the reservation fields and
    /// scheduling the next retry according to the standard backoff policy.
    ///
    /// Returns the number of messages affected.
    fn sweep(&self, spec: &StaleReservationSpec) -> Result<usize, InboxError>;
}

/// Application-owned acknowledgement boundary for inbound delivery handles.
///
/// The application layer owns this trait because recording an inbound message is
/// an application use case: the worker must acknowledge the external delivery
/// only after the store accepts the application envelope. Concrete protocol
/// mechanics, such as AMQP `basic.ack`/`basic.nack`, stay in infra/adapters.
pub trait AcknowledgeHandle: Send {
    fn ack(self: Box<Self>) -> Pin<Box<dyn Future<Output = Result<(), InboxError>> + Send>>;
    fn nack(self: Box<Self>) -> Pin<Box<dyn Future<Output = Result<(), InboxError>> + Send>>;
}

pub type InboxTransportResult =
    Result<Option<(InboundMessageEnvelope, Box<dyn AcknowledgeHandle>)>, InboxError>;

pub type InboxTransportFuture<'a> = Pin<Box<dyn Future<Output = InboxTransportResult> + Send + 'a>>;

/// Application input port for polling inbound messages from an adapter.
///
/// Runtime concerns such as sleeping, cancellation, reconnecting, and concrete
/// broker clients remain outside application. This port only describes the
/// application contract: receive the next inbound envelope with a handle that
/// can acknowledge the external delivery after the use case succeeds or fails.
pub trait InboxTransportPort {
    fn next(&mut self) -> InboxTransportFuture<'_>;
}
