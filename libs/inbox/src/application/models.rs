pub mod prelude {
    pub(crate) use super::InboxMessage;
    pub use super::models::{
        InboundMessageEnvelope,
        InboxError,
        InboxMessageEnvelope,
        InboxMessageMetadata,
        NewInboxMessageEnvelope,
        //
    };
    pub(crate) use super::models::{NewInboxMessage, NewInboxMessageMetadata};
}

mod models {
    use super::InboxMessage;
    use crate::domain::prelude::InboxStatus;
    use uuid::Uuid;

    #[derive(Debug, Clone, Default)]
    pub struct InboundMessageEnvelope {
        pub payload: String,
        pub message_id: Option<Uuid>,
        pub correlation_id: Option<Uuid>,
        pub source: Option<String>,
        pub routing_key: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub struct InboxMessageMetadata {
        pub message_id: Option<Uuid>,
        pub correlation_id: Option<Uuid>,
        pub source: Option<String>,
        pub routing_key: Option<String>,
    }

    #[derive(Debug)]
    pub struct InboxMessageEnvelope {
        pub msg: InboxMessage,
        pub meta: InboxMessageMetadata,
    }

    impl InboxMessageEnvelope {
        pub fn id(&self) -> &Uuid {
            &self.msg.id
        }

        pub fn payload(&self) -> &str {
            &self.msg.payload
        }

        pub fn status(&self) -> InboxStatus {
            self.msg.status
        }

        pub fn reservation_id(&self) -> Option<Uuid> {
            self.msg.reservation_id
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum InboxError {
        #[error("storage error: {0}")]
        Storage(String),
        #[error("transport error: {0}")]
        Transport(String),
        #[error("acknowledgement error: {0}")]
        Acknowledgement(String),
        #[error("recording error: {0}")]
        Recording(String),
        #[error("inbox entry {id} is not reserved by reservation {reservation_id}")]
        ReservationNotOwned { id: Uuid, reservation_id: Uuid },
        #[error("Publish failed: {0}")]
        PublishFailed(String),
        #[error("Missing reservation for message {id}")]
        MissingReservation { id: Uuid },
        #[error("conversion error: {0}")]
        Conversion(String),
    }

    /// Raw payload of a new inbound message before it is stored.
    #[derive(Debug)]
    pub struct NewInboxMessage {
        pub payload: String,
    }

    /// Write-side envelope wrapping a new message and its routing metadata.
    ///
    /// Constructed from an [`InboundMessageEnvelope`] arriving from a transport
    /// adapter (e.g. AMQP). If the transport did not supply a `message_id`, one
    /// is generated as a time-ordered UUID v7 to ensure idempotent inserts.
    pub struct NewInboxMessageEnvelope {
        pub msg: NewInboxMessage,
        pub meta: NewInboxMessageMetadata,
    }

    /// Metadata carried with a new inbound message.
    ///
    /// `message_id` is always present — either forwarded from the transport or
    /// generated at the point of conversion.
    #[derive(Debug, Clone)]
    pub struct NewInboxMessageMetadata {
        pub message_id: Uuid,
        pub correlation_id: Option<Uuid>,
        pub source: Option<String>,
        pub routing_key: Option<String>,
    }

    impl NewInboxMessageEnvelope {
        pub fn id(&self) -> &Uuid {
            &self.meta.message_id
        }

        pub fn payload(&self) -> &str {
            &self.msg.payload
        }
    }
}

use crate::domain::prelude::{ExtraInfo, InboxStatus};
use chrono::{DateTime, Utc};
use uuid::Uuid;
/// A stored inbox message as it exists in the database.
///
/// Carries the full lifecycle state — status, retry counter, reservation
/// ownership — so consumers can make processing decisions without additional
/// queries.
#[derive(Debug)]
pub struct InboxMessage {
    pub id: Uuid,
    pub payload: String,
    pub status: InboxStatus,
    /// Next time this message becomes eligible for reservation.
    /// Advances on each failed attempt according to the retry backoff policy.
    pub scheduled_at: DateTime<Utc>,
    /// Number of times this message has been reserved for processing.
    ///
    /// Incremented on every reservation, including reservations that did not
    /// result in an explicit `completed` or `failed` call (e.g. worker crash
    /// or shutdown). At the configured limit no more automatic reservations are
    /// permitted. Failed outcomes remain `Failed` for inspection regardless of
    /// how many reservations actually ran to completion.
    pub attempts: i32,
    /// Identifies the current reservation holder. `None` when not reserved.
    pub reservation_id: Option<Uuid>,
    pub reserved_at: Option<DateTime<Utc>>,
    pub extra_info: Option<ExtraInfo>,
}
