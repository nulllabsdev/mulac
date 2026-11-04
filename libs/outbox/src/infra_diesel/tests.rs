use super::entity::{NewOutboxEntryRecord, OutboxEntryRecord};
use super::models::MetadataJsonb;
use crate::application::prelude::OutboxEntryEnvelope;
use crate::application::prelude::{
    ExtraInfo,
    NewOutboxEntry,
    OutboxEntryMetadata,
    OutboxStatus,
    //
};
use chrono::Utc;
use uuid::Uuid;

fn metadata(event_id: Uuid) -> OutboxEntryMetadata {
    OutboxEntryMetadata {
        sequence: None,
        event_id,
        message_id: event_id,
        correlation_id: None,
        causation_id: None,
        event_type: "UserRegistered".into(),
        routing_key: "users.registered".into(),
        source: Some("identity-service".into()),
        content_type: Some("application/json".into()),
    }
}

#[test]
fn new_outbox_entry_converts_to_insertable_record() {
    let event_id = Uuid::now_v7();
    let now = Utc::now();
    let entry = NewOutboxEntry {
        id: event_id,
        payload: "{}".into(),
        meta: metadata(event_id),
        scheduled_at: now,
        received_at: now,
    };

    let record = NewOutboxEntryRecord::from(&entry);

    assert_eq!(record.id(), event_id);
    assert_eq!(record.status(), OutboxStatus::Received as i32);
    assert_eq!(record.payload(), "{}");
    assert_eq!(record.meta().event_id, event_id);
    assert_eq!(record.attempts(), 0);
    assert_eq!(record.scheduled_at(), now);
    assert_eq!(record.received_at(), now);
    assert_eq!(record.updated_at(), now);
}

#[test]
fn outbox_record_converts_to_entry_envelope() {
    let event_id = Uuid::now_v7();
    let reservation_id = Uuid::now_v7();
    let now = Utc::now();
    let record = OutboxEntryRecord::new(
        event_id,
        OutboxStatus::Reserved as i32,
        "{}".into(),
        MetadataJsonb(metadata(event_id)),
        now,
        2,
        Some(reservation_id),
        Some(now),
        now,
        now,
        None,
        Some("broker unavailable".into()),
        Some(ExtraInfo::with_error("earlier failure")),
    );

    let envelope = OutboxEntryEnvelope::try_from(record).expect("record converts");

    assert_eq!(envelope.message.id, event_id);
    assert_eq!(envelope.message.status, OutboxStatus::Reserved);
    assert_eq!(envelope.message.attempts, 2);
    assert_eq!(envelope.message.reservation_id, Some(reservation_id));
    assert_eq!(
        envelope.message.last_error.as_deref(),
        Some("broker unavailable")
    );
    assert_eq!(
        envelope.message.extra_info,
        Some(ExtraInfo::with_error("earlier failure"))
    );
    assert_eq!(envelope.metadata.event_id, event_id);
}

#[test]
fn direct_store_rejects_invalid_sequence_before_connecting() {
    use crate::prelude::{DieselPgOutboxRepository, OutboxError, OutboxStorePort};
    use diesel::{
        PgConnection,
        r2d2::{ConnectionManager, Pool},
    };
    // No connection is created unless record() incorrectly reaches the pool.
    let pool = Pool::builder()
        .max_size(1)
        .min_idle(Some(0))
        .connection_timeout(std::time::Duration::from_millis(10))
        .build_unchecked(ConnectionManager::<PgConnection>::new(
            "postgresql://invalid.invalid/test",
        ));
    let repository = DieselPgOutboxRepository::new(pool);
    for sequence in [0, i64::MAX as u64 + 1, u64::MAX] {
        let id = Uuid::now_v7();
        let mut meta = metadata(id);
        meta.sequence = Some(sequence);
        let entry = NewOutboxEntry {
            id,
            meta,
            payload: "{}".into(),
            scheduled_at: Utc::now(),
            received_at: Utc::now(),
        };
        assert!(matches!(
            repository.record(&entry),
            Err(OutboxError::Routing(_))
        ));
    }
}

#[test]
fn supported_sequence_bounds_round_trip() {
    for sequence in [None, Some(1), Some(i64::MAX as u64)] {
        let mut meta = metadata(Uuid::now_v7());
        meta.sequence = sequence;
        assert!(crate::application::prelude::validate_sequence(sequence).is_ok());
        let json = serde_json::to_string(&meta).unwrap();
        assert_eq!(
            serde_json::from_str::<OutboxEntryMetadata>(&json).unwrap(),
            meta
        );
    }
}
