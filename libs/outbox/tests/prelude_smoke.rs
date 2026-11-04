use std::sync::Arc;

use chrono::Duration;
use mulac_outbox::prelude::*;
use uuid::Uuid;

struct NoopStore;

impl OutboxStorePort for NoopStore {
    fn record(&self, _entry: &NewOutboxEntry) -> Result<(), OutboxError> {
        Ok(())
    }
}

struct NoopReserve;

impl OutboxReservePort for NoopReserve {
    fn reserve(
        &self,
        _spec: &ReservableOutboxSpec,
    ) -> Result<Vec<OutboxEntryEnvelope>, OutboxError> {
        Ok(vec![])
    }
}

struct NoopProcess;

impl OutboxProcessPort for NoopProcess {
    fn completed(&self, _id: Uuid, _reservation_id: Uuid) -> Result<(), OutboxError> {
        Ok(())
    }

    fn failed(
        &self,
        _id: Uuid,
        _reservation_id: Uuid,
        _max_attempts: i32,
        _reason: Option<String>,
    ) -> Result<(), OutboxError> {
        Ok(())
    }

    fn dead(
        &self,
        _id: Uuid,
        _reservation_id: Uuid,
        _reason: Option<String>,
    ) -> Result<(), OutboxError> {
        Ok(())
    }
}

struct NoopSweep;

impl OutboxSweepPort for NoopSweep {
    fn sweep(&self, _spec: &StaleReservationSpec) -> Result<u64, OutboxError> {
        Ok(0)
    }
}

struct NoopPublisher;

impl OutboxPublisherPort for NoopPublisher {
    fn publish(&self, _envelope: OutboundMessageEnvelope) -> Result<(), OutboxError> {
        Ok(())
    }
}

#[test]
fn io_facade_reexports_core_outbox_api() {
    let event_id = Uuid::now_v7();
    let metadata = NewOutboxMetadata {
        sequence: None,
        event_id,
        message_id: None,
        correlation_id: None,
        causation_id: None,
        event_type: "UserRegistered".into(),
        routing_key: "users.registered".into(),
        source: Some("identity-service".into()),
        content_type: Some("application/json".into()),
    };

    let envelope = NewOutboxEnvelope {
        payload: "{}".into(),
        metadata,
    };

    let recorder_repo = Arc::new(NoopStore);
    let recorder = OutboxRecorder::new(recorder_repo);
    recorder.record(&envelope).expect("record succeeds");

    let consumer_repo = Arc::new(TestRepository {
        reserve: Arc::new(NoopReserve),
        process: Arc::new(NoopProcess),
    });
    let consumer = OutboxConsumer::new(consumer_repo, Arc::new(NoopPublisher));
    consumer
        .publish_batch(&ReservableOutboxSpec::new(10))
        .expect("empty batch succeeds");

    let sweeper = ReservationSweeper::new(Arc::new(NoopSweep));
    assert_eq!(
        sweeper
            .sweep(&StaleReservationSpec::new(Duration::minutes(5)))
            .expect("sweep succeeds"),
        0
    );

    let _status = OutboxStatus::Received;
    let _error = OutboxError::Storage("boom".into());
}

#[cfg(feature = "diesel")]
#[test]
fn io_facade_reexports_diesel_scaffold() {
    let _pool_type: Option<DbPool> = None;
    let _constructor: fn(DbPool) -> DieselPgOutboxRepository = DieselPgOutboxRepository::new;
    let _builder: fn(&str) -> Result<DbPool, diesel::r2d2::PoolError> = build_pool;
}

#[cfg(feature = "amqp")]
#[test]
fn io_facade_reexports_amqp_scaffold() {
    let _ctor: fn(lapin::Channel, AmqpPublishConfig) -> AmqpPublisher = AmqpPublisher::new;
    let config = AmqpPublishConfig::default();

    assert_eq!(config.exchange, "");
    assert_eq!(config.default_content_type, "application/json");
}

struct TestRepository {
    reserve: Arc<dyn OutboxReservePort>,
    process: Arc<dyn OutboxProcessPort>,
}
impl OutboxReservePort for TestRepository {
    fn reserve(&self, s: &ReservableOutboxSpec) -> Result<Vec<OutboxEntryEnvelope>, OutboxError> {
        self.reserve.reserve(s)
    }
}
impl OutboxProcessPort for TestRepository {
    fn completed(&self, id: Uuid, r: Uuid) -> Result<(), OutboxError> {
        self.process.completed(id, r)
    }
    fn failed(&self, id: Uuid, r: Uuid, m: i32, e: Option<String>) -> Result<(), OutboxError> {
        self.process.failed(id, r, m, e)
    }
    fn dead(&self, id: Uuid, r: Uuid, e: Option<String>) -> Result<(), OutboxError> {
        self.process.dead(id, r, e)
    }
}
