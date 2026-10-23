use commanding::io::ApplicationEvent;
use diesel::{PgConnection, r2d2::ConnectionManager};
use eventing::io::{EventError, EventSubscriberPort, NewEventEnvelope, NewEventMetadata};
use mulac_kernel::io::{
    CommandError, CommandGateway, CommandHandlerPort, DbPool, InboundMessageEnvelope, InboxError,
    InboxStorePort, KernelConfig, KernelError, NewCommand, NewCommandEnvelope, NewCommandMetadata,
    NewInboxMessageEnvelope, boot,
};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct RecordingInboxStore {
    messages: Mutex<Vec<NewInboxMessageEnvelope>>,
}

impl InboxStorePort for RecordingInboxStore {
    fn store(&self, message: NewInboxMessageEnvelope) -> Result<(), InboxError> {
        self.messages.lock().unwrap().push(message);
        Ok(())
    }
}

struct FailingInboxStore;

impl InboxStorePort for FailingInboxStore {
    fn store(&self, _: NewInboxMessageEnvelope) -> Result<(), InboxError> {
        Err(InboxError::Storage("inbox unavailable".into()))
    }
}

#[derive(Default)]
struct RecordingSubscriber {
    events: Mutex<Vec<NewEventEnvelope>>,
}

impl EventSubscriberPort for RecordingSubscriber {
    fn handle(&self, envelope: &NewEventEnvelope) -> Result<(), EventError> {
        self.events.lock().unwrap().push(envelope.clone());
        Ok(())
    }
}

struct FailingSubscriber;

impl EventSubscriberPort for FailingSubscriber {
    fn handle(&self, _: &NewEventEnvelope) -> Result<(), EventError> {
        Err(EventError::SubscriberExecution("outbox unavailable".into()))
    }
}

fn inbound() -> InboundMessageEnvelope {
    InboundMessageEnvelope {
        payload: r#"{"value":42}"#.into(),
        message_id: Some("018f0000-0000-7000-8000-000000000001".parse().unwrap()),
        correlation_id: Some("018f0000-0000-7000-8000-000000000002".parse().unwrap()),
        source: Some("startup-test".into()),
        routing_key: Some("TestCommand".into()),
    }
}

fn event() -> NewEventEnvelope {
    NewEventEnvelope {
        event_type: "TestEvent".into(),
        payload: r#"{"value":42}"#.into(),
        metadata: Some(NewEventMetadata {
            event_id: inbound().message_id.unwrap(),
            correlation_id: None,
            causation_id: None,
            source: None,
        }),
    }
}

fn disconnected_pool() -> DbPool {
    // Startup only assembles adapters. No connections or external services are
    // needed to exercise validation and subscriber factory wiring.
    DbPool::builder()
        .max_size(1)
        .min_idle(Some(0))
        .build_unchecked(ConnectionManager::<PgConnection>::new(
            "postgres://unused:unused@127.0.0.1:1/unused",
        ))
}

#[tokio::test]
async fn direct_start_requires_an_explicit_inbox_store() {
    for config in [
        KernelConfig::default(),
        KernelConfig {
            database_url: Some("postgres://unused".into()),
            ..Default::default()
        },
    ] {
        assert!(matches!(
            boot(config).start().await,
            Err(KernelError::InboxStoreRequired)
        ));
    }
}

#[tokio::test]
async fn direct_inbox_records_the_original_message_in_the_supplied_store() {
    let store = Arc::new(RecordingInboxStore::default());
    let kernel = boot(KernelConfig::default())
        .inbox_store(store.clone())
        .start()
        .await
        .unwrap();
    let message = inbound();

    kernel
        .state()
        .inbox_recorder
        .publish(message.clone().into())
        .unwrap();

    let messages = store.messages.lock().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id(), message.message_id.as_ref().unwrap());
    assert_eq!(messages[0].payload(), message.payload);
    assert_eq!(messages[0].meta.correlation_id, message.correlation_id);
    assert_eq!(messages[0].meta.routing_key, message.routing_key);
    assert_eq!(messages[0].meta.source, message.source);
}

#[tokio::test]
async fn direct_inbox_propagates_storage_failure_instead_of_accepting_the_message() {
    let kernel = boot(KernelConfig::default())
        .inbox_store(Arc::new(FailingInboxStore))
        .start()
        .await
        .unwrap();

    assert!(matches!(
        kernel.state().inbox_recorder.publish(inbound().into()),
        Err(InboxError::Storage(message)) if message == "inbox unavailable"
    ));
}

#[tokio::test]
async fn direct_start_rejects_automatic_outbox_registration() {
    let result = boot(KernelConfig::default())
        .inbox_store(Arc::new(RecordingInboxStore::default()))
        .outbox_subscriber("TestEvent")
        .start()
        .await;

    assert!(matches!(
        result,
        Err(KernelError::OutboxSubscriberUnsupported { event_type }) if event_type == "TestEvent"
    ));
}

#[test]
fn persistent_start_rejects_automatic_outbox_before_invoking_factories() {
    let pool = disconnected_pool();
    let result = boot(KernelConfig::default())
        .outbox_subscriber("TestEvent")
        .event_subscriber_with_command_gateway("TestEvent", "factory", |_| {
            panic!("invalid configuration must fail before running factories")
        })
        .start_persistent(pool.clone(), 1);

    assert!(matches!(
        result,
        Err(KernelError::OutboxSubscriberUnsupported { event_type }) if event_type == "TestEvent"
    ));
    assert_eq!(pool.state().connections, 0);
}

#[tokio::test]
async fn direct_start_rejects_gateway_dependent_subscribers_instead_of_dropping_them() {
    let result = boot(KernelConfig::default())
        .inbox_store(Arc::new(RecordingInboxStore::default()))
        .event_subscriber_with_command_gateway("TestEvent", "factory", |_| {
            panic!("unsupported direct-mode factory must not be called")
        })
        .start()
        .await;

    assert!(matches!(
        result,
        Err(KernelError::CommandGatewaySubscriberRequiresPersistence { event_type, subscriber })
            if event_type == "TestEvent" && subscriber == "factory"
    ));
}

#[test]
fn persistent_start_invokes_factories_with_the_actual_command_gateway() {
    let gateways = Arc::new(Mutex::new(Vec::new()));
    let captured = gateways.clone();
    let pool = disconnected_pool();
    let kernel = boot(KernelConfig::default())
        .event_subscriber_with_command_gateway("TestEvent", "factory", move |gateway| {
            captured.lock().unwrap().push(gateway);
            Arc::new(RecordingSubscriber::default())
        })
        .start_persistent(pool.clone(), 1)
        .unwrap();

    let gateways = gateways.lock().unwrap();
    assert_eq!(gateways.len(), 1);
    assert!(Arc::ptr_eq(&gateways[0], &kernel.state().command_gateway()));
    assert!(matches!(
        gateways[0].as_ref(),
        CommandGateway::TwoPhased { .. }
    ));
    assert_eq!(pool.state().connections, 0);
}

#[test]
fn persistent_start_does_not_silently_ignore_a_direct_mode_inbox_store() {
    let result = boot(KernelConfig::default())
        .inbox_store(Arc::new(RecordingInboxStore::default()))
        .event_subscriber_with_command_gateway("TestEvent", "factory", |_| {
            panic!("invalid configuration must fail before running factories")
        })
        .start_persistent(disconnected_pool(), 1);

    assert!(matches!(
        result,
        Err(KernelError::InboxStoreUnsupportedInPersistentMode)
    ));
}

#[derive(serde::Deserialize, serde::Serialize)]
struct TestMessage {
    value: u32,
}

impl ApplicationEvent for TestMessage {
    fn event_type(&self) -> &'static str {
        "TestEvent"
    }
}

struct EmitEvent;

impl CommandHandlerPort<TestMessage, TestMessage> for EmitEvent {
    fn execute(&self, command: TestMessage) -> Result<Vec<TestMessage>, CommandError> {
        Ok(vec![command])
    }
}

#[tokio::test]
async fn direct_commands_still_execute_and_deliver_events_to_explicit_subscribers() {
    let subscriber = Arc::new(RecordingSubscriber::default());
    let kernel = boot(KernelConfig::default())
        .inbox_store(Arc::new(RecordingInboxStore::default()))
        .command_handler("TestCommand", Arc::new(EmitEvent))
        .event_subscriber("TestEvent", "explicit", subscriber.clone())
        .start()
        .await
        .unwrap();
    let message = inbound();
    kernel
        .state()
        .command_gateway
        .dispatch(NewCommandEnvelope {
            command: NewCommand {
                command_type: "TestCommand".into(),
                payload: message.payload.clone(),
            },
            metadata: Some(NewCommandMetadata {
                command_id: message.message_id.unwrap(),
                correlation_id: message.correlation_id,
                causation_id: None,
                source: message.source,
            }),
        })
        .unwrap();

    let events = subscriber.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "TestEvent");
    assert_eq!(events[0].payload, message.payload);
    let metadata = events[0].metadata.as_ref().unwrap();
    assert_eq!(metadata.causation_id, message.message_id);
    assert_eq!(metadata.correlation_id, message.correlation_id);
}

#[tokio::test]
async fn explicit_subscriber_failure_is_not_reported_as_success() {
    let kernel = boot(KernelConfig::default())
        .inbox_store(Arc::new(RecordingInboxStore::default()))
        .event_subscriber("TestEvent", "real-outbox", Arc::new(FailingSubscriber))
        .start()
        .await
        .unwrap();

    assert!(matches!(
        kernel.state().event_gateway.dispatch(event()),
        Err(EventError::SubscriberExecution(message)) if message == "outbox unavailable"
    ));
}
