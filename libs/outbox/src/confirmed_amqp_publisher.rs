//! Strict AMQP publication: mandatory routing, required publisher confirms,
//! persistent delivery, a bounded wait and a reused connection that is
//! dropped after any failure.

pub mod io {
    pub use super::ConfirmedAmqpPublisher;
}

use std::time::Duration;

use crate::assembly::io::{OutboundMessageEnvelope, OutboxError, OutboxPublisherPort};
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions};
use lapin::types::{AMQPValue, FieldTable, LongString};
use lapin::{BasicProperties, Channel, Confirmation, Connection, ConnectionProperties};
use tokio::sync::Mutex;

const PERSISTENT_DELIVERY_MODE: u8 = 2;
const DEFAULT_CONTENT_TYPE: &str = "application/json";

/// Publishes outbox messages and succeeds only when the broker confirms that
/// it routed them.
///
/// The connection is opened on the first publish, reused while publishing
/// succeeds and dropped after any failure, so the next publish reconnects.
/// Each publish, including the connection attempt, is bounded by
/// [`ConfirmedAmqpPublisher::timeout`].
///
/// [`OutboxPublisherPort::publish`] never creates a runtime: it requires a
/// current Tokio runtime handle and must run on that runtime's blocking pool,
/// for example through `spawn_blocking`.
///
/// The URL is not validated here; lapin parses it when connecting. Error
/// messages are fixed and carry no broker diagnostics.
pub struct ConfirmedAmqpPublisher {
    url: String,
    exchange: String,
    timeout: Duration,
    connection: Mutex<Option<(Connection, Channel)>>,
}

impl ConfirmedAmqpPublisher {
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

    /// Creates a publisher for `exchange` (empty for the default exchange)
    /// without connecting.
    pub fn new(url: impl Into<String>, exchange: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            exchange: exchange.into(),
            timeout: Self::DEFAULT_TIMEOUT,
            connection: Mutex::new(None),
        }
    }

    /// Replaces the default 10-second bound on connecting and publishing.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub async fn publish_async(
        &self,
        envelope: OutboundMessageEnvelope,
    ) -> Result<(), OutboxError> {
        let mut connection = self.connection.lock().await;
        let result = tokio::time::timeout(self.timeout, async {
            if connection.is_none() {
                *connection = Some(self.connect().await?);
            }
            let (_, channel) = connection.as_ref().expect("connection initialized");
            let confirmation = channel
                .basic_publish(
                    self.exchange.clone().into(),
                    envelope.metadata.routing_key.clone().into(),
                    BasicPublishOptions {
                        mandatory: true,
                        ..Default::default()
                    },
                    &envelope.payload,
                    properties_for(&envelope),
                )
                .await
                .map_err(|_| err_transport())?
                .await
                .map_err(|_| err_transport())?;
            require_confirmation(confirmation)
        })
        .await
        .unwrap_or_else(|_| Err(err_timeout()));
        // A fresh connection on retry also recovers from closed channels and uncertain confirms.
        if result.is_err() {
            *connection = None;
        }
        result
    }

    async fn connect(&self) -> Result<(Connection, Channel), OutboxError> {
        let connection = Connection::connect(&self.url, ConnectionProperties::default())
            .await
            .map_err(|_| err_transport())?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|_| err_transport())?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|_| err_transport())?;
        Ok((connection, channel))
    }
}

impl OutboxPublisherPort for ConfirmedAmqpPublisher {
    fn publish(&self, envelope: OutboundMessageEnvelope) -> Result<(), OutboxError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| err_runtime())?;
        // Outbox batches run on the blocking pool, where block_on is allowed.
        runtime.block_on(self.publish_async(envelope))
    }
}

fn require_confirmation(confirmation: Confirmation) -> Result<(), OutboxError> {
    match confirmation {
        Confirmation::Ack(None) => Ok(()),
        Confirmation::Ack(Some(_)) => Err(err_unroutable()),
        Confirmation::Nack(_) => Err(err_rejected()),
        Confirmation::NotRequested => Err(err_unconfirmed()),
    }
}

fn properties_for(envelope: &OutboundMessageEnvelope) -> BasicProperties {
    let metadata = &envelope.metadata;
    let mut headers = FieldTable::default();
    for (name, value) in [
        ("event_id", Some(metadata.event_id.to_string())),
        ("event_type", Some(metadata.event_type.clone())),
        (
            "causation_id",
            metadata.causation_id.map(|id| id.to_string()),
        ),
        ("source", metadata.source.clone()),
    ] {
        if let Some(value) = value {
            headers.insert(name.into(), AMQPValue::LongString(LongString::from(value)));
        }
    }
    let mut properties = BasicProperties::default()
        .with_delivery_mode(PERSISTENT_DELIVERY_MODE)
        .with_message_id(metadata.message_id.to_string().into())
        .with_content_type(
            metadata
                .content_type
                .clone()
                .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.into())
                .into(),
        )
        .with_headers(headers);
    if let Some(id) = metadata.correlation_id {
        properties = properties.with_correlation_id(id.to_string().into());
    }
    properties
}

fn err_transport() -> OutboxError {
    OutboxError::Transport("RabbitMQ connection or publication failed".into())
}

fn err_timeout() -> OutboxError {
    OutboxError::Transport("RabbitMQ publication timed out".into())
}

fn err_runtime() -> OutboxError {
    OutboxError::Transport("RabbitMQ publication requires the application runtime".into())
}

fn err_unroutable() -> OutboxError {
    OutboxError::Routing("RabbitMQ returned an unroutable message".into())
}

fn err_rejected() -> OutboxError {
    OutboxError::Publish("RabbitMQ negatively acknowledged publication".into())
}

fn err_unconfirmed() -> OutboxError {
    OutboxError::Publish("RabbitMQ did not confirm publication".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assembly::io::OutboxEntryMetadata;
    use lapin::message::{BasicReturnMessage, Delivery};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::Uuid;

    const TRANSPORT_FAILURE: &str = "transport error: RabbitMQ connection or publication failed";
    const TIMEOUT_FAILURE: &str = "transport error: RabbitMQ publication timed out";

    fn envelope(routing_key: impl Into<String>) -> OutboundMessageEnvelope {
        let event_id = Uuid::now_v7();
        OutboundMessageEnvelope {
            payload: br#"{"title":"Charger"}"#.to_vec(),
            metadata: OutboxEntryMetadata {
                event_id,
                message_id: event_id,
                correlation_id: Some(Uuid::now_v7()),
                causation_id: Some(Uuid::now_v7()),
                event_type: "OfferCreated".into(),
                routing_key: routing_key.into(),
                source: Some("offers".into()),
                content_type: Some("application/json".into()),
            },
        }
    }

    fn returned_message() -> BasicReturnMessage {
        BasicReturnMessage {
            delivery: Delivery::mock(0, "".into(), "missing".into(), false, Vec::new()),
            reply_code: 312,
            reply_text: "NO_ROUTE".into(),
        }
    }

    /// A TCP endpoint that counts connections and closes each one at once.
    fn closing_listener() -> (String, Arc<AtomicUsize>) {
        listener(false)
    }

    /// A TCP endpoint that counts connections and never answers them.
    fn silent_listener() -> (String, Arc<AtomicUsize>) {
        listener(true)
    }

    fn listener(hold: bool) -> (String, Arc<AtomicUsize>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("amqp://{}", listener.local_addr().unwrap());
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = accepted.clone();
        std::thread::spawn(move || {
            let mut held = Vec::new();
            for stream in listener.incoming() {
                counter.fetch_add(1, Ordering::SeqCst);
                if hold {
                    held.push(stream);
                }
            }
        });
        (url, accepted)
    }

    async fn publish_on_blocking_pool(
        publisher: &Arc<ConfirmedAmqpPublisher>,
    ) -> Result<(), OutboxError> {
        let publisher = publisher.clone();
        tokio::task::spawn_blocking(move || publisher.publish(envelope("customer_facing")))
            .await
            .unwrap()
    }

    #[test]
    fn new_keeps_exchange_and_defaults_to_ten_second_timeout() {
        let publisher = ConfirmedAmqpPublisher::new("amqp://localhost", "offers");
        assert_eq!(publisher.exchange(), "offers");
        assert_eq!(publisher.timeout(), Duration::from_secs(10));
        assert_eq!(
            ConfirmedAmqpPublisher::DEFAULT_TIMEOUT,
            Duration::from_secs(10)
        );
        let publisher = publisher.with_timeout(Duration::from_millis(50));
        assert_eq!(publisher.timeout(), Duration::from_millis(50));
    }

    #[test]
    fn requires_routed_and_confirmed_publication() {
        assert!(require_confirmation(Confirmation::Ack(None)).is_ok());
        for (confirmation, expected) in [
            (
                Confirmation::Ack(Some(returned_message())),
                "routing error: RabbitMQ returned an unroutable message",
            ),
            (
                Confirmation::Nack(None),
                "publish error: RabbitMQ negatively acknowledged publication",
            ),
            (
                Confirmation::Nack(Some(returned_message())),
                "publish error: RabbitMQ negatively acknowledged publication",
            ),
            (
                Confirmation::NotRequested,
                "publish error: RabbitMQ did not confirm publication",
            ),
        ] {
            assert_eq!(
                require_confirmation(confirmation).unwrap_err().to_string(),
                expected
            );
        }
        assert!(matches!(
            require_confirmation(Confirmation::Ack(Some(returned_message()))),
            Err(OutboxError::Routing(_))
        ));
        assert!(matches!(
            require_confirmation(Confirmation::Nack(None)),
            Err(OutboxError::Publish(_))
        ));
    }

    #[test]
    fn transport_errors_are_fixed_and_hide_broker_diagnostics() {
        assert_eq!(err_transport().to_string(), TRANSPORT_FAILURE);
        assert_eq!(err_timeout().to_string(), TIMEOUT_FAILURE);
        assert_eq!(
            err_runtime().to_string(),
            "transport error: RabbitMQ publication requires the application runtime"
        );
        assert!(matches!(err_transport(), OutboxError::Transport(_)));
    }

    #[test]
    fn preserves_metadata_and_requests_persistent_delivery() {
        let envelope = envelope("customer_facing");
        let properties = properties_for(&envelope);
        let metadata = &envelope.metadata;
        assert_eq!(*properties.delivery_mode(), Some(2));
        assert_eq!(
            properties.message_id().as_ref().unwrap().as_str(),
            metadata.message_id.to_string()
        );
        assert_eq!(
            properties.correlation_id().as_ref().unwrap().as_str(),
            metadata.correlation_id.unwrap().to_string()
        );
        assert_eq!(
            properties.content_type().as_ref().unwrap().as_str(),
            "application/json"
        );
        let headers = properties.headers().as_ref().unwrap().inner();
        assert_eq!(headers.len(), 4);
        let header = |name: &str| match headers.get(name) {
            Some(AMQPValue::LongString(value)) => value.to_string(),
            other => panic!("{name}: {other:?}"),
        };
        assert_eq!(header("event_id"), metadata.event_id.to_string());
        assert_eq!(header("event_type"), "OfferCreated");
        assert_eq!(
            header("causation_id"),
            metadata.causation_id.unwrap().to_string()
        );
        assert_eq!(header("source"), "offers");
    }

    #[test]
    fn omits_optional_metadata_and_defaults_content_type() {
        let mut envelope = envelope("customer_facing");
        envelope.metadata.correlation_id = None;
        envelope.metadata.causation_id = None;
        envelope.metadata.source = None;
        envelope.metadata.content_type = None;
        let properties = properties_for(&envelope);
        assert_eq!(*properties.delivery_mode(), Some(2));
        assert!(properties.correlation_id().is_none());
        assert_eq!(
            properties.content_type().as_ref().unwrap().as_str(),
            "application/json"
        );
        let headers = properties.headers().as_ref().unwrap().inner();
        let names: Vec<_> = headers.keys().map(|name| name.as_str()).collect();
        assert_eq!(names, ["event_id", "event_type"]);
    }

    #[test]
    fn keeps_explicit_content_type() {
        let mut envelope = envelope("customer_facing");
        envelope.metadata.content_type = Some("application/cloudevents+json".into());
        assert_eq!(
            properties_for(&envelope)
                .content_type()
                .as_ref()
                .unwrap()
                .as_str(),
            "application/cloudevents+json"
        );
    }

    #[test]
    fn publish_requires_a_current_runtime() {
        let publisher = ConfirmedAmqpPublisher::new("amqp://127.0.0.1:1", "");
        assert_eq!(
            publisher
                .publish(envelope("customer_facing"))
                .unwrap_err()
                .to_string(),
            "transport error: RabbitMQ publication requires the application runtime"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_connections_report_transport_errors_and_reconnect() {
        let (url, accepted) = closing_listener();
        let publisher = Arc::new(ConfirmedAmqpPublisher::new(url, ""));
        for attempt in 1..=2 {
            let result = publish_on_blocking_pool(&publisher).await;
            assert_eq!(result.unwrap_err().to_string(), TRANSPORT_FAILURE);
            assert_eq!(accepted.load(Ordering::SeqCst), attempt);
            assert!(publisher.connection.lock().await.is_none());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unparseable_urls_fail_on_publish_as_transport_errors() {
        let publisher = Arc::new(ConfirmedAmqpPublisher::new("not a url", ""));
        let result = publish_on_blocking_pool(&publisher).await;
        assert_eq!(result.unwrap_err().to_string(), TRANSPORT_FAILURE);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unresponsive_broker_times_out_and_reconnects() {
        let (url, accepted) = silent_listener();
        let timeout = Duration::from_millis(200);
        let publisher = Arc::new(ConfirmedAmqpPublisher::new(url, "").with_timeout(timeout));
        for attempt in 1..=2 {
            let started = std::time::Instant::now();
            let result = publish_on_blocking_pool(&publisher).await;
            assert_eq!(result.unwrap_err().to_string(), TIMEOUT_FAILURE);
            assert!(started.elapsed() >= timeout);
            assert_eq!(accepted.load(Ordering::SeqCst), attempt);
            assert!(publisher.connection.lock().await.is_none());
        }
    }

    #[tokio::test]
    #[ignore = "requires AMQP_URL_TEST"]
    async fn broker_confirms_delivery_rejects_unroutable_and_recovers() {
        use lapin::options::{BasicGetOptions, QueueDeclareOptions, QueueDeleteOptions};
        let url = std::env::var("AMQP_URL_TEST").expect("AMQP_URL_TEST is required");
        let connection = Connection::connect(&url, ConnectionProperties::default())
            .await
            .unwrap();
        let channel = connection.create_channel().await.unwrap();
        let queue = channel
            .queue_declare(
                "".into(),
                QueueDeclareOptions {
                    exclusive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
        let publisher = ConfirmedAmqpPublisher::new(url, "");
        let message = envelope(queue.name().to_string());

        publisher.publish_async(message.clone()).await.unwrap();
        assert!(publisher.connection.lock().await.is_some());
        let received = channel
            .basic_get(queue.name().clone(), BasicGetOptions { no_ack: true })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.data, message.payload);
        assert_eq!(received.properties, properties_for(&message));

        let mut unroutable = message.clone();
        unroutable.metadata.routing_key = format!("mulac-missing-{}", Uuid::now_v7());
        assert_eq!(
            publisher
                .publish_async(unroutable)
                .await
                .unwrap_err()
                .to_string(),
            "routing error: RabbitMQ returned an unroutable message"
        );
        assert!(publisher.connection.lock().await.is_none());

        publisher.publish_async(message.clone()).await.unwrap();
        assert!(
            channel
                .basic_get(queue.name().clone(), BasicGetOptions { no_ack: true })
                .await
                .unwrap()
                .is_some()
        );
        channel
            .queue_delete(queue.name().clone(), QueueDeleteOptions::default())
            .await
            .unwrap();
        connection.close(200, "test complete".into()).await.unwrap();
    }
}
