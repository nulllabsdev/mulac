pub mod prelude {
    #[cfg(feature = "amqp")]
    pub use super::client::AmqpClientError;
    #[cfg(feature = "amqp")]
    pub use super::transport::AmqpTransport;
    #[cfg(feature = "amqp")]
    pub use super::worker::AmqpWorker;
    #[cfg(feature = "amqp")]
    pub use lapin::{Channel, Connection, ConnectionProperties};

    #[cfg(feature = "amqp")]
    pub async fn connection(url: &str) -> Result<Connection, AmqpClientError> {
        let conn = Connection::connect(url, ConnectionProperties::default()).await?;

        Ok(conn)
    }
}

#[cfg(feature = "amqp")]
mod models {
    use crate::application::prelude::{AcknowledgeHandle, InboxError};
    use lapin::Acker;
    use lapin::options::{BasicAckOptions, BasicNackOptions};
    use std::future::Future;
    use std::pin::Pin;
    use uuid::Uuid;

    #[derive(Debug)]
    pub struct AmqpMessageMetadata {
        pub(super) message_id: Option<Uuid>,
        pub(super) correlation_id: Option<Uuid>,
        pub(super) source: Option<String>,
        pub(super) routing_key: Option<String>,
    }

    #[derive(Debug)]
    pub struct AmqpMessage {
        pub(super) payload: String,
        pub(super) metadata: AmqpMessageMetadata,
    }

    impl AmqpMessage {
        pub(super) fn new(payload: String, metadata: AmqpMessageMetadata) -> Self {
            Self { payload, metadata }
        }

        pub fn id(&self) -> Option<Uuid> {
            self.metadata.message_id
        }

        pub fn correlation_id(&self) -> Option<Uuid> {
            self.metadata.correlation_id
        }

        pub fn source(&self) -> Option<&str> {
            self.metadata.source.as_deref()
        }

        pub fn routing_key(&self) -> Option<&str> {
            self.metadata.routing_key.as_deref()
        }

        pub fn payload(&self) -> &str {
            &self.payload
        }
    }

    pub struct DeliveryHandle(pub(super) Acker);

    impl AcknowledgeHandle for DeliveryHandle {
        fn ack(self: Box<Self>) -> Pin<Box<dyn Future<Output = Result<(), InboxError>> + Send>> {
            Box::pin(async move {
                self.0
                    .ack(BasicAckOptions::default())
                    .await
                    .map(|_| ())
                    .map_err(|e| InboxError::Acknowledgement(e.to_string()))
            })
        }

        fn nack(self: Box<Self>) -> Pin<Box<dyn Future<Output = Result<(), InboxError>> + Send>> {
            Box::pin(async move {
                self.0
                    .nack(BasicNackOptions {
                        requeue: true,
                        ..Default::default()
                    })
                    .await
                    .map(|_| ())
                    .map_err(|e| InboxError::Acknowledgement(e.to_string()))
            })
        }
    }
}

#[cfg(feature = "amqp")]
mod conversions {
    use super::models::{AmqpMessage, AmqpMessageMetadata};
    use crate::application::prelude::InboundMessageEnvelope;
    use lapin::message::Delivery;

    impl From<&Delivery> for AmqpMessageMetadata {
        fn from(delivery: &Delivery) -> Self {
            use uuid::Uuid;
            Self {
                message_id: delivery
                    .properties
                    .message_id()
                    .as_ref()
                    .and_then(|s| Uuid::parse_str(s.as_str()).ok()),
                correlation_id: delivery
                    .properties
                    .correlation_id()
                    .as_ref()
                    .and_then(|s| Uuid::parse_str(s.as_str()).ok()),
                source: Some(delivery.exchange.to_string()).filter(|s| !s.is_empty()),
                routing_key: Some(delivery.routing_key.to_string()).filter(|s| !s.is_empty()),
            }
        }
    }

    impl From<&AmqpMessage> for InboundMessageEnvelope {
        fn from(msg: &AmqpMessage) -> Self {
            InboundMessageEnvelope {
                payload: msg.payload().to_string(),
                message_id: msg.id(),
                correlation_id: msg.correlation_id(),
                source: msg.source().map(ToOwned::to_owned),
                routing_key: msg.routing_key().map(ToOwned::to_owned),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::super::models::{AmqpMessage, AmqpMessageMetadata};
        use lapin::BasicProperties;
        use lapin::message::Delivery;
        use std::mem::MaybeUninit;
        use std::ptr;
        use uuid::Uuid;

        impl TryFrom<&Delivery> for AmqpMessage {
            type Error = std::string::FromUtf8Error;

            fn try_from(delivery: &Delivery) -> Result<Self, Self::Error> {
                Ok(Self {
                    payload: String::from_utf8(delivery.data.clone())?,
                    metadata: AmqpMessageMetadata::from(delivery),
                })
            }
        }

        const PAYLOAD: &str = r#"{"type":"user.payment.succeeded","payload":{"user_id":"bb2bfc87-6658-46fe-9765-add110647bf4","amount":1000,"currency":"USD","payment_method":"credit_card","payment_id":"56969c17-5e72-45aa-bf96-ca43ecb836b7"}}"#;

        #[test]
        fn try_from_delivery_extracts_payload_and_metadata() {
            let delivery = unsafe {
                let mut d = MaybeUninit::<Delivery>::zeroed();
                let p = d.as_mut_ptr();
                ptr::write(&raw mut (*p).delivery_tag, 1);
                ptr::write(&raw mut (*p).exchange, "payments".into());
                ptr::write(&raw mut (*p).routing_key, "user.payment.succeeded".into());
                ptr::write(&raw mut (*p).redelivered, false);
                ptr::write(
                    &raw mut (*p).properties,
                    BasicProperties::default()
                        .with_message_id("61033752-4588-4eb1-908c-411f8ab94cef".into())
                        .with_correlation_id("268f7dc5-7cef-4a6a-ae0a-40253d9acfb5".into()),
                );
                ptr::write(&raw mut (*p).data, PAYLOAD.as_bytes().to_vec());
                d.assume_init()
            };

            let result = AmqpMessage::try_from(&delivery);
            std::mem::forget(delivery);

            let message = result.unwrap();
            assert_eq!(message.payload, PAYLOAD);
            assert_eq!(
                message.metadata.message_id,
                Some(Uuid::parse_str("61033752-4588-4eb1-908c-411f8ab94cef").unwrap())
            );
            assert_eq!(
                message.metadata.correlation_id,
                Some(Uuid::parse_str("268f7dc5-7cef-4a6a-ae0a-40253d9acfb5").unwrap())
            );
            assert_eq!(message.metadata.source, Some("payments".to_string()));
            assert_eq!(
                message.metadata.routing_key,
                Some("user.payment.succeeded".to_string())
            );
        }
    }
}

#[cfg(feature = "amqp")]
mod client {
    use super::models::{AmqpMessage, AmqpMessageMetadata, DeliveryHandle};
    use futures_lite::StreamExt;
    use lapin::Channel;
    use lapin::message::Delivery;
    use lapin::options::{BasicConsumeOptions, BasicNackOptions, QueueDeclareOptions};
    use lapin::types::FieldTable;
    use thiserror::Error;
    use uuid::Uuid;

    #[derive(Debug, Error)]
    pub enum AmqpClientError {
        #[error("lapin error: {0}")]
        Lapin(#[from] lapin::Error),
        #[error("invalid utf-8 payload: {0}")]
        InvalidUtf8(#[from] std::string::FromUtf8Error),
    }

    pub struct AmqpClient {
        consumer: lapin::Consumer,
    }

    impl AmqpClient {
        pub async fn new(channel: &Channel, queue: &str) -> Result<Self, AmqpClientError> {
            let consumer_tag = format!("inbox_worker_{}", Uuid::now_v7());
            Self::new_with_consumer_tag(channel, queue, consumer_tag).await
        }

        pub async fn new_with_consumer_tag(
            channel: &Channel,
            queue: &str,
            consumer_tag: impl Into<String>,
        ) -> Result<Self, AmqpClientError> {
            channel
                .queue_declare(
                    queue.into(),
                    QueueDeclareOptions::default(),
                    FieldTable::default(),
                )
                .await?;

            let consumer = channel
                .basic_consume(
                    queue.into(),
                    consumer_tag.into().into(),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await?;

            Ok(Self { consumer })
        }

        pub async fn next(
            &mut self,
        ) -> Result<Option<(AmqpMessage, DeliveryHandle)>, AmqpClientError> {
            loop {
                match self.consumer.next().await {
                    Some(Ok(delivery)) => {
                        let (handle, result) = unpack(delivery);
                        match result {
                            Ok(msg) => return Ok(Some((msg, handle))),
                            Err(AmqpClientError::InvalidUtf8(_)) => {
                                // Malformed delivery: nack and continue to the next one
                                // so a single poison message cannot stop the worker.
                                let _ = handle.0.nack(BasicNackOptions::default()).await;
                                continue;
                            }
                            Err(e) => {
                                let _ = handle.0.nack(BasicNackOptions::default()).await;
                                return Err(e);
                            }
                        }
                    }
                    Some(Err(e)) => return Err(AmqpClientError::Lapin(e)),
                    None => return Ok(None),
                }
            }
        }
    }

    fn unpack(delivery: Delivery) -> (DeliveryHandle, Result<AmqpMessage, AmqpClientError>) {
        let metadata = AmqpMessageMetadata::from(&delivery);
        let Delivery { data, acker, .. } = delivery;
        let handle = DeliveryHandle(acker);
        let result = String::from_utf8(data)
            .map_err(AmqpClientError::InvalidUtf8)
            .map(|payload| AmqpMessage::new(payload, metadata));
        (handle, result)
    }
}

#[cfg(feature = "amqp")]
mod transport {
    use super::client::{AmqpClient, AmqpClientError};
    use crate::application::prelude::{
        AcknowledgeHandle,
        InboundMessageEnvelope,
        InboxError,
        InboxTransportFuture,
        InboxTransportPort,
        //
    };
    use lapin::Channel;

    pub struct AmqpTransport {
        client: AmqpClient,
    }

    impl AmqpTransport {
        pub async fn new(channel: &Channel, queue: &str) -> Result<Self, AmqpClientError> {
            Ok(Self {
                client: AmqpClient::new(channel, queue).await?,
            })
        }

        pub async fn new_with_consumer_tag(
            channel: &Channel,
            queue: &str,
            consumer_tag: impl Into<String>,
        ) -> Result<Self, AmqpClientError> {
            Ok(Self {
                client: AmqpClient::new_with_consumer_tag(channel, queue, consumer_tag).await?,
            })
        }
    }

    impl InboxTransportPort for AmqpTransport {
        fn next(&mut self) -> InboxTransportFuture<'_> {
            Box::pin(async move {
                match self.client.next().await {
                    Ok(Some((msg, handle))) => Ok(Some((
                        InboundMessageEnvelope::from(&msg),
                        Box::new(handle) as Box<dyn AcknowledgeHandle>,
                    ))),
                    Ok(None) => Ok(None),
                    Err(e) => Err(InboxError::Transport(e.to_string())),
                }
            })
        }
    }
}

#[cfg(feature = "amqp")]
mod worker_loop {
    use crate::application::prelude::{InboxError, InboxTransportPort};
    use crate::application::prelude::{InboxStorePort, NewInboxMessageEnvelope};
    use std::sync::Arc;
    use tokio::time::{Duration, sleep};
    use tokio_util::sync::CancellationToken;

    /// Transport-agnostic worker loop that continuously receives inbound messages
    /// and records them into the inbox.
    ///
    /// The loop polls the transport for the next delivery, offloads the synchronous
    /// `InboxStorePort::store` call to a blocking thread (Diesel is synchronous),
    /// then acknowledges or nacks the external delivery based on the recording outcome.
    ///
    /// **On recording failure:** the delivery is nacked with requeue so the broker
    /// re-delivers it, and the loop backs off for 10 seconds before continuing.
    ///
    /// **On transport error:** the loop backs off for 10 seconds, then propagates
    /// the error to the caller.
    ///
    /// **On transport close** (`Ok(None)`): returns
    /// `Err(InboxError::Transport("consumer stream closed"))` so the caller
    /// can distinguish a broken connection from a clean shutdown.
    ///
    /// **Cancellation** via [`CancellationToken`] takes effect at the next
    /// `tokio::select!` point — either before the next poll or during a backoff
    /// sleep. Any in-flight recording completes before the loop exits.
    pub struct WorkerLoop<T: InboxTransportPort> {
        transport: T,
        store: Arc<dyn InboxStorePort>,
    }

    impl<T: InboxTransportPort> WorkerLoop<T> {
        pub fn new(transport: T, store: Arc<dyn InboxStorePort>) -> Self {
            Self { transport, store }
        }

        /// Run the consume-record-ack loop until the token is cancelled or a
        /// non-recoverable error occurs.
        ///
        /// Returns `Ok(())` on clean cancellation.
        pub async fn run(&mut self, token: CancellationToken) -> Result<(), InboxError> {
            let interval = Duration::from_secs(10);
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    result = self.transport.next() => {
                        match result {
                            Ok(Some((msg, handle))) => {
                                let envelope = NewInboxMessageEnvelope::from(msg);
                                let store = self.store.clone();
                                let recording_result =
                                    tokio::task::spawn_blocking(move || store.store(envelope))
                                        .await
                                        .map_err(|e| InboxError::Recording(e.to_string()))?;

                                match recording_result {
                                    Ok(_) => handle.ack().await?,
                                    Err(_) => {
                                        handle.nack().await?;
                                        tokio::select! {
                                            _ = token.cancelled() => break,
                                            _ = sleep(interval) => continue,
                                        }
                                    }
                                }
                            }
                            Ok(None) => {
                                return Err(InboxError::Transport(
                                    "consumer stream closed".into(),
                                ));
                            }
                            Err(e) => {
                                tokio::select! {
                                    _ = token.cancelled() => break,
                                    _ = sleep(interval) => return Err(e),
                                }
                            }
                        }
                    }
                }
            }

            Ok(())
        }
    }
}

#[cfg(feature = "amqp")]
mod worker {
    use super::client::AmqpClientError;
    use super::transport::AmqpTransport;
    use super::worker_loop::WorkerLoop;
    use crate::application::prelude::InboxError;
    use crate::application::prelude::InboxStorePort;
    use lapin::Channel;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    pub struct AmqpWorker {
        inner: WorkerLoop<AmqpTransport>,
    }

    impl AmqpWorker {
        pub async fn new(
            channel: &Channel,
            queue: &str,
            store: Arc<dyn InboxStorePort>,
        ) -> Result<Self, AmqpClientError> {
            let transport = AmqpTransport::new(channel, queue).await?;
            Ok(Self::from_transport(transport, store))
        }

        pub async fn new_with_consumer_tag(
            channel: &Channel,
            queue: &str,
            consumer_tag: impl Into<String>,
            store: Arc<dyn InboxStorePort>,
        ) -> Result<Self, AmqpClientError> {
            let transport =
                AmqpTransport::new_with_consumer_tag(channel, queue, consumer_tag).await?;
            Ok(Self::from_transport(transport, store))
        }

        pub fn from_transport(transport: AmqpTransport, store: Arc<dyn InboxStorePort>) -> Self {
            Self {
                inner: WorkerLoop::new(transport, store),
            }
        }

        pub async fn run(&mut self, token: CancellationToken) -> Result<(), InboxError> {
            self.inner.run(token).await
        }
    }
}
