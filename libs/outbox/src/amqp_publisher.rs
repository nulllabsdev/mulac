pub mod prelude {
    pub use super::{AmqpPublishConfig, AmqpPublisher};
}

use crate::application::prelude::{
    OutboundMessageEnvelope,
    OutboxEntryMetadata,
    OutboxError,
    OutboxPublisherPort, //
};
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions};
use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};
use lapin::{BasicProperties, Channel, Confirmation};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmqpPublishConfig {
    pub exchange: String,
    pub mandatory: bool,
    pub default_content_type: String,
}

impl Default for AmqpPublishConfig {
    fn default() -> Self {
        Self {
            exchange: String::new(),
            mandatory: false,
            default_content_type: "application/json".into(),
        }
    }
}

#[derive(Clone)]
pub struct AmqpPublisher {
    channel: Channel,
    config: AmqpPublishConfig,
}

impl AmqpPublisher {
    pub fn new(channel: Channel, config: AmqpPublishConfig) -> Self {
        Self { channel, config }
    }

    pub fn config(&self) -> &AmqpPublishConfig {
        &self.config
    }

    pub async fn publish_async(
        &self,
        envelope: OutboundMessageEnvelope,
    ) -> Result<(), OutboxError> {
        self.channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(transport_error)?;

        let confirm = self
            .channel
            .basic_publish(
                self.config.exchange.clone().into(),
                envelope.metadata.routing_key.clone().into(),
                BasicPublishOptions {
                    mandatory: self.config.mandatory,
                    ..Default::default()
                },
                &envelope.payload,
                properties_for(&self.config, &envelope),
            )
            .await
            .map_err(transport_error)?
            .await
            .map_err(transport_error)?;

        match confirm {
            Confirmation::Ack(_) | Confirmation::NotRequested => Ok(()),
            Confirmation::Nack(_) => Err(OutboxError::Transport(
                "broker negatively acknowledged publish".into(),
            )),
        }
    }
}

impl OutboxPublisherPort for AmqpPublisher {
    fn publish(&self, envelope: OutboundMessageEnvelope) -> Result<(), OutboxError> {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.block_on(self.publish_async(envelope)),
            Err(_) => {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| OutboxError::Transport(err.to_string()))?;

                runtime.block_on(self.publish_async(envelope))
            }
        }
    }
}

fn properties_for(
    config: &AmqpPublishConfig,
    envelope: &OutboundMessageEnvelope,
) -> BasicProperties {
    let mut properties = BasicProperties::default()
        .with_message_id(envelope.metadata.message_id.to_string().into())
        .with_content_type(
            envelope
                .metadata
                .content_type
                .clone()
                .unwrap_or_else(|| config.default_content_type.clone())
                .into(),
        )
        .with_headers(headers_for(&envelope.metadata));

    if let Some(correlation_id) = envelope.metadata.correlation_id {
        properties = properties.with_correlation_id(correlation_id.to_string().into());
    }

    properties
}

fn headers_for(metadata: &OutboxEntryMetadata) -> FieldTable {
    let mut headers = FieldTable::default();
    headers.insert(
        ShortString::from("event_id"),
        string_header(metadata.event_id.to_string()),
    );
    headers.insert(
        ShortString::from("event_type"),
        string_header(metadata.event_type.clone()),
    );

    if let Some(causation_id) = metadata.causation_id {
        headers.insert(
            ShortString::from("causation_id"),
            string_header(causation_id.to_string()),
        );
    }

    if let Some(source) = &metadata.source {
        headers.insert(ShortString::from("source"), string_header(source.clone()));
    }

    headers
}

fn string_header(value: String) -> AMQPValue {
    AMQPValue::LongString(LongString::from(value))
}

fn transport_error(err: lapin::Error) -> OutboxError {
    OutboxError::Transport(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    fn envelope(content_type: Option<&str>) -> OutboundMessageEnvelope {
        let event_id = Uuid::now_v7();
        OutboundMessageEnvelope {
            payload: br#"{"user_id":"123"}"#.to_vec(),
            metadata: OutboxEntryMetadata {
                sequence: None,
                event_id,
                message_id: event_id,
                correlation_id: Some(Uuid::now_v7()),
                causation_id: Some(Uuid::now_v7()),
                event_type: "UserRegistered".into(),
                routing_key: "users.registered".into(),
                source: Some("identity-service".into()),
                content_type: content_type.map(str::to_owned),
            },
        }
    }

    #[test]
    fn properties_use_outbox_metadata() {
        let config = AmqpPublishConfig::default();
        let envelope = envelope(Some("application/cloudevents+json"));

        let properties = properties_for(&config, &envelope);

        assert_eq!(
            properties.message_id().as_ref().map(|value| value.as_str()),
            Some(envelope.metadata.message_id.to_string().as_str())
        );
        assert_eq!(
            properties
                .correlation_id()
                .as_ref()
                .map(|value| value.as_str()),
            envelope
                .metadata
                .correlation_id
                .map(|value| value.to_string())
                .as_deref()
        );
        assert_eq!(
            properties
                .content_type()
                .as_ref()
                .map(|value| value.as_str()),
            Some("application/cloudevents+json")
        );
    }

    #[test]
    fn properties_default_content_type_when_missing() {
        let config = AmqpPublishConfig {
            exchange: "events".into(),
            mandatory: true,
            default_content_type: "application/json".into(),
        };
        let envelope = envelope(None);

        let properties = properties_for(&config, &envelope);

        assert_eq!(
            properties
                .content_type()
                .as_ref()
                .map(|value| value.as_str()),
            Some("application/json")
        );
    }

    #[test]
    fn headers_include_event_metadata() {
        let envelope = envelope(Some("application/json"));
        let headers = headers_for(&envelope.metadata);

        let map: BTreeMap<String, String> = headers
            .inner()
            .iter()
            .filter_map(|(key, value)| match value {
                AMQPValue::LongString(value) => Some((key.to_string(), value.to_string())),
                _ => None,
            })
            .collect();

        assert_eq!(
            map.get("event_id").map(String::as_str),
            Some(envelope.metadata.event_id.to_string().as_str())
        );
        assert_eq!(
            map.get("event_type").map(String::as_str),
            Some("UserRegistered")
        );
        assert_eq!(
            map.get("causation_id").map(String::as_str),
            envelope
                .metadata
                .causation_id
                .map(|value| value.to_string())
                .as_deref()
        );
        assert_eq!(
            map.get("source").map(String::as_str),
            Some("identity-service")
        );
    }
}
