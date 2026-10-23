mod command_handler_registry;
mod event_subscriber_registry;
mod workers;

pub mod io {
    pub use super::command_handler_registry::CommandHandlerRegistry;
    pub use super::event_subscriber_registry::EventSubscriberRegistry;
    pub use super::workers::{run_command_worker, run_event_worker};
    pub use super::{
        KernelBuilder, KernelConfig, KernelError, block_on_blocking, boot, first_command_error,
        first_event_error,
    };
    pub use commanding::io::{
        CommandConsumer,
        CommandConsumerRepository,
        CommandConsumerStorage,
        CommandDispatcher,
        CommandError,
        CommandGateway,
        CommandHandlerPort,
        CommandRecorder,
        CommandRecorderRepository,
        CommandStoreStorage,
        NewCommand,
        NewCommandEnvelope,
        NewCommandMetadata,
        ReservableCommandSpec,
        wrap_handler, //
    };
    pub use mulac_diesel::{DbPool, build_pool};

    pub use eventing::io::{
        EventConsumer,
        EventConsumerRepository,
        EventConsumerStorage,
        EventDispatcher,
        EventGateway,
        EventRecorder,
        EventRecorderRepository,
        EventStoreStorage,
        ReservableEventSpec, //
    };
    pub use inbox::io::{
        InboundMessageEnvelope,
        InboxConsumer,
        InboxConsumerRepository,
        InboxConsumerStorage,
        InboxError,
        InboxStorePort,
        InboxStoreStorage,
        NewInboxMessageEnvelope,
        ReservableInboxSpec, //
    };
}

use crate::command_handler_registry::CommandHandlerRegistry;
use crate::event_subscriber_registry::EventSubscriberRegistry;
pub use commanding::io::{
    ApplicationEvent,
    CommandError,
    CommandHandlerPort,
    NewCommandMetadata, //
};
use commanding::io::{
    CommandConsumer,
    CommandConsumerRepository,
    CommandDispatcher,
    CommandGateway,
    ErasedCommandHandler,
    NewCommand as GatewayNewCommand,
    NewCommandEnvelope as GatewayCommandEnvelope,
    wrap_handler, //
};
pub use eventing::io::{
    EventConsumer,
    EventConsumerRepository,
    EventDispatcher,
    EventEnvelope,
    EventError,
    EventGateway,
    EventMetadata,
    EventProcessPort,
    EventReservePort,
    EventSubscriberPort,
    NewEventEnvelope,
    NewEventMetadata,
    ReservableEventSpec, //
};
pub use inbox::io::{
    InboundMessageEnvelope,
    InboxError,
    InboxMessageMetadata,
    InboxRecorder,
    InboxRecorderRepository,
    InboxStorePort,
    NewInboxMessageEnvelope, //
};
pub use outbox::io::{NewOutboxEnvelope, NewOutboxMetadata, OutboxError};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub type GatewayNewCommandEnvelope = GatewayCommandEnvelope;

const OUTBOX_SUBSCRIBER: &str = "_outbox";

/// Application-owned command boundary.
///
/// Apps define their own command enum and implement this trait. The kernel stays
/// independent of application command types while still offering typed dispatch
/// ergonomics to HTTP handlers and other app entrypoints.
pub trait ApplicationCommand: serde::Serialize {
    fn command_type(&self) -> &'static str;
}

/// Typed application command envelope.
///
/// The command is app-defined, while metadata is the mulac/kernel routing and
/// correlation metadata needed to cross the command gateway boundary.
pub struct NewCommandEnvelope<C> {
    pub command: C,
    pub metadata: NewCommandMetadata,
}

impl<C> NewCommandEnvelope<C>
where
    C: ApplicationCommand,
{
    pub fn into_gateway_envelope(self) -> Result<GatewayNewCommandEnvelope, serde_json::Error> {
        Ok(GatewayNewCommandEnvelope {
            command: GatewayNewCommand {
                command_type: self.command.command_type().to_string(),
                payload: serde_json::to_string(&self.command)?,
            },
            metadata: Some(self.metadata),
        })
    }
}

pub fn boot(config: KernelConfig) -> KernelBuilder {
    KernelBuilder::new(config)
}

#[derive(Debug, Clone, Default)]
pub struct KernelConfig {
    pub database_url: Option<String>,
    pub amqp_url: Option<String>,
    pub inbox_queue: Option<String>,
    pub inbox_exchange: Option<String>,
    pub outbox_exchange: Option<String>,
    pub outbox_content_type: Option<String>,
}

impl KernelConfig {
    pub fn from_env() -> Result<Self, KernelError> {
        Ok(Self {
            database_url: std::env::var("DATABASE_URL").ok(),
            amqp_url: std::env::var("AMQP_URL").ok(),
            inbox_queue: std::env::var("INBOX_QUEUE").ok(),
            inbox_exchange: std::env::var("INBOX_EXCHANGE").ok(),
            outbox_exchange: std::env::var("OUTBOX_EXCHANGE").ok(),
            outbox_content_type: std::env::var("OUTBOX_CONTENT_TYPE").ok(),
        })
    }
}

#[derive(Clone)]
pub struct AppState {
    pub inbox_recorder: Arc<InboxRecorder>,
    pub command_gateway: Arc<CommandGateway>,
    pub event_gateway: Arc<EventGateway>,
}

impl AppState {
    pub fn dispatch_command<C>(&self, envelope: NewCommandEnvelope<C>) -> Result<(), KernelError>
    where
        C: ApplicationCommand,
    {
        let envelope = envelope
            .into_gateway_envelope()
            .map_err(|e| KernelError::Command(CommandError::Conversion(e.to_string())))?;
        self.command_gateway.dispatch(envelope)?;
        Ok(())
    }
}

pub struct KernelBuilder {
    _config: KernelConfig,
    inbox_store: Option<Arc<dyn InboxStorePort>>,
    command_handlers: Vec<(String, Arc<dyn ErasedCommandHandler>)>,
    event_subscribers: Vec<EventSubscriberRegistration>,
    outbox_subscribers: Vec<String>,
}

enum EventSubscriberRegistration {
    Direct {
        event_type: String,
        subscriber_name: String,
        subscriber: Arc<dyn EventSubscriberPort>,
    },
    WithCommandGateway {
        event_type: String,
        subscriber_name: String,
        factory: Arc<dyn Fn(Arc<CommandGateway>) -> Arc<dyn EventSubscriberPort> + Send + Sync>,
    },
}

impl EventSubscriberRegistration {
    fn event_type(&self) -> &str {
        match self {
            Self::Direct { event_type, .. } | Self::WithCommandGateway { event_type, .. } => {
                event_type
            }
        }
    }

    fn subscriber_name(&self) -> &str {
        match self {
            Self::Direct {
                subscriber_name, ..
            }
            | Self::WithCommandGateway {
                subscriber_name, ..
            } => subscriber_name,
        }
    }
}

impl KernelBuilder {
    fn new(config: KernelConfig) -> Self {
        Self {
            _config: config,
            inbox_store: None,
            command_handlers: Vec::new(),
            event_subscribers: Vec::new(),
            outbox_subscribers: Vec::new(),
        }
    }

    /// Supplies the inbox acceptance boundary used by [`Self::start`].
    ///
    /// Production stores must return success only after durable recording.
    /// This option is rejected by [`Self::start_persistent`], which currently
    /// assembles command and event processing only, not an inbox recorder.
    pub fn inbox_store(mut self, store: Arc<dyn InboxStorePort>) -> Self {
        self.inbox_store = Some(store);
        self
    }

    pub fn command_handler<C, E>(
        mut self,
        command_type: impl Into<String>,
        handler: Arc<dyn CommandHandlerPort<C, E>>,
    ) -> Self
    where
        C: serde::de::DeserializeOwned + Send + Sync + 'static,
        E: ApplicationEvent + 'static,
    {
        self.command_handlers
            .push((command_type.into(), wrap_handler(handler)));
        self
    }

    pub fn event_subscriber(
        mut self,
        event_type: impl Into<String>,
        subscriber_name: impl Into<String>,
        subscriber: Arc<dyn EventSubscriberPort>,
    ) -> Self {
        self.event_subscribers
            .push(EventSubscriberRegistration::Direct {
                event_type: event_type.into(),
                subscriber_name: subscriber_name.into(),
                subscriber,
            });
        self
    }

    /// Registers a subscriber factory for [`Self::start_persistent`].
    ///
    /// Direct startup rejects this registration: its command gateway depends on
    /// the event subscriber registry, so it cannot supply the gateway while
    /// constructing that registry.
    pub fn event_subscriber_with_command_gateway<F>(
        mut self,
        event_type: impl Into<String>,
        subscriber_name: impl Into<String>,
        factory: F,
    ) -> Self
    where
        F: Fn(Arc<CommandGateway>) -> Arc<dyn EventSubscriberPort> + Send + Sync + 'static,
    {
        self.event_subscribers
            .push(EventSubscriberRegistration::WithCommandGateway {
                event_type: event_type.into(),
                subscriber_name: subscriber_name.into(),
                factory: Arc::new(factory),
            });
        self
    }

    /// Requests automatic outbox recording, which is not implemented.
    ///
    /// Both startup modes reject this registration. Use [`Self::event_subscriber`]
    /// with an application-owned, recorder-backed subscriber and an explicit
    /// routing policy instead.
    pub fn outbox_subscriber(mut self, event_type: impl Into<String>) -> Self {
        self.outbox_subscribers.push(event_type.into());
        self
    }

    /// Starts direct command and event dispatch with an explicitly supplied inbox store.
    ///
    /// Fails if the inbox store is missing or any registration is unsupported.
    /// `KernelConfig` alone does not provision storage or AMQP infrastructure.
    pub async fn start(self) -> Result<KernelHandle, KernelError> {
        self.validate()?;

        let subscribers = self
            .event_subscribers
            .into_iter()
            .map(|registration| match registration {
                EventSubscriberRegistration::Direct {
                    event_type,
                    subscriber_name,
                    subscriber,
                } => Ok((event_type, subscriber_name, subscriber)),
                EventSubscriberRegistration::WithCommandGateway {
                    event_type,
                    subscriber_name,
                    ..
                } => Err(KernelError::CommandGatewaySubscriberRequiresPersistence {
                    event_type,
                    subscriber: subscriber_name,
                }),
            })
            .collect::<Result<Vec<_>, KernelError>>()?;
        let inbox_store = self.inbox_store.ok_or(KernelError::InboxStoreRequired)?;

        let event_registry = Arc::new(EventSubscriberRegistry::from_subscribers(subscribers));
        let event_dispatcher = Arc::new(EventDispatcher::new(event_registry));
        let event_gateway = Arc::new(EventGateway::direct(event_dispatcher.clone()));

        let command_registry =
            Arc::new(CommandHandlerRegistry::from_handlers(self.command_handlers));
        let command_dispatcher = Arc::new(CommandDispatcher::new(
            command_registry,
            event_gateway.clone(),
        ));
        let command_gateway = Arc::new(CommandGateway::direct(command_dispatcher.clone()));

        let inbox_recorder = Arc::new(InboxRecorder::new(Arc::new(InboxRecorderRepository::new(
            inbox_store,
        ))));

        let state = AppState {
            inbox_recorder,
            command_gateway,
            event_gateway,
        };

        Ok(KernelHandle {
            state,
            token: CancellationToken::new(),
            command_dispatcher,
            event_dispatcher,
        })
    }

    /// Assembles durable command and event processing using the supplied pool.
    ///
    /// Gateway-dependent subscriber factories are supported. Automatic outbox
    /// registrations and the direct-mode inbox store option are rejected rather
    /// than silently ignored. Workers and inbox ingestion remain application-owned.
    pub fn start_persistent(
        self,
        db_pool: io::DbPool,
        drain_rounds: usize,
    ) -> Result<PersistentKernelHandle, KernelError> {
        self.validate()?;
        if self.inbox_store.is_some() {
            return Err(KernelError::InboxStoreUnsupportedInPersistentMode);
        }

        let command_registry =
            Arc::new(CommandHandlerRegistry::from_handlers(self.command_handlers));

        let command_store = Arc::new(io::CommandStoreStorage::new(db_pool.clone()));
        let command_recorder = Arc::new(io::CommandRecorder::new(Arc::new(
            io::CommandRecorderRepository::new(command_store),
        )));
        let command_gateway = Arc::new(CommandGateway::two_phased(command_recorder));

        let subscribers = self
            .event_subscribers
            .into_iter()
            .map(|registration| match registration {
                EventSubscriberRegistration::Direct {
                    event_type,
                    subscriber_name,
                    subscriber,
                } => (event_type, subscriber_name, subscriber),
                EventSubscriberRegistration::WithCommandGateway {
                    event_type,
                    subscriber_name,
                    factory,
                } => (
                    event_type,
                    subscriber_name,
                    factory(command_gateway.clone()),
                ),
            })
            .collect();

        let event_registry = Arc::new(EventSubscriberRegistry::from_subscribers(subscribers));
        let event_dispatcher = Arc::new(EventDispatcher::new(event_registry));

        let event_store = Arc::new(io::EventStoreStorage::new(db_pool.clone()));
        let event_recorder = Arc::new(io::EventRecorder::new(Arc::new(
            io::EventRecorderRepository::new(event_store),
        )));
        let event_gateway = Arc::new(EventGateway::two_phased(event_recorder));

        let command_dispatcher = Arc::new(CommandDispatcher::new(command_registry, event_gateway));

        let command_storage = Arc::new(io::CommandConsumerStorage::new(db_pool.clone()));
        let command_consumer_repository =
            CommandConsumerRepository::new(command_storage.clone(), command_storage);
        let command_consumer = Arc::new(CommandConsumer::new(
            command_consumer_repository,
            command_dispatcher,
        ));

        let event_storage = Arc::new(io::EventConsumerStorage::new(db_pool));
        let event_consumer_repository =
            EventConsumerRepository::new(event_storage.clone(), event_storage);
        let event_consumer = Arc::new(EventConsumer::new(
            event_consumer_repository,
            event_dispatcher,
        ));

        Ok(PersistentKernelHandle {
            state: PersistentKernelState {
                command_gateway,
                command_consumer,
                event_consumer,
                drain_rounds,
            },
            token: CancellationToken::new(),
        })
    }

    fn validate(&self) -> Result<(), KernelError> {
        let mut command_types = std::collections::HashSet::new();
        for (command_type, _) in &self.command_handlers {
            if !command_types.insert(command_type.clone()) {
                return Err(KernelError::DuplicateCommandHandler(command_type.clone()));
            }
        }

        let mut event_subscribers = std::collections::HashSet::new();
        for registration in &self.event_subscribers {
            let event_type = registration.event_type();
            let subscriber = registration.subscriber_name();
            if subscriber == OUTBOX_SUBSCRIBER {
                return Err(KernelError::ReservedSubscriberName(subscriber.to_string()));
            }
            if !event_subscribers.insert((event_type.to_string(), subscriber.to_string())) {
                return Err(KernelError::DuplicateEventSubscriber {
                    event_type: event_type.to_string(),
                    subscriber: subscriber.to_string(),
                });
            }
        }

        let mut outbox_event_types = std::collections::HashSet::new();
        for event_type in &self.outbox_subscribers {
            if !outbox_event_types.insert(event_type.clone()) {
                return Err(KernelError::ReservedSubscriberName(
                    OUTBOX_SUBSCRIBER.into(),
                ));
            }
        }

        if let Some(event_type) = self.outbox_subscribers.first() {
            return Err(KernelError::OutboxSubscriberUnsupported {
                event_type: event_type.clone(),
            });
        }

        Ok(())
    }
}

pub struct KernelHandle {
    state: AppState,
    token: CancellationToken,
    command_dispatcher: Arc<CommandDispatcher>,
    event_dispatcher: Arc<EventDispatcher>,
}

#[derive(Clone)]
pub struct PersistentKernelState {
    command_gateway: Arc<CommandGateway>,
    command_consumer: Arc<CommandConsumer>,
    event_consumer: Arc<EventConsumer>,
    drain_rounds: usize,
}

impl PersistentKernelState {
    pub fn dispatch_command<C>(&self, envelope: NewCommandEnvelope<C>) -> Result<(), KernelError>
    where
        C: ApplicationCommand,
    {
        let envelope = envelope
            .into_gateway_envelope()
            .map_err(|e| KernelError::Command(CommandError::Conversion(e.to_string())))?;

        self.command_gateway.dispatch(envelope)?;

        for _ in 0..self.drain_rounds {
            self.command_consumer
                .consume(&io::ReservableCommandSpec::new(64))
                .map_err(first_command_error)?;
            self.event_consumer
                .consume(&io::ReservableEventSpec::new(64))
                .map_err(first_event_error)?;
        }

        Ok(())
    }

    pub fn command_gateway(&self) -> Arc<CommandGateway> {
        self.command_gateway.clone()
    }
}

pub struct PersistentKernelHandle {
    state: PersistentKernelState,
    token: CancellationToken,
}

impl PersistentKernelHandle {
    pub fn state(&self) -> PersistentKernelState {
        self.state.clone()
    }

    pub fn child_token(&self) -> CancellationToken {
        self.token.child_token()
    }

    pub fn command_consumer(&self) -> Arc<CommandConsumer> {
        self.state.command_consumer.clone()
    }

    pub fn event_consumer(&self) -> Arc<EventConsumer> {
        self.state.event_consumer.clone()
    }

    pub fn shutdown(&self) {
        self.token.cancel();
    }

    pub async fn wait(self) -> Result<(), KernelError> {
        self.token.cancel();
        Ok(())
    }
}

impl KernelHandle {
    pub fn state(&self) -> AppState {
        self.state.clone()
    }

    pub fn child_token(&self) -> CancellationToken {
        self.token.child_token()
    }

    pub fn command_consumer(&self, repo: CommandConsumerRepository) -> CommandConsumer {
        CommandConsumer::new(repo, self.command_dispatcher.clone())
    }

    pub fn event_consumer(&self, repo: EventConsumerRepository) -> EventConsumer {
        EventConsumer::new(repo, self.event_dispatcher.clone())
    }

    pub fn shutdown(&self) {
        self.token.cancel();
    }

    pub async fn wait(self) -> Result<(), KernelError> {
        Ok(())
    }
}

pub fn first_command_error(errors: Vec<CommandError>) -> KernelError {
    match errors.into_iter().next() {
        Some(error) => KernelError::Command(error),
        None => KernelError::Worker("command consumer failed without an error".to_string()),
    }
}

pub fn first_event_error(errors: Vec<EventError>) -> KernelError {
    match errors.into_iter().next() {
        Some(error) => KernelError::Event(error),
        None => KernelError::Worker("event consumer failed without an error".to_string()),
    }
}

pub fn block_on_blocking<F>(future: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
}

#[derive(Debug, thiserror::Error)]
pub enum KernelError {
    #[error("direct startup requires an inbox store; configure KernelBuilder::inbox_store")]
    InboxStoreRequired,
    #[error(
        "inbox_store is only supported by start(); configure inbox ingestion separately when using start_persistent()"
    )]
    InboxStoreUnsupportedInPersistentMode,
    #[error(
        "automatic outbox recording for event type '{event_type}' is not supported; register a recorder-backed event_subscriber instead"
    )]
    OutboxSubscriberUnsupported { event_type: String },
    #[error(
        "event subscriber '{subscriber}' for event type '{event_type}' requires start_persistent() to receive a command gateway"
    )]
    CommandGatewaySubscriberRequiresPersistence {
        event_type: String,
        subscriber: String,
    },
    #[error("database connection failed: {0}")]
    Database(String),
    #[error("AMQP connection failed: {0}")]
    Amqp(String),
    #[error("database migration failed: {0}")]
    Migration(String),
    #[error("command handler already registered for type '{0}'")]
    DuplicateCommandHandler(String),
    #[error("event subscriber '{subscriber}' already registered for event type '{event_type}'")]
    DuplicateEventSubscriber {
        event_type: String,
        subscriber: String,
    },
    #[error("subscriber name '{0}' is reserved")]
    ReservedSubscriberName(String),
    #[error("worker task failed: {0}")]
    Worker(String),
    #[error("inbox error: {0}")]
    Inbox(#[from] InboxError),
    #[error("command error: {0}")]
    Command(#[from] CommandError),
    #[error("event error: {0}")]
    Event(#[from] EventError),
    #[error("outbox error: {0}")]
    Outbox(#[from] OutboxError),
}
