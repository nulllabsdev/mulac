pub mod prelude {
    pub use super::{EventDispatcher, EventSubscriberPort};
}

use crate::application::prelude::{EventError, NewEventEnvelope};
use std::sync::Arc;

pub trait EventSubscriberPort: Send + Sync {
    fn handle(&self, envelope: &NewEventEnvelope) -> Result<(), EventError>;
}

pub struct EventDispatcher {
    subscriber: Arc<dyn EventSubscriberPort>,
}

impl EventDispatcher {
    pub fn new(subscriber: Arc<dyn EventSubscriberPort>) -> Self {
        Self { subscriber }
    }

    pub fn dispatch(&self, envelope: &NewEventEnvelope) -> Result<(), EventError> {
        self.subscriber.handle(envelope)
    }
}
