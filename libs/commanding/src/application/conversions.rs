use crate::application::prelude::{
    CommandEnvelope,
    CommandMetadata,
    NewCommand,
    NewCommandEnvelope,
    NewCommandMetadata,
    //
};

impl From<&CommandMetadata> for NewCommandMetadata {
    fn from(meta: &CommandMetadata) -> Self {
        NewCommandMetadata {
            command_id: meta.command_id,
            correlation_id: meta.correlation_id,
            causation_id: meta.causation_id,
            source: meta.source.clone(),
        }
    }
}

impl From<&CommandEnvelope> for NewCommandEnvelope {
    fn from(envelope: &CommandEnvelope) -> Self {
        let command = NewCommand {
            command_type: envelope.command.command_type.clone(),
            payload: envelope.command.payload.clone(),
        };
        let metadata = envelope.metadata.as_ref().map(NewCommandMetadata::from);

        NewCommandEnvelope { command, metadata }
    }
}

impl From<CommandMetadata> for NewCommandMetadata {
    fn from(meta: CommandMetadata) -> Self {
        NewCommandMetadata {
            command_id: meta.command_id,
            correlation_id: meta.correlation_id,
            causation_id: meta.causation_id,
            source: meta.source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn owned_and_borrowed_metadata_conversions_preserve_all_fields() {
        let metadata = CommandMetadata {
            command_id: Uuid::now_v7(),
            correlation_id: Some(Uuid::now_v7()),
            causation_id: Some(Uuid::now_v7()),
            source: Some("test".into()),
        };
        let borrowed = NewCommandMetadata::from(&metadata);
        let owned = NewCommandMetadata::from(metadata);
        assert_eq!(owned.command_id, borrowed.command_id);
        assert_eq!(owned.correlation_id, borrowed.correlation_id);
        assert_eq!(owned.causation_id, borrowed.causation_id);
        assert_eq!(owned.source, borrowed.source);
    }
}
