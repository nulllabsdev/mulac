use super::entity::{CommandEntry, NewCommandEntry};
use super::models::MetadataJsonb;
use crate::application::prelude::{
    Command,
    CommandEnvelope,
    CommandError,
    CommandMetadata,
    NewCommandEnvelope,
    //
};
use chrono::Utc;

impl TryFrom<&NewCommandEnvelope> for NewCommandEntry {
    type Error = CommandError;

    fn try_from(envelope: &NewCommandEnvelope) -> Result<Self, Self::Error> {
        let metadata = envelope
            .metadata
            .as_ref()
            .ok_or_else(CommandError::missing_command_metadata)?;

        let now = Utc::now();
        let meta = Some(MetadataJsonb(
            serde_json::to_value(metadata).map_err(CommandError::conversion)?,
        ));

        Ok(NewCommandEntry {
            id: metadata.command_id,
            command_type: envelope.command.command_type.clone(),
            payload: envelope.command.payload.clone(),
            meta,
            scheduled_at: now,
            received_at: now,
            extra_info: None,
        })
    }
}

impl TryFrom<CommandEntry> for CommandEnvelope {
    type Error = CommandError;

    fn try_from(entry: CommandEntry) -> Result<Self, Self::Error> {
        let metadata = entry
            .meta
            .as_ref()
            .map(|m| serde_json::from_value::<CommandMetadata>(m.0.clone()))
            .transpose()
            .map_err(CommandError::conversion)?;

        let reservation_id = entry
            .reservation_id
            .ok_or_else(|| CommandError::MissingReservation { id: entry.id })?;

        Ok(CommandEnvelope {
            command: Command {
                id: entry.id,
                reservation_id,
                command_type: entry.command_type,
                payload: entry.payload,
                attempts: entry.attempts,
                extra_info: entry.extra_info.map(Into::into),
            },
            metadata,
        })
    }
}
