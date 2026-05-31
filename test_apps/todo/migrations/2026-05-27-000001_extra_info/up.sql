ALTER TABLE command_entries
    ADD COLUMN IF NOT EXISTS extra_info jsonb;

ALTER TABLE event_entries
    ADD COLUMN IF NOT EXISTS extra_info jsonb;
