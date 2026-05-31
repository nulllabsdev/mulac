CREATE TABLE IF NOT EXISTS command_entries (
    id uuid PRIMARY KEY,
    command_type text NOT NULL,
    status integer NOT NULL DEFAULT 0,
    payload text NOT NULL,
    meta jsonb,
    scheduled_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    reservation_id uuid,
    reserved_at timestamptz,
    received_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz
);

CREATE TABLE IF NOT EXISTS event_entries (
    id uuid PRIMARY KEY,
    event_type text NOT NULL,
    status integer NOT NULL DEFAULT 0,
    payload text NOT NULL,
    meta jsonb,
    scheduled_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    reservation_id uuid,
    reserved_at timestamptz,
    received_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_command_entries_status_scheduled
    ON command_entries(status, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_command_entries_reservation
    ON command_entries(reservation_id);

CREATE INDEX IF NOT EXISTS idx_event_entries_status_scheduled
    ON event_entries(status, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_event_entries_reservation
    ON event_entries(reservation_id);
