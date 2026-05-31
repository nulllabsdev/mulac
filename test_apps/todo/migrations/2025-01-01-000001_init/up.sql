CREATE TABLE IF NOT EXISTS todos (
    id uuid PRIMARY KEY,
    title text NOT NULL CHECK (length(trim(title)) > 0),
    description text,
    status text NOT NULL CHECK (status IN ('active', 'completed', 'archived')),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    due_at timestamptz
);

CREATE TABLE IF NOT EXISTS inbox_messages (
    id uuid PRIMARY KEY,
    message_type text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('received', 'processed', 'failed')),
    received_at timestamptz NOT NULL,
    processed_at timestamptz,
    error text
);

CREATE TABLE IF NOT EXISTS outbox_messages (
    id uuid PRIMARY KEY,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'published', 'failed')),
    created_at timestamptz NOT NULL,
    published_at timestamptz,
    attempts integer NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_todos_status ON todos(status);
CREATE INDEX IF NOT EXISTS idx_outbox_messages_status ON outbox_messages(status, created_at);
CREATE INDEX IF NOT EXISTS idx_inbox_messages_status ON inbox_messages(status, received_at);
