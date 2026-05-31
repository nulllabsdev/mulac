// Diesel schema for the todo app's own tables. The kernel-owned tables
// (`command_entries`, `event_entries`) are managed by the kernel and are not
// declared here; tests read them via raw SQL.

diesel::table! {
    todos (id) {
        id -> Uuid,
        title -> Text,
        description -> Nullable<Text>,
        status -> Text,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
        due_at -> Nullable<Timestamptz>,
    }
}

diesel::table! {
    inbox_messages (id) {
        id -> Uuid,
        message_type -> Text,
        payload -> Jsonb,
        status -> Text,
        received_at -> Timestamptz,
        processed_at -> Nullable<Timestamptz>,
        error -> Nullable<Text>,
    }
}

diesel::table! {
    outbox_messages (id) {
        id -> Uuid,
        event_type -> Text,
        payload -> Jsonb,
        status -> Text,
        created_at -> Timestamptz,
        published_at -> Nullable<Timestamptz>,
        attempts -> Int4,
    }
}

diesel::allow_tables_to_appear_in_same_query!(todos, inbox_messages, outbox_messages);
