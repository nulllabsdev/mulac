diesel::table! {
    event_entries (id) {
        id -> Uuid,
        event_type -> Text,
        status -> Int4,
        payload -> Text,
        meta -> Nullable<Jsonb>,
        scheduled_at -> Timestamptz,
        attempts -> Int4,
        reservation_id -> Nullable<Uuid>,
        reserved_at -> Nullable<Timestamptz>,
        received_at -> Timestamptz,
        updated_at -> Timestamptz,
        processed_at -> Nullable<Timestamptz>,
        extra_info -> Nullable<Jsonb>,
    }
}
