diesel::table! {
    outbox_entries (id) {
        id -> Uuid,
        status -> Int4,
        payload -> Text,
        meta -> Jsonb,
        scheduled_at -> Timestamptz,
        attempts -> Int4,
        reservation_id -> Nullable<Uuid>,
        reserved_at -> Nullable<Timestamptz>,
        received_at -> Timestamptz,
        updated_at -> Timestamptz,
        processed_at -> Nullable<Timestamptz>,
        last_error -> Nullable<Text>,
        extra_info -> Nullable<Jsonb>,
    }
}
