-- +goose Up
CREATE TYPE event_status AS ENUM ('pending', 'processing', 'completed', 'failed');

ALTER TABLE events
    ALTER COLUMN status DROP DEFAULT,
    ALTER COLUMN status TYPE event_status USING status::event_status,
    ALTER COLUMN status SET DEFAULT 'pending';

-- +goose Down
ALTER TABLE events
    ALTER COLUMN status DROP DEFAULT,
    ALTER COLUMN status TYPE VARCHAR(20) USING status::text,
    ALTER COLUMN status SET DEFAULT 'pending';

DROP TYPE event_status;
