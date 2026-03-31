-- +goose Up
CREATE TYPE event_status AS ENUM ('pending', 'processing', 'completed', 'failed');

ALTER TABLE events
    ALTER COLUMN status DROP DEFAULT,
    ALTER COLUMN status TYPE event_status USING (
        CASE LOWER(TRIM(status))
            WHEN 'pending'    THEN 'pending'::event_status
            WHEN 'processing' THEN 'processing'::event_status
            WHEN 'completed'  THEN 'completed'::event_status
            WHEN 'failed'     THEN 'failed'::event_status
            ELSE 'pending'::event_status
        END
    ),
    ALTER COLUMN status SET DEFAULT 'pending';

-- +goose Down
ALTER TABLE events
    ALTER COLUMN status DROP DEFAULT,
    ALTER COLUMN status TYPE VARCHAR(20) USING status::text,
    ALTER COLUMN status SET DEFAULT 'pending';

DROP TYPE event_status;
