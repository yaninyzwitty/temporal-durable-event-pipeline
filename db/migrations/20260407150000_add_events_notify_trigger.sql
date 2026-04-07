-- +goose Up
CREATE OR REPLACE FUNCTION notify_event_insert()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('events_notification', json_build_object(
        'id', NEW.id,
        'event_type', NEW.event_type,
        'status', NEW.status
    )::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER event_insert_notification
    AFTER INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION notify_event_insert();

-- +goose Down
DROP TRIGGER IF EXISTS event_insert_notification ON events;
DROP FUNCTION IF EXISTS notify_event_insert();