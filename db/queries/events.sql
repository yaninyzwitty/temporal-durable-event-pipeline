-- name: CreateEvent :one
INSERT INTO events (
    event_type,
    payload
) VALUES (
    $1,
    $2
) RETURNING id, event_type, payload, status, created_at, updated_at, processed_at;

-- name: GetEventByID :one
SELECT 
    id,
    event_type,
    payload,
    status,
    created_at,
    updated_at,
    processed_at
FROM events
WHERE id = $1;

-- name: PollPendingEvents :many
SELECT 
    id,
    event_type,
    payload,
    status,
    created_at,
    updated_at,
    processed_at
FROM events
WHERE status = 'pending'
ORDER BY created_at ASC
FOR UPDATE SKIP LOCKED
LIMIT $1;

-- name: UpdateEventStatus :one
UPDATE events SET
    status = $2,
    processed_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING id, event_type, payload, status, created_at, updated_at, processed_at;

-- name: ListEvents :many
SELECT 
    id,
    event_type,
    payload,
    status,
    created_at,
    updated_at,
    processed_at
FROM events
ORDER BY created_at DESC
LIMIT $1 OFFSET $2;