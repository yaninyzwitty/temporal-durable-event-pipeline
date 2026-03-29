-- name: CreateUser :one
INSERT INTO users (
    username,
    email,
    password_hash
) VALUES (
    $1,
    $2,
    $3
) RETURNING id, username, email, created_at, updated_at;

-- name: GetUserByID :one
SELECT 
    id,
    username,
    email,
    created_at,
    updated_at
FROM users
WHERE id = $1;

-- name: GetUserByEmail :one
SELECT 
    id,
    username,
    email,
    password_hash,
    created_at,
    updated_at
FROM users
WHERE email = $1;

-- name: ListUsers :many
SELECT 
    id,
    username,
    email,
    created_at,
    updated_at
FROM users
ORDER BY created_at DESC;

-- name: UpdateUser :one
UPDATE users SET
    username = $2,
    email = $3,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING id, username, email, created_at, updated_at;

-- name: DeleteUser :exec
DELETE FROM users WHERE id = $1;