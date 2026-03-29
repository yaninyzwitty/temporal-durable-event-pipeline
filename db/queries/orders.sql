-- name: CreateOrder :one
INSERT INTO orders (
    user_id,
    total_amount
) VALUES (
    $1,
    $2
) RETURNING id, user_id, order_date, status, total_amount;

-- name: GetOrderByID :one
SELECT 
    id,
    user_id,
    order_date,
    status,
    total_amount
FROM orders
WHERE id = $1;

-- name: ListOrdersByUser :many
SELECT 
    id,
    user_id,
    order_date,
    status,
    total_amount
FROM orders
WHERE user_id = $1
ORDER BY order_date DESC;

-- name: UpdateOrderStatus :one
UPDATE orders SET
    status = $2
WHERE id = $1
RETURNING id, user_id, order_date, status, total_amount;

-- name: DeleteOrder :exec
DELETE FROM orders WHERE id = $1;