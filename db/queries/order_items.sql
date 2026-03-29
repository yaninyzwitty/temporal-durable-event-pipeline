-- name: CreateOrderItem :one
INSERT INTO order_items (
    order_id,
    product_id,
    quantity,
    unit_price
) VALUES (
    $1,
    $2,
    $3,
    $4
) RETURNING id, order_id, product_id, quantity, unit_price;

-- name: GetOrderItemsByOrderID :many
SELECT 
    id,
    order_id,
    product_id,
    quantity,
    unit_price
FROM order_items
WHERE order_id = $1;

-- name: UpdateOrderItemQuantity :one
UPDATE order_items SET
    quantity = $2
WHERE id = $1
RETURNING id, order_id, product_id, quantity, unit_price;

-- name: DeleteOrderItem :exec
DELETE FROM order_items WHERE id = $1;