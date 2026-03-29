-- name: CreateProduct :one
INSERT INTO products (
    name,
    description,
    price,
    stock_quantity
) VALUES (
    $1,
    $2,
    $3,
    $4
) RETURNING id, name, description, price, stock_quantity, created_at, updated_at;

-- name: GetProductByID :one
SELECT 
    id,
    name,
    description,
    price,
    stock_quantity,
    created_at,
    updated_at
FROM products
WHERE id = $1;

-- name: ListProducts :many
SELECT 
    id,
    name,
    description,
    price,
    stock_quantity,
    created_at,
    updated_at
FROM products
ORDER BY created_at DESC;

-- name: UpdateProduct :one
UPDATE products SET
    name = $2,
    description = $3,
    price = $4,
    stock_quantity = $5,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING id, name, description, price, stock_quantity, created_at, updated_at;

-- name: DeleteProduct :exec
DELETE FROM products WHERE id = $1;