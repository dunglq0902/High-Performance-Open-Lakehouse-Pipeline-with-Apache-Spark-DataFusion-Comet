-- M04: one-to-many fact/dimension join over runner-bound immutable relations.
SELECT
    oi.order_id,
    oi.line_number,
    oi.product_id,
    p.product_name,
    p.category,
    oi.quantity,
    oi.unit_price,
    oi.discount
FROM bench_order_items AS oi
INNER JOIN bench_products AS p
    ON oi.product_id = p.product_id
ORDER BY
    oi.order_id ASC,
    oi.line_number ASC,
    oi.product_id ASC
LIMIT {{result_limit}};
