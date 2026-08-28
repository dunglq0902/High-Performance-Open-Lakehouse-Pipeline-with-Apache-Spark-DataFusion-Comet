-- M05: bounded low-cardinality aggregation across status and payment method.
SELECT
    status,
    payment_method,
    COUNT(*) AS order_count,
    COALESCE(SUM(order_id), CAST(0 AS BIGINT)) AS order_id_checksum
FROM bench_orders
WHERE order_time >= TIMESTAMP '{{analysis_start}}'
  AND order_time < TIMESTAMP '{{analysis_end}}'
GROUP BY
    status,
    payment_method
ORDER BY
    status ASC,
    payment_method ASC
LIMIT {{result_limit}};
