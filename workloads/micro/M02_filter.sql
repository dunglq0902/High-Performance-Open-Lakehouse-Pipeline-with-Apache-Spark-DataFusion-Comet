-- M02: controlled-selectivity filter over the runner-bound orders relation.
-- The runner validates and substitutes all placeholders before Spark parses SQL.
SELECT
    CAST({{selectivity_bucket}} AS INT) AS selectivity_bucket,
    COUNT(*) AS matched_order_count,
    COALESCE(SUM(order_id), CAST(0 AS BIGINT)) AS matched_order_id_sum
FROM bench_orders
WHERE order_time >= TIMESTAMP '{{analysis_start}}'
  AND order_time < TIMESTAMP '{{analysis_end}}'
  AND ((order_id - 1) % 100) < CAST({{selectivity_bucket}} AS BIGINT);
