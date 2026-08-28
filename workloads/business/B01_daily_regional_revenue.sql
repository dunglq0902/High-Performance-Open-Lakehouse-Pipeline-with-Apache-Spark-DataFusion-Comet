-- B01: completed-order gross, discount, and net revenue by UTC date and region.
WITH completed_lines AS (
    SELECT
        CAST(o.order_time AS DATE) AS revenue_date,
        c.region,
        o.order_id,
        CAST(
            CAST(oi.quantity AS DECIMAL(10,0)) * oi.unit_price
            AS DECIMAL(20,4)
        ) AS gross_line_revenue,
        CAST(
            CAST(oi.quantity AS DECIMAL(10,0)) * oi.unit_price * oi.discount
            AS DECIMAL(20,4)
        ) AS discount_line_amount,
        CAST(
            CAST(oi.quantity AS DECIMAL(10,0))
                * oi.unit_price
                * (CAST(1 AS DECIMAL(5,4)) - oi.discount)
            AS DECIMAL(20,4)
        ) AS net_line_revenue
    FROM bench_orders AS o
    INNER JOIN bench_customers AS c
        ON o.customer_id = c.customer_id
    INNER JOIN bench_order_items AS oi
        ON o.order_id = oi.order_id
    WHERE o.status = 'COMPLETED'
      AND o.order_time >= TIMESTAMP '{{analysis_start}}'
      AND o.order_time < TIMESTAMP '{{analysis_end}}'
)
SELECT
    revenue_date,
    region,
    COUNT(DISTINCT order_id) AS order_count,
    CAST(
        COALESCE(SUM(gross_line_revenue), CAST(0 AS DECIMAL(38,4)))
        AS DECIMAL(38,4)
    ) AS gross_revenue,
    CAST(
        COALESCE(SUM(discount_line_amount), CAST(0 AS DECIMAL(38,4)))
        AS DECIMAL(38,4)
    ) AS discount_amount,
    CAST(
        COALESCE(SUM(net_line_revenue), CAST(0 AS DECIMAL(38,4)))
        AS DECIMAL(38,4)
    ) AS net_revenue
FROM completed_lines
GROUP BY
    revenue_date,
    region
ORDER BY
    revenue_date ASC,
    region ASC
LIMIT {{result_limit}};
