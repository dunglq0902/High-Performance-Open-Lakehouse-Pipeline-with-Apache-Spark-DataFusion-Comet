-- Q12, derived from TPC-H: shipping-mode and order-priority report.
-- This is a bounded research workload, not an audited TPC-H result.
SELECT
    l.l_shipmode,
    CAST(
        COALESCE(
            SUM(
                CASE
                    WHEN o.o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1
                    ELSE 0
                END
            ),
            CAST(0 AS BIGINT)
        )
        AS BIGINT
    ) AS high_line_count,
    CAST(
        COALESCE(
            SUM(
                CASE
                    WHEN o.o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1
                    ELSE 0
                END
            ),
            CAST(0 AS BIGINT)
        )
        AS BIGINT
    ) AS low_line_count
FROM bench_orders AS o
INNER JOIN bench_lineitem AS l
    ON o.o_orderkey = l.l_orderkey
WHERE l.l_shipmode IN ('{{ship_mode_1}}', '{{ship_mode_2}}')
  AND l.l_commitdate < l.l_receiptdate
  AND l.l_shipdate < l.l_commitdate
  AND l.l_receiptdate >= DATE '{{receipt_date_start}}'
  AND l.l_receiptdate < DATE '{{receipt_date_end}}'
GROUP BY
    l.l_shipmode
ORDER BY
    l.l_shipmode ASC
LIMIT {{result_limit}};
