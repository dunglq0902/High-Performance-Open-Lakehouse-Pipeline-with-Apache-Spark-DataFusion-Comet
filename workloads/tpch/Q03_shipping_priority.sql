-- Q03, derived from TPC-H: shipping-priority revenue report.
-- This is a bounded research workload, not an audited TPC-H result.
WITH revenue_lines AS (
    SELECT
        l.l_orderkey,
        o.o_orderdate,
        o.o_shippriority,
        CAST(
            l.l_extendedprice * (CAST(1 AS DECIMAL(3,2)) - l.l_discount)
            AS DECIMAL(20,4)
        ) AS revenue_line
    FROM bench_customer AS c
    INNER JOIN bench_orders AS o
        ON c.c_custkey = o.o_custkey
    INNER JOIN bench_lineitem AS l
        ON o.o_orderkey = l.l_orderkey
    WHERE c.c_mktsegment = '{{market_segment}}'
      AND o.o_orderdate < DATE '{{order_date_cutoff}}'
      AND l.l_shipdate > DATE '{{order_date_cutoff}}'
)
SELECT
    l_orderkey,
    CAST(
        COALESCE(SUM(revenue_line), CAST(0 AS DECIMAL(38,4)))
        AS DECIMAL(38,4)
    ) AS revenue,
    o_orderdate,
    o_shippriority
FROM revenue_lines
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate ASC,
    l_orderkey ASC,
    o_shippriority ASC
LIMIT {{result_limit}};
