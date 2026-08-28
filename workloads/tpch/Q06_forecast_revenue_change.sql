-- Q06, derived from TPC-H: forecast-revenue-change filter and reduction.
-- This is a bounded research workload, not an audited TPC-H result.
SELECT
    CAST(
        COALESCE(
            SUM(
                CAST(
                    l_extendedprice * l_discount
                    AS DECIMAL(20,4)
                )
            ),
            CAST(0 AS DECIMAL(38,4))
        )
        AS DECIMAL(38,4)
    ) AS revenue
FROM bench_lineitem
WHERE l_shipdate >= DATE '{{ship_date_start}}'
  AND l_shipdate < DATE '{{ship_date_end}}'
  AND l_discount >= CAST({{discount_lower}} AS DECIMAL(15,2))
  AND l_discount <= CAST({{discount_upper}} AS DECIMAL(15,2))
  AND l_quantity < CAST({{quantity_upper}} AS DECIMAL(15,2))
ORDER BY
    revenue ASC
LIMIT {{result_limit}};
