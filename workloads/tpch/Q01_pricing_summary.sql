-- Q01, derived from TPC-H: pricing summary report.
-- This is a bounded research workload, not an audited TPC-H result.
WITH filtered_lineitem AS (
    SELECT
        l_returnflag,
        l_linestatus,
        CAST(l_quantity AS DECIMAL(15,2)) AS l_quantity,
        CAST(l_extendedprice AS DECIMAL(15,2)) AS l_extendedprice,
        CAST(l_discount AS DECIMAL(15,2)) AS l_discount,
        CAST(l_tax AS DECIMAL(15,2)) AS l_tax
    FROM bench_lineitem
    WHERE l_shipdate <= DATE '{{ship_date_cutoff}}'
),
line_metrics AS (
    SELECT
        l_returnflag,
        l_linestatus,
        l_quantity,
        l_extendedprice,
        l_discount,
        CAST(
            l_extendedprice * (CAST(1 AS DECIMAL(3,2)) - l_discount)
            AS DECIMAL(20,4)
        ) AS disc_price,
        CAST(
            CAST(
                l_extendedprice * (CAST(1 AS DECIMAL(3,2)) - l_discount)
                AS DECIMAL(20,4)
            ) * (CAST(1 AS DECIMAL(3,2)) + l_tax)
            AS DECIMAL(38,6)
        ) AS charge
    FROM filtered_lineitem
)
SELECT
    l_returnflag,
    l_linestatus,
    CAST(
        COALESCE(SUM(l_quantity), CAST(0 AS DECIMAL(38,2)))
        AS DECIMAL(38,2)
    ) AS sum_qty,
    CAST(
        COALESCE(SUM(l_extendedprice), CAST(0 AS DECIMAL(38,2)))
        AS DECIMAL(38,2)
    ) AS sum_base_price,
    CAST(
        COALESCE(SUM(disc_price), CAST(0 AS DECIMAL(38,4)))
        AS DECIMAL(38,4)
    ) AS sum_disc_price,
    CAST(
        COALESCE(SUM(charge), CAST(0 AS DECIMAL(38,6)))
        AS DECIMAL(38,6)
    ) AS sum_charge,
    CAST(
        COALESCE(AVG(l_quantity), CAST(0 AS DECIMAL(38,6)))
        AS DECIMAL(38,6)
    ) AS avg_qty,
    CAST(
        COALESCE(AVG(l_extendedprice), CAST(0 AS DECIMAL(38,6)))
        AS DECIMAL(38,6)
    ) AS avg_price,
    CAST(
        COALESCE(AVG(l_discount), CAST(0 AS DECIMAL(38,6)))
        AS DECIMAL(38,6)
    ) AS avg_disc,
    COUNT(*) AS count_order
FROM line_metrics
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag ASC,
    l_linestatus ASC
LIMIT {{result_limit}};
