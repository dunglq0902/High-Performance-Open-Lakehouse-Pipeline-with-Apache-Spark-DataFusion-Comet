-- M10: high-cardinality session aggregation that requires a shuffle exchange.
WITH session_rollup AS (
    SELECT
        session_id,
        COALESCE(customer_id, CAST(-1 AS BIGINT)) AS customer_id_bucket,
        device_type,
        COUNT(*) AS event_count,
        COALESCE(
            SUM(
                CASE
                    WHEN event_type = 'purchase' THEN CAST(1 AS BIGINT)
                    ELSE CAST(0 AS BIGINT)
                END
            ),
            CAST(0 AS BIGINT)
        ) AS purchase_event_count,
        COALESCE(SUM(event_id), CAST(0 AS BIGINT)) AS event_id_checksum
    FROM bench_events
    WHERE event_time >= TIMESTAMP '{{analysis_start}}'
      AND event_time < TIMESTAMP '{{analysis_end}}'
    GROUP BY
        session_id,
        COALESCE(customer_id, CAST(-1 AS BIGINT)),
        device_type
)
SELECT
    session_id,
    customer_id_bucket,
    device_type,
    event_count,
    purchase_event_count,
    event_id_checksum
FROM session_rollup
ORDER BY
    event_count DESC,
    session_id ASC,
    customer_id_bucket ASC,
    device_type ASC
LIMIT {{result_limit}};
