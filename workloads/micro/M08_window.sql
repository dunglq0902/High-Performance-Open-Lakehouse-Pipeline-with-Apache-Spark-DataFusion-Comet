-- M08: deterministic row numbering within each event session.
WITH ranked_events AS (
    SELECT
        session_id,
        event_id,
        event_time,
        event_type,
        ROW_NUMBER() OVER (
            PARTITION BY session_id
            ORDER BY event_time ASC, event_id ASC
        ) AS event_rank
    FROM bench_events
    WHERE event_time >= TIMESTAMP '{{analysis_start}}'
      AND event_time < TIMESTAMP '{{analysis_end}}'
)
SELECT
    session_id,
    event_id,
    event_time,
    event_type,
    event_rank
FROM ranked_events
WHERE event_rank <= CAST({{events_per_session}} AS INT)
ORDER BY
    session_id ASC,
    event_rank ASC,
    event_time ASC,
    event_id ASC
LIMIT {{result_limit}};
