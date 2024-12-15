-- 1. Deduplicate game_details
CREATE TABLE game_details_deduplicated AS
SELECT DISTINCT * 
FROM game_details;

-- 2. DDL for user_devices_cumulated
CREATE TABLE user_devices_cumulated (
    user_id BIGINT,
    device_activity_datelist MAP<STRING, ARRAY<DATE>>
);

-- 3. Cumulative query for device_activity_datelist
WITH activity_per_user AS (
    SELECT
        user_id,
        browser_type,
        ARRAY_AGG(DISTINCT DATE(event_time)) AS active_days
    FROM events
    JOIN devices ON events.device_id = devices.device_id
    GROUP BY user_id, browser_type
)
SELECT 
    user_id,
    MAP_AGG(browser_type, active_days) AS device_activity_datelist
FROM activity_per_user
GROUP BY user_id;

-- 4. Convert device_activity_datelist to datelist_int
SELECT
    user_id,
    browser_type,
    ARRAY_TRANSFORM(active_days, day -> CAST(FORMAT_DATE('%Y%m%d', day) AS INT)) AS datelist_int
FROM user_devices_cumulated;

-- 5. DDL for hosts_cumulated
CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist ARRAY<DATE>
);

-- 6. Incremental query for host_activity_datelist
SELECT
    host,
    ARRAY_AGG(DISTINCT DATE(event_time)) AS host_activity_datelist
FROM events
GROUP BY host;

-- 7. DDL for host_activity_reduced
CREATE TABLE host_activity_reduced (
    month DATE,
    host TEXT,
    hit_array ARRAY<INT>,
    unique_visitors_array ARRAY<INT>
);

-- 8. Incremental query to load host_activity_reduced
WITH daily_activity AS (
    SELECT
        host,
        DATE_TRUNC('day', event_time) AS activity_date,
        COUNT(*) AS hits,
        COUNT(DISTINCT user_id) AS unique_visitors
    FROM events
    GROUP BY host, DATE_TRUNC('day', event_time)
),
monthly_aggregation AS (
    SELECT
        DATE_TRUNC('month', activity_date) AS month,
        host,
        ARRAY_AGG(hits ORDER BY activity_date) AS hit_array,
        ARRAY_AGG(unique_visitors ORDER BY activity_date) AS unique_visitors_array
    FROM daily_activity
    GROUP BY DATE_TRUNC('month', activity_date), host
)
SELECT * FROM monthly_aggregation;
