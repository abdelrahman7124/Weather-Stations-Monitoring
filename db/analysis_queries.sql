USE weather_monitoring;

-- ============================================================
-- 1. Battery status distribution per station
--    Should trend towards low=30% / medium=40% / high=30%
-- ============================================================
SELECT
    station_id,
    battery_status,
    COUNT(*) AS message_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY station_id), 2) AS percentage
FROM weather_readings
GROUP BY station_id, battery_status
ORDER BY station_id, battery_status;


-- ============================================================
-- 2. Dropped messages per station
--    "Expected" messages = the highest sequence_number seen for that
--    station (sequence increments every 1-second tick regardless of
--    whether the message was actually sent), "received" = rows we
--    actually stored. The difference approximates dropped messages
--    (should trend towards ~10%).
-- ============================================================
SELECT
    station_id,
    MAX(sequence_number)                                        AS expected_messages,
    COUNT(*)                                                     AS received_messages,
    MAX(sequence_number) - COUNT(*)                              AS dropped_messages,
    ROUND(100.0 * (MAX(sequence_number) - COUNT(*)) / MAX(sequence_number), 2) AS drop_rate_percent
FROM weather_readings
GROUP BY station_id
ORDER BY station_id;
