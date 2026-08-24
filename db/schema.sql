-- PostgreSQL schema for the weather monitoring store.
--
-- The database itself is created by the deployment (POSTGRES_DB in
-- Kubernetes, or by the provider for a managed instance). To load this
-- file by hand:
--
--   psql -h <host> -U <user> -d weather_monitoring -f db/schema.sql

CREATE TABLE IF NOT EXISTS weather_readings (
    id                BIGSERIAL PRIMARY KEY,
    station_id        BIGINT      NOT NULL,
    sequence_number   BIGINT      NOT NULL,
    battery_status    VARCHAR(10) NOT NULL,
    timestamp         BIGINT      NOT NULL,
    humidity          INT         NOT NULL,
    temperature       INT         NOT NULL,
    wind_speed        INT         NOT NULL,

    -- Kafka delivery is at-least-once, so the Central Station can replay a
    -- batch after a crash between the database commit and the offset
    -- commit. This constraint makes those replays idempotent via
    -- ON CONFLICT DO NOTHING.
    --
    -- Its backing B-tree is also the index the analysis queries use: a
    -- composite index on (station_id, sequence_number) already serves
    -- lookups and grouping on the station_id prefix, so no separate
    -- station_id index is needed.
    CONSTRAINT uq_station_sequence UNIQUE (station_id, sequence_number)
);
