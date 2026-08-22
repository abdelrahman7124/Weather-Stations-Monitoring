-- Run this once against your MySQL database (weather_monitoring)

CREATE DATABASE IF NOT EXISTS weather_monitoring
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE weather_monitoring;

CREATE TABLE IF NOT EXISTS weather_readings (
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    station_id        BIGINT NOT NULL,
    sequence_number   BIGINT NOT NULL,
    battery_status    VARCHAR(10) NOT NULL,
    timestamp         BIGINT NOT NULL,
    humidity          INT NOT NULL,
    temperature       INT NOT NULL,
    wind_speed        INT NOT NULL,

    INDEX idx_station_id (station_id),
    INDEX idx_station_seq (station_id, sequence_number)
);
