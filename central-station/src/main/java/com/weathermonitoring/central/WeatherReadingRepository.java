package com.weathermonitoring.central;

import com.weathermonitoring.common.model.WeatherStatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/** Handles transactional batch inserts of weather readings into MySQL. */
public class WeatherReadingRepository implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WeatherReadingRepository.class);

    private static final String INSERT_SQL =
            "INSERT INTO weather_readings " +
            "(station_id, sequence_number, battery_status, timestamp, humidity, temperature, wind_speed) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?) " +
            "ON DUPLICATE KEY UPDATE id = id";

    private final Connection connection;

    public WeatherReadingRepository() throws SQLException {
        this.connection = DbConfig.getConnection();
        this.connection.setAutoCommit(false);
    }

    /**
     * Inserts a batch in one transaction. A failed batch is rolled back and
     * the exception is propagated so the Kafka consumer does not commit the
     * corresponding offsets.
     */
    public void insertBatch(List<WeatherStatusMessage> batch) throws SQLException {
        if (batch.isEmpty()) {
            return;
        }

        try (PreparedStatement ps = connection.prepareStatement(INSERT_SQL)) {
            for (WeatherStatusMessage m : batch) {
                ps.setLong(1, m.getStationId());
                ps.setLong(2, m.getsNo());
                ps.setString(3, m.getBatteryStatus());
                ps.setLong(4, m.getStatusTimestamp());
                ps.setInt(5, m.getWeather().getHumidity());
                ps.setInt(6, m.getWeather().getTemperature());
                ps.setInt(7, m.getWeather().getWindSpeed());
                ps.addBatch();
            }
            ps.executeBatch();
            connection.commit();
            log.info("Persisted batch of {} readings", batch.size());
        } catch (SQLException e) {
            log.error("Batch insert failed, rolling back", e);
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                e.addSuppressed(rollbackEx);
            }
            throw e;
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            log.warn("Error closing DB connection", e);
        }
    }
}
