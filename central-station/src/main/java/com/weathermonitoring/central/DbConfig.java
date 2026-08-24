package com.weathermonitoring.central;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Reads DB connection parameters from environment variables so no secrets
 * are hardcoded in source (also needed later for the cloud bonus).
 *
 * Defaults target a local PostgreSQL instance:
 *   DB_URL      jdbc:postgresql://127.0.0.1:5432/weather_monitoring
 *   DB_USER     postgres
 *   DB_PASSWORD password
 *
 * Managed providers such as Aiven require TLS, which is requested by
 * appending "?sslmode=require" to DB_URL.
 */
public final class DbConfig {

    private static final Logger log = LoggerFactory.getLogger(DbConfig.class);

    private static final int CONNECT_ATTEMPTS = 30;
    private static final long CONNECT_BACKOFF_MS = 2000;

    public static final String URL =
            System.getenv().getOrDefault("DB_URL", "jdbc:postgresql://127.0.0.1:5432/weather_monitoring");
    public static final String USER =
            System.getenv().getOrDefault("DB_USER", "postgres");
    public static final String PASSWORD =
            System.getenv().getOrDefault("DB_PASSWORD", "password");

    private DbConfig() {
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }

    /**
     * Opens a connection, retrying while the server refuses them.
     *
     * Kubernetes starts the Central Station and the database at the same
     * time, so the first attempts routinely fail while PostgreSQL is still
     * running initdb. Without this the pod exits, backs off, and spends its
     * first minutes in CrashLoopBackOff before the database catches up.
     */
    public static Connection awaitConnection() throws SQLException {
        SQLException lastFailure = null;
        for (int attempt = 1; attempt <= CONNECT_ATTEMPTS; attempt++) {
            try {
                Connection connection = getConnection();
                if (attempt > 1) {
                    log.info("Database reachable after {} attempts", attempt);
                }
                return connection;
            } catch (SQLException e) {
                lastFailure = e;
                log.warn("Database not reachable yet (attempt {}/{}): {}",
                        attempt, CONNECT_ATTEMPTS, e.getMessage());
                try {
                    Thread.sleep(CONNECT_BACKOFF_MS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
        throw lastFailure;
    }
}
