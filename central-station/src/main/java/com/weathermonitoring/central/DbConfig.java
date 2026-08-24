package com.weathermonitoring.central;

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
}
