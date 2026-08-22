package com.weathermonitoring.central;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Reads DB connection parameters from environment variables so no secrets
 * are hardcoded in source (also needed later for the cloud bonus).
 *
 * Defaults target a local MySQL instance running on WSL:
 *   DB_URL      jdbc:mysql://127.0.0.1:3306/weather_monitoring
 *   DB_USER     root
 *   DB_PASSWORD password
 */
public final class DbConfig {

    public static final String URL =
            System.getenv().getOrDefault("DB_URL", "jdbc:mysql://127.0.0.1:3306/weather_monitoring?useSSL=false&allowPublicKeyRetrieval=true");
    public static final String USER =
            System.getenv().getOrDefault("DB_USER", "root");
    public static final String PASSWORD =
            System.getenv().getOrDefault("DB_PASSWORD", "password");

    private DbConfig() {
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }
}
