package com.weathermonitoring.common.util;

/**
 * Central place for Kafka topic names so producer, streams processor
 * and consumer all agree on them.
 */
public final class Topics {

    public static final String WEATHER_READINGS = "weather-readings";
    public static final String RAIN_ALERTS = "rain-alerts";

    private Topics() {
    }
}
