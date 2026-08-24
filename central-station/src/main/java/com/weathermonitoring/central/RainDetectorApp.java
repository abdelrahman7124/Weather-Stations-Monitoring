package com.weathermonitoring.central;

import com.weathermonitoring.common.model.WeatherStatusMessage;
import com.weathermonitoring.common.util.JsonUtil;
import com.weathermonitoring.common.util.Topics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** Detects humidity > 70% using Kafka Streams and publishes rain alerts. */
public class RainDetectorApp {

    private static final Logger log = LoggerFactory.getLogger(RainDetectorApp.class);
    private static final int HUMIDITY_THRESHOLD = 70;

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");

        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detector-app");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> readings = builder.stream(Topics.WEATHER_READINGS);

        readings
                .mapValues(RainDetectorApp::parseOrNull)
                .filter((key, message) -> message != null && message.getWeather() != null)
                .filter((key, message) -> message.getWeather().getHumidity() > HUMIDITY_THRESHOLD)
                .mapValues(message -> {
                    log.info("Rain detected at station {} (humidity={}%)",
                            message.getStationId(), message.getWeather().getHumidity());
                    return String.format(
                            "{\"station_id\":%d,\"s_no\":%d,\"humidity\":%d,\"status_timestamp\":%d,\"alert\":\"RAINING\"}",
                            message.getStationId(), message.getsNo(),
                            message.getWeather().getHumidity(), message.getStatusTimestamp());
                })
                .to(Topics.RAIN_ALERTS);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler(
                exception -> {
                    log.error("Rain detector stream thread died; replacing it", exception);
                    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
                });
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        log.info("Rain Detector started. Watching humidity > {}%", HUMIDITY_THRESHOLD);
        streams.start();
    }

    /**
     * A record the producer never wrote, or wrote in an older shape, must not
     * be able to stop rain detection for every station. Returns null so the
     * topology can filter the record out.
     */
    private static WeatherStatusMessage parseOrNull(String value) {
        try {
            return JsonUtil.fromJson(value, WeatherStatusMessage.class);
        } catch (RuntimeException e) {
            log.error("Skipping malformed weather message: {}", value, e);
            return null;
        }
    }
}
