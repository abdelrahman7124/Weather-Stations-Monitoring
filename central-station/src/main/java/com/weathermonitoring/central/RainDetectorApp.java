package com.weathermonitoring.central;

import com.weathermonitoring.common.model.WeatherStatusMessage;
import com.weathermonitoring.common.util.JsonUtil;
import com.weathermonitoring.common.util.Topics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Part C of the lab: detects when it's raining (humidity > 70%) using the
 * Kafka Streams DSL and republishes a special alert message to a
 * dedicated "rain-alerts" topic.
 *
 * Run this as its own process (separate from CentralStationApp), or
 * launch it on its own thread from CentralStationApp if you'd rather
 * keep everything in one JVM.
 *
 * Configuration (env vars, optional):
 *   KAFKA_BOOTSTRAP_SERVERS - default 127.0.0.1:9092
 */
public class RainDetectorApp {

    private static final Logger log = LoggerFactory.getLogger(RainDetectorApp.class);
    private static final int HUMIDITY_THRESHOLD = 70;

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");

        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detector-app");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> readings = builder.stream(Topics.WEATHER_READINGS);

        readings
                .filter((key, value) -> {
                    WeatherStatusMessage msg = JsonUtil.fromJson(value, WeatherStatusMessage.class);
                    return msg.getWeather().getHumidity() > HUMIDITY_THRESHOLD;
                })
                .mapValues(value -> {
                    WeatherStatusMessage msg = JsonUtil.fromJson(value, WeatherStatusMessage.class);
                    String alert = String.format(
                            "{\"station_id\":%d,\"s_no\":%d,\"humidity\":%d,\"status_timestamp\":%d,\"alert\":\"RAINING\"}",
                            msg.getStationId(), msg.getsNo(), msg.getWeather().getHumidity(), msg.getStatusTimestamp());
                    log.info("Rain detected at station {} (humidity={}%)", msg.getStationId(), msg.getWeather().getHumidity());
                    return alert;
                })
                .to(Topics.RAIN_ALERTS);

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        log.info("Rain Detector started. Watching topic '{}' for humidity > {}%", Topics.WEATHER_READINGS, HUMIDITY_THRESHOLD);
        streams.start();
    }
}
