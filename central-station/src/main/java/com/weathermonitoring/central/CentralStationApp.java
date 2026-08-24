package com.weathermonitoring.central;

import com.weathermonitoring.common.model.WeatherStatusMessage;
import com.weathermonitoring.common.util.JsonUtil;
import com.weathermonitoring.common.util.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/** Central Base Station: consumes readings and persists them in batches. */
public class CentralStationApp {

    private static final Logger log = LoggerFactory.getLogger(CentralStationApp.class);
    private static final int BATCH_SIZE = 5000;
    private static final long FLUSH_INTERVAL_MS = 5000;
    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) throws SQLException {
        String bootstrapServers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "central-station-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(Topics.WEATHER_READINGS));
        WeatherReadingRepository repository = new WeatherReadingRepository();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Central Station shutting down...");
            running.set(false);
        }));

        List<WeatherStatusMessage> buffer = new ArrayList<>(BATCH_SIZE);
        long lastFlush = System.currentTimeMillis();
        log.info("Central Station started. Consuming '{}' from {}", Topics.WEATHER_READINGS, bootstrapServers);

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        buffer.add(JsonUtil.fromJson(record.value(), WeatherStatusMessage.class));
                    } catch (RuntimeException e) {
                        log.error("Invalid weather message at topic={}, partition={}, offset={}; skipping it",
                                record.topic(), record.partition(), record.offset(), e);
                    }
                }

                boolean sizeThresholdHit = buffer.size() >= BATCH_SIZE;
                boolean timeThresholdHit = !buffer.isEmpty()
                        && System.currentTimeMillis() - lastFlush >= FLUSH_INTERVAL_MS;

                if (sizeThresholdHit || timeThresholdHit) {
                    flush(repository, consumer, buffer);
                    lastFlush = System.currentTimeMillis();
                }
            }
            // Reached only on a clean shutdown; drain whatever is still buffered.
            flush(repository, consumer, buffer);
            log.info("Central Station stopped cleanly.");
        } finally {
            repository.close();
            consumer.close();
        }
    }

    /**
     * Persists the buffer and only then commits the offsets it came from, so a
     * crash in between replays the batch rather than losing it. Inserts are
     * idempotent on (station_id, sequence_number), so a replay is harmless.
     */
    private static void flush(WeatherReadingRepository repository,
                              KafkaConsumer<String, String> consumer,
                              List<WeatherStatusMessage> buffer) throws SQLException {
        if (buffer.isEmpty()) {
            return;
        }
        repository.insertBatch(buffer);
        buffer.clear();
        consumer.commitSync();
    }
}
