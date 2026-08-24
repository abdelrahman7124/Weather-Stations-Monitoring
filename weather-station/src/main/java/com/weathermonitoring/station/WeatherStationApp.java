package com.weathermonitoring.station;

import com.weathermonitoring.common.model.Weather;
import com.weathermonitoring.common.model.WeatherStatusMessage;
import com.weathermonitoring.common.util.JsonUtil;
import com.weathermonitoring.common.util.Topics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Mocks a single weather station. Emits one status message per second.
 * STATION_ID can be supplied explicitly or derived from a Kubernetes
 * StatefulSet pod hostname such as weather-station-0.
 */
public class WeatherStationApp {

    private static final Logger log = LoggerFactory.getLogger(WeatherStationApp.class);
    private static final Pattern ORDINAL_HOSTNAME = Pattern.compile(".*-(\\d+)$");
    private static final int LOW_THRESHOLD = 30;
    private static final int MEDIUM_THRESHOLD = 70;
    private static final int DROP_RATE_PERCENT = 10;

    public static void main(String[] args) throws InterruptedException {
        long stationId = resolveStationId(args);
        String bootstrapServers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Station {} shutting down, flushing producer...", stationId);
            producer.flush();
            producer.close();
        }));

        log.info("Station {} started. Bootstrap servers: {}", stationId, bootstrapServers);

        long sNo = 0;
        while (true) {
            sNo++;
            WeatherStatusMessage message = buildMessage(stationId, sNo);

            if (shouldDrop()) {
                log.warn("Station {} DROPPED message s_no={}", stationId, sNo);
            } else {
                String json = JsonUtil.toJson(message);
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        Topics.WEATHER_READINGS, String.valueOf(stationId), json);
                long currentSNo = sNo;
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Station {} failed to send s_no={}", stationId, currentSNo, exception);
                    }
                });
                log.info("Station {} sent s_no={} battery={} humidity={}",
                        stationId, sNo, message.getBatteryStatus(), message.getWeather().getHumidity());
            }
            Thread.sleep(1000);
        }
    }

    private static WeatherStatusMessage buildMessage(long stationId, long sNo) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Weather weather = new Weather(
                random.nextInt(0, 101),
                random.nextInt(20, 111),
                random.nextInt(0, 41));
        return new WeatherStatusMessage(
                stationId,
                sNo,
                randomBatteryStatus(random),
                System.currentTimeMillis() / 1000,
                weather);
    }

    private static String randomBatteryStatus(ThreadLocalRandom random) {
        int roll = random.nextInt(0, 100);
        if (roll < LOW_THRESHOLD) return "low";
        if (roll < MEDIUM_THRESHOLD) return "medium";
        return "high";
    }

    private static boolean shouldDrop() {
        return ThreadLocalRandom.current().nextInt(0, 100) < DROP_RATE_PERCENT;
    }

    private static long resolveStationId(String[] args) {
        String stationIdEnv = System.getenv("STATION_ID");
        if (stationIdEnv != null && !stationIdEnv.isBlank()) {
            return Long.parseLong(stationIdEnv.trim());
        }
        if (args.length > 0) {
            return Long.parseLong(args[0]);
        }

        String hostname = System.getenv("HOSTNAME");
        if (hostname != null) {
            Matcher matcher = ORDINAL_HOSTNAME.matcher(hostname);
            if (matcher.matches()) {
                return Long.parseLong(matcher.group(1)) + 1;
            }
        }

        throw new IllegalArgumentException(
                "STATION_ID must be provided, or HOSTNAME must end in a numeric StatefulSet ordinal");
    }
}
