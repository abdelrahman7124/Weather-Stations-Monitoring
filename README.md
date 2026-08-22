# Weather Stations Monitoring — Lab 4

Multi-module Maven project:

```
weather-monitoring/
├── pom.xml                 (parent)
├── common/                 (shared model classes + JSON util)
├── weather-station/        (producer — WeatherStationApp)
├── central-station/        (consumer + rain detector — CentralStationApp, RainDetectorApp)
└── db/
    ├── schema.sql
    └── analysis_queries.sql
```

Docker & Kubernetes are intentionally **not** used yet — everything below runs
natively on WSL so you can develop/debug fast from IntelliJ on Windows.
We'll containerize + deploy to Kubernetes at the end, per the plan.

---

## 1. Import into IntelliJ

1. Open IntelliJ → `File > Open` → select the `weather-monitoring` folder (the
   one containing the parent `pom.xml`).
2. IntelliJ will detect it as a Maven project and import all 3 modules
   automatically (`common`, `weather-station`, `central-station`).
3. Make sure Project SDK is set to Java 17 (`File > Project Structure > SDK`).

---

## 2. Install Kafka on WSL (no Docker)

Run these inside your WSL terminal:

```bash
# 1. Prerequisites
sudo apt update && sudo apt install -y openjdk-17-jdk wget

# 2. Download Kafka (includes Zookeeper)
cd ~
wget https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz
tar -xzf kafka_2.13-3.7.0.tgz
mv kafka_2.13-3.7.0 kafka
cd kafka

# 3. Start Zookeeper (leave this terminal running)
bin/zookeeper-server-start.sh config/zookeeper.properties
```

In a **second WSL terminal**:

```bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

In a **third WSL terminal**, create the two topics the project uses:

```bash
cd ~/kafka
bin/kafka-topics.sh --create --topic weather-readings \
  --bootstrap-server 127.0.0.1:9092 --partitions 3 --replication-factor 1

bin/kafka-topics.sh --create --topic rain-alerts \
  --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1

# sanity check
bin/kafka-topics.sh --list --bootstrap-server 127.0.0.1:9092
```

You can watch messages flowing in with:

```bash
bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 \
  --topic weather-readings --from-beginning
```

---

## 3. Install MySQL on WSL

```bash
sudo apt install -y mysql-server
sudo service mysql start

# set a password for root (or create a dedicated user) and create the DB
sudo mysql
```

Inside the `mysql>` prompt:

```sql
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password';
FLUSH PRIVILEGES;
EXIT;
```

Then load the schema:

```bash
mysql -u root -p < db/schema.sql
```

(Adjust `DB_URL` / `DB_USER` / `DB_PASSWORD` env vars in Central Station's run
config if you use different credentials — see `DbConfig.java`.)

---

## 4. Build

From the project root (WSL or Windows terminal, both fine since it's just Maven):

```bash
mvn clean package
```

---

## 5. Run

### 5.1 Weather Stations (need 10 running instances, one per station_id)

In IntelliJ: create a Run Configuration for `WeatherStationApp`, and set
environment variable `STATION_ID=1`. Duplicate the run config 10 times with
`STATION_ID=1..10` (Run Configs → "Modify options" → "Environment variables").

Or from the terminal, one per station:

```bash
STATION_ID=1 java -jar weather-station/target/weather-station.jar
STATION_ID=2 java -jar weather-station/target/weather-station.jar
# ... up to 10
```

Or a quick loop to launch all 10 in the background:

```bash
for i in $(seq 1 10); do
  STATION_ID=$i nohup java -jar weather-station/target/weather-station.jar \
    > station-$i.log 2>&1 &
done
```

### 5.2 Rain Detector (Kafka Streams — Part C)

```bash
java -cp central-station/target/central-station.jar com.weathermonitoring.central.RainDetectorApp
```

### 5.3 Central Station (consumer + batch insert into MySQL — Part D)

```bash
DB_URL="jdbc:mysql://127.0.0.1:3306/weather_monitoring" \
DB_USER=root \
DB_PASSWORD=password \
java -cp central-station/target/central-station.jar com.weathermonitoring.central.CentralStationApp
```

---

## 6. Validate

```bash
mysql -u root -p weather_monitoring -e "SELECT COUNT(*) FROM weather_readings;"
mysql -u root -p weather_monitoring < db/analysis_queries.sql
```

---

## 7. Deferred for later

- Dockerfiles for `weather-station` and `central-station`
- Kubernetes manifests (10 station pods, Kafka+Zookeeper, central station,
  MySQL, PersistentVolumes)
- Bonus: cloud VM deployment + Aiven managed MySQL
