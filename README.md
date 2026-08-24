# Weather Stations Monitoring — Lab 4

Distributed weather monitoring system for Alexandria University Net Centric Computing Lab 4.

## Architecture

```text
10 Weather Station pods
        |
        v
   Kafka: weather-readings
        |
        +----------------------+
        |                      |
        v                      v
Central Station          Kafka Streams Rain Detector
        |                      |
        v                      v
    PostgreSQL            Kafka: rain-alerts
        |
        v
 Historical SQL analysis
```

The application uses Java 17+, Maven, Apache Kafka, Kafka Streams, and PostgreSQL. The mandatory deployment is containerized with Docker and deployed with Kubernetes.

## Project structure

```text
weather-monitoring/
├── pom.xml
├── common/                       # shared models + JSON/topic utilities
├── weather-station/              # Kafka producer + Dockerfile
├── central-station/              # Kafka consumer + rain detector + Dockerfile
├── db/
│   ├── schema.sql
│   └── analysis_queries.sql
├── k8s/
│   ├── namespace.yaml
│   ├── kafka/
│   │   ├── zookeeper.yaml
│   │   └── kafka.yaml
│   ├── database/postgres.yaml
│   ├── weather-stations.yaml
│   ├── central-station.yaml
│   ├── rain-detector.yaml
│   └── README.md
├── cloud/                        # Azure two-VM bonus deployment
│   ├── docker-compose.central.yml
│   ├── docker-compose.stations.yml
│   ├── .env.example
│   └── README.md
└── Lab4-NetCentric.pdf
```

## Application behavior

Each station emits one reading every second. Battery status is generated with the required 30% low / 40% medium / 30% high distribution, and 10% of generated readings are intentionally dropped. `s_no` increments on every station tick, including dropped readings.

Weather readings are produced to `weather-readings` using the station ID as the Kafka key. The Central Station consumes with manual offset commits and writes batches of up to 5,000 records to PostgreSQL. Kafka offsets are committed only after a successful database transaction.

The rain detector uses Kafka Streams and publishes an alert to `rain-alerts` whenever humidity is greater than 70%.

## Local development without Kubernetes

Install Java 17, Maven, Kafka/ZooKeeper, and PostgreSQL. Then:

```bash
mvn clean package
```

Start Kafka and create topics if auto-creation is disabled:

```bash
kafka-topics.sh --create --topic weather-readings --bootstrap-server 127.0.0.1:9092 --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic rain-alerts --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1
```

Start the rain detector and Central Station, then launch ten station instances with `STATION_ID=1..10`.

## Kubernetes deployment — mandatory lab requirement

The Kubernetes implementation contains:

- 1 ZooKeeper Deployment + Service
- 1 Kafka Deployment + Service
- 1 PostgreSQL Deployment + Service
- 1 PostgreSQL PersistentVolumeClaim
- Kubernetes Secret for the local database password
- 1 Central Station Deployment + Service
- 1 Rain Detector Deployment
- 1 StatefulSet with **10 weather station pods**

The StatefulSet is used because each station needs a unique ID. Pods `weather-station-0` through `weather-station-9` automatically become station IDs `1` through `10` in the Java application.

See [`k8s/README.md`](k8s/README.md) for the exact Minikube build, deployment, and validation commands.

## Cloud deployment — bonus

[`cloud/README.md`](cloud/README.md) deploys the same system across two
Microsoft Azure virtual machines without Kubernetes, writing to a managed
Aiven PostgreSQL database. Stations run on one VM, Kafka and the central
station on the other, and all configuration comes from a gitignored
`cloud/.env`. Azure for Students provides the credit without requiring a
payment card, within a 3 vCPU subscription cap that dictates the VM sizes.

## SQL analysis

`db/analysis_queries.sql` contains the required historical checks:

1. Battery status distribution per station — expected to trend toward 30% / 40% / 30%.
2. Expected vs received message counts using the highest observed sequence number.

Because the station runs continuously, the dropped-message query is an estimate over the observed sequence range; the current sequence is not yet visible until its message is successfully sent.

## Build

```bash
mvn clean package
```

The shaded JARs are generated as:

```text
weather-station/target/weather-station.jar
central-station/target/central-station.jar
```

## Docker

Build from the repository root:

```bash
docker build -f weather-station/Dockerfile -t weather-station:latest .
docker build -f central-station/Dockerfile -t central-station:latest .
```

For Kubernetes with Minikube, build the images inside Minikube's Docker daemon:

```bash
minikube start
eval $(minikube -p minikube docker-env)
docker build -f weather-station/Dockerfile -t weather-station:latest .
docker build -f central-station/Dockerfile -t central-station:latest .
```

## Submission checklist

Mandatory:

- [x] Weather station mock
- [x] 30/40/30 battery distribution
- [x] 10% message drop simulation
- [x] Kafka producer
- [x] Rain detection using Kafka Streams
- [x] Central Station Kafka consumer
- [x] SQL persistence with 5,000-record batch target
- [x] Historical SQL analysis queries
- [x] Dockerfile for Weather Station
- [x] Dockerfile for Central Station
- [x] Kubernetes ZooKeeper + Kafka
- [x] Kubernetes PostgreSQL + persistent storage
- [x] Kubernetes Central Station
- [x] Kubernetes 10-station deployment
- [x] Kubernetes Rain Detector
- [ ] Run the full cluster and capture screenshots/logs for the final report
- [ ] Export the final technical report as PDF with actual deployment evidence

Bonus:

- [x] Two-VM deployment assets (Azure, no Kubernetes) — `cloud/`
- [x] Managed-database configuration with no secrets in source
- [ ] Provision the two Azure VMs and the Aiven service
- [ ] Capture the cloud evidence listed in [`cloud/README.md`](cloud/README.md)
- [ ] Write the Cloud Deployment section of the report
