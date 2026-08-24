# Kubernetes deployment

This deployment satisfies the mandatory Kubernetes part of Lab 4: ZooKeeper, Kafka, PostgreSQL with persistent storage, one Central Station, one Kafka Streams rain detector, and 10 weather-station pods.

## Prerequisites

- Docker
- Minikube (recommended) or another Kubernetes cluster
- kubectl

### Cluster sizing

Every pod declares resource requests, which together come to roughly
**2.3 GiB of memory and 0.85 CPU** — the ten weather stations alone
account for about 1 GiB. Minikube's defaults (2 CPU / 2 GiB) cannot
schedule that, and pods will sit in `Pending` with
`Insufficient memory`. Start it with room to spare:

```bash
minikube start --cpus=4 --memory=6g
```

Check for unschedulable pods with:

```bash
kubectl -n weather-monitoring get pods --field-selector=status.phase=Pending
```

## 1. Build the application images

From the repository root:

```bash
mvn clean package
```

For Minikube, make the local Docker daemon point at Minikube:

```bash
minikube start
minikube docker-env
# Bash/WSL:
eval $(minikube -p minikube docker-env)
```

Build both images:

```bash
docker build -f weather-station/Dockerfile -t weather-station:latest .
docker build -f central-station/Dockerfile -t central-station:latest .
```

## 2. Deploy infrastructure

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka/zookeeper.yaml
kubectl apply -f k8s/kafka/kafka.yaml
kubectl apply -f k8s/database/postgres.yaml
```

Wait for Kafka and PostgreSQL:

```bash
kubectl -n weather-monitoring rollout status deployment/zookeeper
kubectl -n weather-monitoring rollout status deployment/kafka
kubectl -n weather-monitoring rollout status deployment/postgres
```

## 3. Deploy applications

```bash
kubectl apply -f k8s/central-station.yaml
kubectl apply -f k8s/rain-detector.yaml
kubectl apply -f k8s/weather-stations.yaml
```

Verify all 10 stations exist:

```bash
kubectl -n weather-monitoring get pods -o wide
```

The StatefulSet names are `weather-station-0` through `weather-station-9`; the application converts those ordinals to station IDs 1 through 10.

## 4. Validate Kafka

```bash
kubectl -n weather-monitoring exec deploy/kafka -- \
  kafka-topics.sh --bootstrap-server kafka:9092 --list
```

You should see `weather-readings` and, after the rain detector starts processing, `rain-alerts`.

Watch weather messages:

```bash
kubectl -n weather-monitoring exec deploy/kafka -- \
  kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic weather-readings --from-beginning
```

Watch rain alerts:

```bash
kubectl -n weather-monitoring exec deploy/kafka -- \
  kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic rain-alerts --from-beginning
```

## 5. Validate PostgreSQL

```bash
kubectl -n weather-monitoring exec deploy/postgres -- \
  psql -U postgres -d weather_monitoring \
  -c 'SELECT COUNT(*) AS received_messages FROM weather_readings;'
```

Battery distribution:

```bash
kubectl -n weather-monitoring exec deploy/postgres -- \
  psql -U postgres -d weather_monitoring \
  -c 'SELECT station_id, battery_status, COUNT(*) AS message_count, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY station_id), 2) AS percentage FROM weather_readings GROUP BY station_id, battery_status ORDER BY station_id, battery_status;'
```

Dropped-message estimate:

```bash
kubectl -n weather-monitoring exec deploy/postgres -- \
  psql -U postgres -d weather_monitoring \
  -c 'SELECT station_id, MAX(sequence_number) AS expected_messages, COUNT(*) AS received_messages, MAX(sequence_number)-COUNT(*) AS dropped_messages, ROUND(100.0*(MAX(sequence_number)-COUNT(*))/MAX(sequence_number),2) AS drop_rate_percent FROM weather_readings GROUP BY station_id ORDER BY station_id;'
```

Or run the checked-in query file directly:

```bash
kubectl -n weather-monitoring exec -i deploy/postgres -- \
  psql -U postgres -d weather_monitoring < db/analysis_queries.sql
```

## 6. Useful debugging commands

```bash
kubectl -n weather-monitoring logs statefulset/weather-station --tail=50
kubectl -n weather-monitoring logs deployment/central-station --tail=100
kubectl -n weather-monitoring logs deployment/rain-detector --tail=100
kubectl -n weather-monitoring describe pod weather-station-0
kubectl -n weather-monitoring logs deployment/postgres --tail=50
```

## Important note about the image tags

### Kafka and ZooKeeper come from `bitnamilegacy`

In August 2025 Bitnami removed every non-`latest` tag from its Docker Hub
repositories and republished the versioned images under `bitnamilegacy/`.
`bitnami/kafka` and `bitnami/zookeeper` now list **zero** tags, so the
`bitnami/kafka:3.7` and `bitnami/zookeeper:3.9` references this project
originally used fail with `ImagePullBackOff`.

The manifests therefore pull `bitnamilegacy/kafka:3.7` and
`bitnamilegacy/zookeeper:3.9`. These are the identical images under a new
name, so every `KAFKA_CFG_*` environment variable behaves as before.

### Local image tags

The manifests use `imagePullPolicy: IfNotPresent`, so the commands above are intended for Minikube/local images. For a remote Kubernetes cluster, push the images to a registry and replace the image names in the manifests with the registry-qualified names.

The PostgreSQL password in this mandatory local lab deployment is a demo credential stored in a Kubernetes Secret manifest. Do not reuse it for the cloud bonus; use a generated secret or managed database credentials there.
