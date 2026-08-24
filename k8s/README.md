# Kubernetes deployment

This deployment satisfies the mandatory Kubernetes part of Lab 4: ZooKeeper, Kafka, MySQL with persistent storage, one Central Station, one Kafka Streams rain detector, and 10 weather-station pods.

## Prerequisites

- Docker
- Minikube (recommended) or another Kubernetes cluster
- kubectl

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
kubectl apply -f k8s/database/mysql.yaml
```

Wait for Kafka and MySQL:

```bash
kubectl -n weather-monitoring rollout status deployment/zookeeper
kubectl -n weather-monitoring rollout status deployment/kafka
kubectl -n weather-monitoring rollout status deployment/mysql
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

## 5. Validate MySQL

```bash
kubectl -n weather-monitoring exec deploy/mysql -- \
  mysql -uroot -pweatherroot weather_monitoring \
  -e 'SELECT COUNT(*) AS received_messages FROM weather_readings;'
```

Battery distribution:

```bash
kubectl -n weather-monitoring exec deploy/mysql -- \
  mysql -uroot -pweatherroot weather_monitoring \
  -e 'SELECT station_id, battery_status, COUNT(*) AS message_count, ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY station_id), 2) AS percentage FROM weather_readings GROUP BY station_id, battery_status ORDER BY station_id, battery_status;'
```

Dropped-message estimate:

```bash
kubectl -n weather-monitoring exec deploy/mysql -- \
  mysql -uroot -pweatherroot weather_monitoring \
  -e 'SELECT station_id, MAX(sequence_number) AS expected_messages, COUNT(*) AS received_messages, MAX(sequence_number)-COUNT(*) AS dropped_messages, ROUND(100*(MAX(sequence_number)-COUNT(*))/MAX(sequence_number),2) AS drop_rate_percent FROM weather_readings GROUP BY station_id ORDER BY station_id;'
```

## 6. Useful debugging commands

```bash
kubectl -n weather-monitoring logs statefulset/weather-station --tail=50
kubectl -n weather-monitoring logs deployment/central-station --tail=100
kubectl -n weather-monitoring logs deployment/rain-detector --tail=100
kubectl -n weather-monitoring describe pod weather-station-0
```

## Important note about the image tags

The manifests use `imagePullPolicy: IfNotPresent`, so the commands above are intended for Minikube/local images. For a remote Kubernetes cluster, push the images to a registry and replace the image names in the manifests with the registry-qualified names.

The MySQL password in this mandatory local lab deployment is a demo credential stored in a Kubernetes Secret manifest. Do not reuse it for the cloud bonus; use a generated secret or managed database credentials there.
