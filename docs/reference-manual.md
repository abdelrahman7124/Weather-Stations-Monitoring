---
title: "Weather Stations Monitoring — Reference Manual"
subtitle: "Technical terms and technologies, with sources in reading order"
---

# How to use this document

Every technical term and technology used in the Weather Stations Monitoring
project is listed below, in the order the project depends on it. Each entry
names where the concept appears in the codebase and points to the primary
source that documents it. The sources are the explanation; this document
only orders them.

All links were checked and resolve at the time of writing.

# Contents

1. Streaming foundations
2. Language and build
3. Kafka producer — the weather stations
4. Kafka consumer — the central station
5. Kafka Streams — the rain detector
6. Relational persistence
7. Analytical SQL
8. Containers
9. Kubernetes
10. Local cluster


```{=openxml}
<w:p><w:r><w:br w:type="page"/></w:r></w:p>
```


# 1. Streaming foundations

Read first. Everything else assumes the vocabulary established here.

**Event streaming, brokers, records**  
Apache Kafka — Introduction  
<https://kafka.apache.org/documentation/#introduction>

**Topics, partitions, offsets, producers, consumers, consumer groups**  
Apache Kafka — Main Concepts and Terminology  
<https://kafka.apache.org/documentation/#intro_concepts>

**Log structure, replication, delivery semantics (at-most-once, at-least-once, exactly-once)**  
Apache Kafka — Design  
<https://kafka.apache.org/documentation/#design>

**Running a broker and producing and consuming a first message**  
Apache Kafka — Quickstart  
<https://kafka.apache.org/quickstart>

**Coordination service used by the broker for cluster metadata**  
Apache ZooKeeper — Overview  
<https://zookeeper.apache.org/doc/current/zookeeperOver.html>

*Where in the project:* `k8s/kafka/kafka.yaml`, `k8s/kafka/zookeeper.yaml`,
`common/src/main/java/com/weathermonitoring/common/util/Topics.java`


```{=openxml}
<w:p><w:r><w:br w:type="page"/></w:r></w:p>
```


# 2. Language and build

**Java 17 — language and standard library**  
Oracle — Java SE 17 Documentation  
<https://docs.oracle.com/en/java/javase/17/>

**Project Object Model, dependencies, dependency management**  
Apache Maven — Introduction to the POM  
<https://maven.apache.org/guides/introduction/introduction-to-the-pom.html>

**Multi-module (reactor) builds, module ordering, `-pl` and `-am`**  
Apache Maven — Guide to Working with Multiple Modules  
<https://maven.apache.org/guides/mini/guide-multiple-modules.html>

**Uber-JAR / shaded JAR packaging, manifest main class**  
Apache Maven — Shade Plugin  
<https://maven.apache.org/plugins/maven-shade-plugin/>

**JSON serialisation and deserialisation, field binding annotations**  
FasterXML — Jackson Databind  
<https://github.com/FasterXML/jackson-databind>

**Logging facade and binding**  
SLF4J — User Manual  
<https://slf4j.org/manual.html>

*Where in the project:* `pom.xml`, `common/pom.xml`,
`weather-station/pom.xml`, `central-station/pom.xml`,
`common/src/main/java/com/weathermonitoring/common/util/JsonUtil.java`


```{=openxml}
<w:p><w:r><w:br w:type="page"/></w:r></w:p>
```


# 3. Kafka producer — the weather stations

**Producer API, `send`, callbacks, partitioning by key**  
Apache Kafka — `KafkaProducer` Javadoc (3.7)  
<https://kafka.apache.org/37/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html>

**`acks`, `enable.idempotence`, `retries`, `batch.size`, `buffer.memory`**  
Apache Kafka — Producer Configs  
<https://kafka.apache.org/documentation/#producerconfigs>

*Where in the project:*
`weather-station/src/main/java/com/weathermonitoring/station/WeatherStationApp.java`


```{=openxml}
<w:p><w:r><w:br w:type="page"/></w:r></w:p>
```


# 4. Kafka consumer — the central station

**Consumer API, `poll` loop, subscription, rebalancing**  
Apache Kafka — `KafkaConsumer` Javadoc (3.7)  
<https://kafka.apache.org/37/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html>

**`group.id`, `auto.offset.reset`, `enable.auto.commit`, manual `commitSync`**  
Apache Kafka — Consumer Configs  
<https://kafka.apache.org/documentation/#consumerconfigs>

*Where in the project:*
`central-station/src/main/java/com/weathermonitoring/central/CentralStationApp.java`


```{=openxml}
<w:p><w:r><w:br w:type="page"/></w:r></w:p>
```


# 5. Kafka Streams — the rain detector

**Stream processing topology, `KStream`, stream threads, application id**  
Apache Kafka — Streams Core Concepts (3.7)  
<https://kafka.apache.org/37/documentation/streams/core-concepts>

**Streams Developer Guide**  
<https://kafka.apache.org/37/documentation/streams/developer-guide/>

**DSL operators: `mapValues`, `filter`, `to`; stateless transformations**  
Apache Kafka — Streams DSL API  
<https://kafka.apache.org/documentation/streams/developer-guide/dsl-api.html>

*Where in the project:*
`central-station/src/main/java/com/weathermonitoring/central/RainDetectorApp.java`


```{=openxml}
<w:p><w:r><w:br w:type="page"/></w:r></w:p>
```


# 6. Relational persistence

**JDBC connections, statements, result sets**  
Oracle — JDBC Basics  
<https://docs.oracle.com/javase/tutorial/jdbc/basics/index.html>

**`PreparedStatement`, parameter binding, `addBatch` / `executeBatch`**  
Oracle — Using Prepared Statements  
<https://docs.oracle.com/javase/tutorial/jdbc/basics/prepared.html>

**PostgreSQL JDBC driver, connection URLs, TLS parameters**  
pgJDBC — Documentation  
<https://jdbc.postgresql.org/documentation/>

**`CREATE TABLE`, column constraints, table constraints**  
PostgreSQL 16 — `CREATE TABLE`  
<https://www.postgresql.org/docs/16/sql-createtable.html>

**`BIGSERIAL` and the serial pseudo-types**  
PostgreSQL 16 — Numeric Types  
<https://www.postgresql.org/docs/16/datatype-numeric.html>

**Unique constraints and their backing indexes**  
PostgreSQL 16 — Unique Indexes  
<https://www.postgresql.org/docs/16/indexes-unique.html>

**`INSERT ... ON CONFLICT DO NOTHING` (upsert, idempotent insert)**  
PostgreSQL 16 — `INSERT`  
<https://www.postgresql.org/docs/16/sql-insert.html>

**Transactions, commit and rollback**  
PostgreSQL 16 — Transactions  
<https://www.postgresql.org/docs/16/tutorial-transactions.html>

*Where in the project:* `db/schema.sql`,
`central-station/src/main/java/com/weathermonitoring/central/WeatherReadingRepository.java`,
`central-station/src/main/java/com/weathermonitoring/central/DbConfig.java`


```{=openxml}
<w:p><w:r><w:br w:type="page"/></w:r></w:p>
```


# 7. Analytical SQL

**`SELECT`, `GROUP BY`, aggregate functions**  
PostgreSQL 16 — `SELECT`  
<https://www.postgresql.org/docs/16/sql-select.html>

**Window functions and `OVER (PARTITION BY ...)`**  
PostgreSQL 16 — Window Functions  
<https://www.postgresql.org/docs/16/tutorial-window.html>

*Where in the project:* `db/analysis_queries.sql`


```{=openxml}
<w:p><w:r><w:br w:type="page"/></w:r></w:p>
```


# 8. Containers

**Dockerfile instructions: `FROM`, `COPY`, `RUN`, `ENV`, `ENTRYPOINT`**  
Docker — Dockerfile Reference  
<https://docs.docker.com/reference/dockerfile/>

**Multi-stage builds, build stages, copying artefacts between stages**  
Docker — Multi-stage Builds  
<https://docs.docker.com/build/building/multi-stage/>

**Image architectures and multi-platform images (amd64, arm64)**  
Docker — Multi-platform Builds  
<https://docs.docker.com/build/building/multi-platform/>

*Where in the project:* `weather-station/Dockerfile`,
`central-station/Dockerfile`


```{=openxml}
<w:p><w:r><w:br w:type="page"/></w:r></w:p>
```


# 9. Kubernetes

Read in this order; each concept builds on the previous.

**Pods — the unit of scheduling**  
<https://kubernetes.io/docs/concepts/workloads/pods/>

**Deployments — stateless replica management**  
<https://kubernetes.io/docs/concepts/workloads/controllers/deployment/>

**StatefulSets — stable ordinal identities and hostnames**  
<https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/>

**Services — stable virtual IPs, DNS names, headless services**  
<https://kubernetes.io/docs/concepts/services-networking/service/>

**ConfigMaps — non-secret configuration injected as files or variables**  
<https://kubernetes.io/docs/concepts/configuration/configmap/>

**Secrets — credential storage and injection**  
<https://kubernetes.io/docs/concepts/configuration/secret/>

**PersistentVolumes and PersistentVolumeClaims — durable storage**  
<https://kubernetes.io/docs/concepts/storage/persistent-volumes/>

**Resource requests and limits, scheduling and eviction**  
<https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/>

**Liveness, readiness and startup probes**  
<https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/>

*Where in the project:* `k8s/namespace.yaml`, `k8s/weather-stations.yaml`,
`k8s/central-station.yaml`, `k8s/rain-detector.yaml`,
`k8s/database/postgres.yaml`, `k8s/kafka/kafka.yaml`,
`k8s/kafka/zookeeper.yaml`


```{=openxml}
<w:p><w:r><w:br w:type="page"/></w:r></w:p>
```


# 10. Local cluster

**Running a single-node Kubernetes cluster locally**  
Kubernetes — Hello Minikube tutorial  
<https://kubernetes.io/docs/tutorials/hello-minikube/>

**minikube — source, releases and command reference**  
<https://github.com/kubernetes/minikube>

*Where in the project:* `k8s/README.md`
