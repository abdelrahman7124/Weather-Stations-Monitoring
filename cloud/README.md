# Cloud deployment (Bonus A, B and C)

Deploys the system across **two Oracle Cloud Infrastructure (OCI) compute
instances** without Kubernetes, against an **Aiven managed PostgreSQL**
database. This file covers provisioning and running; the evidence you
capture along the way is what Bonus C asks you to put in the report.

```text
   Stations VM (OCI)                Central VM (OCI)              Aiven
 ┌──────────────────┐            ┌────────────────────┐      ┌────────────┐
 │ station-1 … -10  │ ─9092─────▶│ ZooKeeper + Kafka  │      │ PostgreSQL │
 │ (10 containers)  │  public    │ Central Station    │─5432▶│  (managed) │
 └──────────────────┘            │ Rain Detector      │ TLS  └────────────┘
                                 └────────────────────┘
```

## Why Oracle Cloud

The Always Free tier includes an **Ampere A1 (ARM) allowance of 4 OCPUs and
24 GB of RAM**, which can be split across up to four instances and does not
expire. That is enough to run both machines properly, for free, instead of
squeezing ten JVMs into AWS's 1 GiB `t3.micro`.

All five images this project uses — `bitnamilegacy/kafka`,
`bitnamilegacy/zookeeper`, `postgres`, `maven:3.9.9-eclipse-temurin-17` and
`eclipse-temurin:17-jre` — publish `linux/arm64` variants, so everything
runs natively on Ampere with no emulation.

## 1. Instance shapes

Split the free Ampere allowance evenly:

| Role | Shape | OCPU / RAM | Image |
|---|---|---|---|
| Weather stations | `VM.Standard.A1.Flex` | 2 / 12 GB | Ubuntu 22.04 or 24.04 (aarch64) |
| Central station | `VM.Standard.A1.Flex` | 2 / 12 GB | Ubuntu 22.04 or 24.04 (aarch64) |

That is exactly the Always Free allocation, so neither instance costs
anything. Default 50 GB boot volumes are fine and stay inside the free
200 GB block-storage limit.

> **"Out of host capacity."** Ampere A1 is frequently exhausted in busy
> regions and this is the single most common blocker. If you hit it, try a
> different availability domain, try again later, or pick a less
> oversubscribed home region. Retrying is normal; it is not a
> misconfiguration on your side.

## 2. Networking

OCI needs the port opened in **two** places. Missing the second one is the
classic OCI mistake — the security list looks correct and traffic still
never arrives.

### 2a. VCN security list (or Network Security Group)

On the VCN your instances live in, add **ingress** rules:

| Applies to | Source | Protocol / Port | Purpose |
|---|---|---|---|
| Central VM subnet | your IP `/32` | TCP 22 | Administration |
| Central VM subnet | stations VM public IP `/32` | TCP 9092 | Kafka external listener |
| Stations VM subnet | your IP `/32` | TCP 22 | Administration |

Egress stays at the default allow-all, which is what lets the stations
reach 9092 and the central VM reach Aiven on 5432.

### 2b. The instance firewall

OCI's Ubuntu images ship with an iptables ruleset that rejects everything
except SSH. Opening 9092 in the security list is **not** sufficient. On the
**central VM** only:

```bash
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 9092 -j ACCEPT
sudo apt-get install -y iptables-persistent
sudo netfilter-persistent save
```

Without `netfilter-persistent save` the rule disappears on reboot.

Screenshot both the security list rules and `sudo iptables -L INPUT -n
--line-numbers` — together they are the "network configuration" item
Bonus C asks for.

## 3. Aiven PostgreSQL

1. Create a **PostgreSQL** service on the **Free** plan. Pick a region near
   your OCI region.
2. From *Overview*, copy the host, port, database name, user and password.
3. Under *Allowed IP addresses*, add the **central VM's public IP `/32`**.
   The stations never talk to the database and must not be allowlisted.
4. Load the schema. No local `psql` needed:

   ```bash
   docker run --rm -i postgres:16 \
     psql "postgresql://avnadmin:<password>@<host>:<port>/defaultdb?sslmode=require" \
     < db/schema.sql
   ```

Capture the `CREATE TABLE` output and the service's "Running" status page.

## 4. Provision both VMs

Run on **each** instance:

```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2 git
sudo usermod -aG docker $USER && newgrp docker
git clone https://github.com/abdelrahman7124/Weather-Stations-Monitoring.git
cd Weather-Stations-Monitoring
cp cloud/.env.example cloud/.env
```

Then edit `cloud/.env`:

- **Both VMs** — set `CENTRAL_PUBLIC_IP` to the central instance's public
  IPv4 address. Reserve the IP in the OCI console if you plan to stop and
  restart the instance, otherwise it changes and the stations silently stop
  delivering.
- **Central VM only** — set `DB_URL`, `DB_USER`, `DB_PASSWORD` from Aiven.
  `DB_URL` must end in `?sslmode=require`.

`cloud/.env` is gitignored, so credentials never enter the repository. That
is the "avoid hardcoding secrets" requirement.

## 5. Start the central VM first

Kafka must be accepting connections before the stations start, or they
spend their first minute retrying.

```bash
docker compose -f cloud/docker-compose.central.yml up -d --build
docker compose -f cloud/docker-compose.central.yml ps
```

The first build compiles the project inside the container and takes a few
minutes. Then confirm:

```bash
docker compose -f cloud/docker-compose.central.yml logs kafka | grep -i started
docker compose -f cloud/docker-compose.central.yml logs central-station
```

A healthy central station logs `Central Station started. Consuming
'weather-readings'` and then `Persisted batch of N readings` every few
seconds once data arrives. **That log line is your proof of connectivity to
the managed database** (Bonus B).

## 6. Start the stations VM

```bash
docker compose -f cloud/docker-compose.stations.yml up -d --build
docker compose -f cloud/docker-compose.stations.yml ps
docker compose -f cloud/docker-compose.stations.yml logs -f station-1
```

You should see `Station 1 sent s_no=… battery=… humidity=…` about once a
second, with the occasional `DROPPED message`.

## 7. Verify end to end

From the **central VM**, confirm all ten stations are arriving:

```bash
docker compose -f cloud/docker-compose.central.yml exec kafka \
  kafka-console-consumer.sh --bootstrap-server kafka:9093 \
  --topic weather-readings --max-messages 20
```

Rain alerts:

```bash
docker compose -f cloud/docker-compose.central.yml exec kafka \
  kafka-console-consumer.sh --bootstrap-server kafka:9093 \
  --topic rain-alerts --max-messages 10
```

And against Aiven, that all ten station IDs are landing:

```bash
docker run --rm -i postgres:16 psql "$AIVEN_URL" \
  -c 'SELECT station_id, COUNT(*) FROM weather_readings GROUP BY station_id ORDER BY station_id;'
```

Then the Part E analysis:

```bash
docker run --rm -i postgres:16 psql "$AIVEN_URL" < db/analysis_queries.sql
```

Let it run ~20 minutes before capturing the analysis queries, so the
battery split has converged near 30/40/30.

## 8. Evidence checklist for the report

Bonus C is graded on the write-up, so collect these as you go:

- [ ] OCI console showing **two running instances**, with shape and OCPU/RAM
- [ ] VCN security list ingress rules
- [ ] `sudo iptables -L INPUT -n --line-numbers` on the central VM
- [ ] Aiven service overview (plan, region, "Running")
- [ ] Aiven allowed-IP list showing only the central VM
- [ ] `docker compose ps` on **each** VM
- [ ] Station logs showing sends and drops (stations VM)
- [ ] `Persisted batch of N readings` (central VM) — proves managed-DB writes
- [ ] `kafka-console-consumer` output for both topics
- [ ] Per-station row counts from Aiven, showing all ten IDs
- [ ] Both Part E query results

## 9. Tear down

The Ampere instances are Always Free and cost nothing if you leave them,
but the Aiven free plan and your own tidiness argue for stopping:

```bash
docker compose -f cloud/docker-compose.stations.yml down
docker compose -f cloud/docker-compose.central.yml down
```

Terminate both instances in the OCI console if you want the Ampere
allowance back, and power off the Aiven service.

## Troubleshooting

**Stations log `Connection to node -1 could not be established` forever.**
The broker is unreachable on 9092. Check in order: `CENTRAL_PUBLIC_IP`
matches the current public IP; the VCN security list allows 9092 from the
stations VM; and — most likely — the iptables rule from §2b is present on
the central VM. Verify with `sudo iptables -L INPUT -n --line-numbers`.

**Stations connect, then time out sending.** Advertised-listener problem:
the broker handed back an address the stations cannot reach. Confirm it
resolved to the real public IP:

```bash
docker compose -f cloud/docker-compose.central.yml exec kafka env | grep ADVERTISED
```

**Central station exits with an SSL or authentication error.** Aiven
requires TLS — `DB_URL` must end in `?sslmode=require`. If it instead
cannot connect at all, the central VM's IP is missing from the Aiven
allowlist.

**`exec format error` when a container starts.** An amd64-only image was
pulled onto ARM. All images this project uses have arm64 variants; if you
changed a tag, check it with `docker manifest inspect <image>`.

**"Out of host capacity" creating the instance.** See §1 — retry, change
availability domain, or change region.
