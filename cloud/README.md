# Cloud deployment (Bonus A, B and C)

Deploys the system across **two AWS EC2 instances** without Kubernetes,
against an **Aiven managed PostgreSQL** database. This file covers the
provisioning and run steps; the evidence you capture along the way is what
Bonus C asks you to paste into the report.

```text
   Stations VM (EC2)                Central VM (EC2)              Aiven
 ┌──────────────────┐            ┌────────────────────┐      ┌────────────┐
 │ station-1 … -10  │ ─9092─────▶│ ZooKeeper + Kafka  │      │ PostgreSQL │
 │ (10 containers)  │  public    │ Central Station    │─5432▶│  (managed) │
 └──────────────────┘            │ Rain Detector      │ TLS  └────────────┘
                                 └────────────────────┘
```

## 1. Instance sizing

| Role | Instance type | vCPU / RAM | Why |
|---|---|---|---|
| Weather stations | `t3.small` | 2 / 2 GiB | Ten JVMs at ~150 MiB each |
| Central station | `t3.medium` | 2 / 4 GiB | Kafka + ZooKeeper + two JVMs, one with RocksDB |

Use Ubuntu Server 24.04 LTS (x86_64) on both, 20 GiB gp3 root volumes.

> **The free tier is not big enough.** `t3.micro` gives you 1 GiB, and ten
> JVMs plus the OS will not fit — the kernel OOM-killer starts reaping
> station containers. `t3.small` + `t3.medium` run about **$0.06/hour
> combined** in `us-east-1`. Start them, capture your evidence, then
> terminate. If you would rather stay strictly inside the free tier, run
> 3 stations instead of 10 on a `t3.micro` and say so in the report.

## 2. Security groups

Create two groups. Keep the broker port closed to the world — reference
the other group by ID, not by CIDR.

**`weather-central-sg`** (Central VM)

| Type | Port | Source | Purpose |
|---|---|---|---|
| SSH | 22 | *your* IP `/32` | Administration |
| Custom TCP | 9092 | `weather-stations-sg` | Kafka external listener |

**`weather-stations-sg`** (Stations VM)

| Type | Port | Source | Purpose |
|---|---|---|---|
| SSH | 22 | *your* IP `/32` | Administration |

Outbound stays at the default allow-all, which is what lets the stations
reach 9092 and the central VM reach Aiven on 5432.

Screenshot both inbound rule tables — that is the "network configuration"
item Bonus C asks for.

## 3. Aiven PostgreSQL

1. Create a **PostgreSQL** service on the free plan.
2. Under *Overview*, copy the host, port, database name, user and password.
3. Under *Allowed IP addresses*, add the **Central VM's public IP `/32`**.
   The stations never connect to the database and must not be allowlisted.
4. Load the schema from your laptop (or the central VM):

   ```bash
   psql "postgresql://avnadmin:<password>@<service>.aivencloud.com:<port>/weather_monitoring?sslmode=require" \
     -f db/schema.sql
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
  IPv4 address. Allocate an Elastic IP for it first if you plan to stop and
  restart the instance, otherwise the address changes and the stations
  silently stop delivering.
- **Central VM only** — set `DB_URL`, `DB_USER`, `DB_PASSWORD` from Aiven.

`cloud/.env` is gitignored, so credentials stay off the machine image and
out of the repository. That is the "avoid hardcoding secrets" requirement.

## 5. Start the central VM first

Kafka must be accepting connections before the stations start, or they
spend their first minute retrying.

```bash
docker compose -f cloud/docker-compose.central.yml up -d --build
docker compose -f cloud/docker-compose.central.yml ps
```

Confirm the broker came up and the database connected:

```bash
docker compose -f cloud/docker-compose.central.yml logs kafka | grep -i started
docker compose -f cloud/docker-compose.central.yml logs central-station
```

A healthy central station logs `Central Station started. Consuming
'weather-readings'` and then `Persisted batch of N readings` every few
seconds once data arrives. **This log is your proof of connectivity to the
managed database** (Bonus B).

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
psql "$AIVEN_URL" -c \
  'SELECT station_id, COUNT(*) FROM weather_readings GROUP BY station_id ORDER BY station_id;'
```

Then run the Part E analysis:

```bash
psql "$AIVEN_URL" -f db/analysis_queries.sql
```

## 8. Evidence checklist for the report

Bonus C is graded on the write-up, so collect these as you go:

- [ ] EC2 console showing **two running instances**, with types and AZ
- [ ] Both security groups' inbound rules
- [ ] Aiven service overview (plan, region, "Running")
- [ ] Aiven allowed-IP list showing only the central VM
- [ ] `docker compose ps` on **each** VM
- [ ] Station logs showing sends and drops (stations VM)
- [ ] `Persisted batch of N readings` (central VM) — proves managed-DB writes
- [ ] `kafka-console-consumer` output for both topics
- [ ] Per-station row counts from Aiven, showing all ten IDs
- [ ] Both Part E query results
- [ ] A note that `cloud/.env` holds the credentials and is gitignored

## 9. Tear down

EC2 bills per second and the Aiven free plan does not expire, but neither
stops on its own:

```bash
docker compose -f cloud/docker-compose.stations.yml down
docker compose -f cloud/docker-compose.central.yml down
```

Then **terminate both instances** in the EC2 console, release the Elastic
IP if you allocated one (an unattached EIP is billed), and power off the
Aiven service.

## Troubleshooting

**Stations log `Connection to node -1 could not be established` forever.**
The broker is unreachable on 9092. Check `CENTRAL_PUBLIC_IP` matches the
current public IP, and that `weather-central-sg` allows 9092 from
`weather-stations-sg`.

**Stations connect, then time out sending.** Classic advertised-listener
problem: the broker handed back an address the stations cannot reach.
Confirm `KAFKA_CFG_ADVERTISED_LISTENERS` resolved to the real public IP:

```bash
docker compose -f cloud/docker-compose.central.yml exec kafka \
  env | grep ADVERTISED
```

**Central station exits with an SSL or authentication error.** Aiven
requires TLS — `DB_URL` must end in `?sslmode=require`. If it instead
reports being unable to connect at all, the central VM's IP is missing
from the Aiven allowlist.

**A station container keeps restarting.** Almost always the OOM-killer on
an undersized instance. Check `docker inspect <container> | grep OOMKilled`
and move to a larger type.
