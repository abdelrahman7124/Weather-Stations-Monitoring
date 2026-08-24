# Cloud deployment (Bonus A, B and C)

Deploys the system across **two Microsoft Azure virtual machines** without
Kubernetes, against an **Aiven managed PostgreSQL** database. This file
covers provisioning and running; the evidence you capture along the way is
what Bonus C asks you to put in the report.

```text
   Stations VM (Azure)              Central VM (Azure)            Aiven
 ┌──────────────────┐            ┌────────────────────┐      ┌────────────┐
 │ station-1 … -10  │ ─9092─────▶│ ZooKeeper + Kafka  │      │ PostgreSQL │
 │ (10 containers)  │  VNet      │ Central Station    │─5432▶│  (managed) │
 └──────────────────┘            │ Rain Detector      │ TLS  └────────────┘
                                 └────────────────────┘
```

## Why Azure for Students

[Azure for Students](https://azure.microsoft.com/en-us/free/students) grants
**$100 of credit with no credit card**, verified with an academic email.
That matters here: Oracle Cloud and AWS both require a payment card for
identity verification, and the AWS free tier's 1 GiB instances cannot hold
ten JVMs anyway.

### The 3 vCPU ceiling

Azure for Students subscriptions are capped at **3 vCPUs in total**, and
free subscriptions are not eligible for quota increases. Both machines must
therefore fit inside 3 vCPUs. The free-hours sizes (`B1s`, `B2ats_v2`,
`B2pts_v2`) all carry only 1 GiB of RAM, which is not enough, so this guide
uses slightly larger burstable sizes paid out of the $100 credit — a few
hours of running costs well under a dollar.

| Role | Size | vCPU / RAM | Why |
|---|---|---|---|
| Weather stations | `Standard_B1ms` | 1 / 2 GiB | Ten stations measured at ~78 MiB each |
| Central station | `Standard_B2s` | 2 / 4 GiB | Kafka and ZooKeeper are the memory-hungry pair |

That is exactly 3 vCPUs. Check your own quota before creating anything:
**Subscription → Usage + quotas → Compute**. If yours is lower than 3, use
`Standard_B1ms` for both and expect the stations VM to be slower to start.

## 1. Create the virtual machines

Portal → **Virtual machines → Create → Azure virtual machine**. Create the
**central VM first** so the stations VM can join the network it creates.

Both machines:

- **Image**: Ubuntu Server 24.04 LTS — x64 Gen2
- **Authentication**: SSH public key. Paste the output of `cat ~/.ssh/id_ed25519.pub`
- **Inbound ports**: allow **SSH (22)** only
- **Virtual network**: central VM creates one; put the stations VM in the
  **same VNet and subnet**

Name them `weather-central` (`Standard_B2s`) and `weather-stations`
(`Standard_B1ms`). The default admin username is `azureuser`.

## 2. Networking

Because both VMs sit in one virtual network, the stations reach the broker
over its **private** IP. Azure's default `AllowVnetInBound` NSG rule already
permits that, so **port 9092 never has to be exposed to the internet** and
no extra inbound rule is needed.

Note the central VM's **private** IP from its Overview page — that is the
value `CENTRAL_HOST` takes.

Screenshot the NSG's inbound rules. Showing SSH restricted and 9092 absent
is a stronger answer to the Bonus C "network configuration" item than
opening the port would be: the broker is unreachable from outside the VNet
by construction.

> Deploying the two VMs to **separate** virtual networks instead is also
> valid, and closer to the lab's wording about remote connection. In that
> case set `CENTRAL_HOST` to the central VM's public IP and add an inbound
> NSG rule on the central VM for TCP 9092 whose source is the stations VM's
> public IP `/32` — never `Any`.

## 3. Aiven PostgreSQL

1. Create a **PostgreSQL** service on the **Free** plan, in a region near
   your Azure region.
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

```bash
ssh azureuser@<vm-public-ip>
```

Then on **each** instance:

```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2 git
sudo usermod -aG docker $USER && newgrp docker
git clone https://github.com/abdelrahman7124/Weather-Stations-Monitoring.git
cd Weather-Stations-Monitoring
cp cloud/.env.example cloud/.env
```

Then edit `cloud/.env`:

- **Both VMs** — `CENTRAL_HOST` = the central VM's **private** IP (or its
  public IP if you split the VNets).
- **Central VM only** — `DB_URL`, `DB_USER`, `DB_PASSWORD` from Aiven.
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

The first build compiles the project inside the container and takes several
minutes on a burstable VM. Then confirm:

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

Check memory has room to spare — expect roughly 800 MiB in use of 2 GiB:

```bash
free -h && docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}'
```

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

Let it run ~20 minutes before capturing the analysis queries, so the battery
split has converged near 30/40/30.

## 8. Evidence checklist for the report

Bonus C is graded on the write-up, so collect these as you go:

- [ ] Azure portal showing **two running VMs**, with size and region
- [ ] NSG inbound rules for both VMs
- [ ] Aiven service overview (plan, region, "Running")
- [ ] Aiven allowed-IP list showing only the central VM
- [ ] `docker compose ps` on **each** VM
- [ ] Station logs showing sends and drops (stations VM)
- [ ] `Persisted batch of N readings` (central VM) — proves managed-DB writes
- [ ] `kafka-console-consumer` output for both topics
- [ ] Per-station row counts from Aiven, showing all ten IDs
- [ ] Both Part E query results

## 9. Tear down

Azure bills the credit for as long as the VMs exist, so stop them when done:

```bash
docker compose -f cloud/docker-compose.stations.yml down
docker compose -f cloud/docker-compose.central.yml down
```

Then **Stop (deallocate)** both VMs in the portal — a merely "stopped" VM
still bills. Delete the resource group to remove everything at once, and
power off the Aiven service.

## Troubleshooting

**Stations log `Connection to node -1 could not be established` forever.**
The broker is unreachable on 9092. Check `CENTRAL_HOST` matches the central
VM's current IP, that both VMs really are in the same VNet, and — if you
split the VNets — that the NSG rule for 9092 exists.

**Stations connect, then time out sending.** Advertised-listener problem:
the broker handed back an address the stations cannot reach. Confirm what it
resolved to:

```bash
docker compose -f cloud/docker-compose.central.yml exec kafka env | grep ADVERTISED
```

**Central station exits with an SSL or authentication error.** Aiven
requires TLS — `DB_URL` must end in `?sslmode=require`. If it cannot connect
at all, the central VM's public IP is missing from the Aiven allowlist.

**A station container is killed and restarts repeatedly.** The stations VM
is out of memory. Each station needs ~78 MiB; ten plus the OS fit 2 GiB with
room to spare, so suspect a size smaller than `Standard_B1ms`.

**"This size is not available in this region" or a quota error.** See the
3 vCPU ceiling above — check Subscription → Usage + quotas, and try another
region if the size is simply unavailable.
