# ClickHouse High Availability — Production Setup & Replication Runbook

> Migrate a standalone ClickHouse EC2 instance to a fault-tolerant **2-node replicated cluster** using ClickHouse Keeper.

## 1. Overview & Architecture

This runbook describes how to migrate a standalone ClickHouse EC2 instance to a production-ready, fault-tolerant 2-node replicated cluster backed by ClickHouse Keeper. Following these steps eliminates the single point of failure inherent in a standalone deployment and guarantees data durability through replication.

### 1.1 Target Architecture

```
                    ┌─────────────────────────┐
                    │   HAPROXY / NETWORK LB  │
                    └────────────┬────────────┘
                                 │
              ┌──────────────────┴──────────────────┐
              │                                      │
   ┌──────────▼──────────┐            ┌──────────────▼──────────┐
   │   Node 1 (Primary)  │◄──────────►│   Node 2 (Replica)      │
   │   AZ: ap-south-1a   │            │   AZ: ap-south-1b       │
   └──────────┬──────────┘            └──────────────┬──────────┘
              │                                      │
   ┌──────────▼──────────────────────────────────────▼──────────┐
   │                     ClickHouse Keeper                       │
   │            (runs on both nodes, ports 9181 / 9234)          │
   └─────────────────────────────────────────────────────────────┘
```

### 1.2 Port Requirements

| Port | Protocol | Purpose | Direction |
|---|---|---|---|
| 9000 | TCP | ClickHouse native protocol (inter-node) | Both nodes ↔ each other |
| 9009 | TCP | ClickHouse replication data transfer | Both nodes ↔ each other |
| 8123 | TCP | HTTP interface | App → both nodes |
| 9181 | TCP | ClickHouse Keeper client port | Both nodes ↔ each other |
| 9234 | TCP | Keeper Raft consensus | Both nodes ↔ each other |

---

## 2. Launch & Prepare Nodes

### 2.1 Launch the First Node

1. Create an AMI of the currently running ClickHouse instance.
2. Launch a new instance from that AMI in availability zone `ap-south-1a`.
3. Add the required inbound rules to the security group (9000, 9009, 8123, 9181, 9234).
4. Launch the instance.

### 2.2 Launch the Second Node

1. Launch a fresh EC2 instance (preferably using the same OS version as the first node).
2. Deploy it in availability zone `ap-south-1b`.
3. Add the required inbound rules to the security group (9000, 9009, 8123, 9181, 9234).
4. Connect to the CLI of the second node.
5. Install the same version of ClickHouse using the commands below, and set the same `default` user password as the first node when prompted.

```bash
sudo apt-get update
sudo apt-get install -y apt-transport-https ca-certificates dirmngr gnupg

sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key | \
    sudo gpg --dearmor -o /etc/apt/keyrings/clickhouse.gpg

echo "deb [signed-by=/etc/apt/keyrings/clickhouse.gpg] https://packages.clickhouse.com/deb stable main" | \
    sudo tee /etc/apt/sources.list.d/clickhouse.list

sudo apt-get update

sudo apt-get install -y \
    clickhouse-common-static=24.2.2.71 \
    clickhouse-client=24.2.2.71 \
    clickhouse-server=24.2.2.71
```

### 2.3 Record Both Node IPs

Note the private IP address of each node — these values are referenced throughout the rest of the runbook.

`Node 1 IP = <Node1_IP>`

`Node 2 IP = <Node2_IP>`

---

## 3. Configure ClickHouse Keeper

ClickHouse Keeper replaces ZooKeeper for replication coordination.

### 3.1 Keeper Config on Node 1 (`server_id: 1`)

```bash
sudo vim /etc/clickhouse-server/config.d/keeper.xml
```

Replace `<Node1_IP>` and `<Node2_IP>` with the actual IP addresses.

```xml
<!-- /etc/clickhouse-server/config.d/keeper.xml -->
<clickhouse>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>30000</session_timeout_ms>
            <raft_logs_level>warning</raft_logs_level>
        </coordination_settings>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname><Node1_IP></hostname>
                <port>9234</port>
            </server>
            <server>
                <id>2</id>
                <hostname><Node2_IP></hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
```

### 3.2 Keeper Config on Node 2 (`server_id: 2`)

```bash
sudo vim /etc/clickhouse-server/config.d/keeper.xml
```

Replace `<Node1_IP>` and `<Node2_IP>` with the actual IP addresses.

```xml
<clickhouse>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>2</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>30000</session_timeout_ms>
            <raft_logs_level>warning</raft_logs_level>
        </coordination_settings>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname><Node1_IP></hostname>
                <port>9234</port>
            </server>
            <server>
                <id>2</id>
                <hostname><Node2_IP></hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
```

### 3.3 Verify Keeper is Running

```bash
# Restart both nodes after adding the Keeper config
sudo systemctl restart clickhouse-server

# Run on either node — should return 2 rows
clickhouse-client --password 'YOUR_PASSWORD' \
  --query "SELECT * FROM system.zookeeper WHERE path = '/';"

# Expected output:
#   keeper      /
#   clickhouse  /
```

---

## 4. Configure Cluster & Replication

### 4.1 Remote Servers Config — Node 1

```bash
sudo vim /etc/clickhouse-server/config.d/remote_servers.xml
```

```xml
<clickhouse>
    <remote_servers>
        <reelo_cluster>
            <shard>
                <replica>
                    <host><Node1_IP></host>
                    <port>9000</port>
                    <user>default</user>
                    <password>YOUR_PASSWORD</password>
                </replica>
                <replica>
                    <host><Node2_IP></host>
                    <port>9000</port>
                    <user>default</user>
                    <password>YOUR_PASSWORD</password>
                </replica>
            </shard>
        </reelo_cluster>
    </remote_servers>
    <zookeeper>
        <node><host><Node1_IP></host><port>9181</port></node>
        <node><host><Node2_IP></host><port>9181</port></node>
    </zookeeper>
    <macros>
        <cluster>reelo_cluster</cluster>
        <shard>01</shard>
        <replica>node1</replica>
    </macros>
</clickhouse>
```

### 4.2 Remote Servers Config — Node 2

```bash
sudo vim /etc/clickhouse-server/config.d/remote_servers.xml
```

```xml
<clickhouse>
    <remote_servers>
        <reelo_cluster>
            <shard>
                <replica>
                    <host><Node1_IP></host>
                    <port>9000</port>
                    <user>default</user>
                    <password>YOUR_PASSWORD</password>
                </replica>
                <replica>
                    <host><Node2_IP></host>
                    <port>9000</port>
                    <user>default</user>
                    <password>YOUR_PASSWORD</password>
                </replica>
            </shard>
        </reelo_cluster>
    </remote_servers>
    <zookeeper>
        <node><host><Node1_IP></host><port>9181</port></node>
        <node><host><Node2_IP></host><port>9181</port></node>
    </zookeeper>
    <macros>
        <cluster>reelo_cluster</cluster>
        <shard>01</shard>
        <replica>node2</replica>
    </macros>
</clickhouse>
```

> **Note:** The only difference between the two configs is the `<replica>` macro (`node1` vs `node2`). Everything else is identical.

### 4.3 Enable the Experimental Object Type (Both Nodes)

Required for tables with `Object('json')` columns.

```bash
sudo vim /etc/clickhouse-server/users.d/settings.xml
```

```xml
<clickhouse>
    <profiles>
        <default>
            <allow_experimental_object_type>1</allow_experimental_object_type>
        </default>
    </profiles>
</clickhouse>
```

### 4.4 Allow Listening for ClickHouse (Second Node)

Open the ClickHouse configuration file:

```bash
sudo vim /etc/clickhouse-server/config.xml
```

Update the `listen_host` setting to allow ClickHouse to accept connections from all network interfaces:

```xml
<listen_host>0.0.0.0</listen_host>
```

### 4.5 Verify the Cluster is Configured

Restart both nodes, then confirm the cluster topology:

```bash
sudo systemctl restart clickhouse-server

clickhouse-client --password 'YOUR_PASSWORD' \
  --query "SELECT * FROM system.clusters WHERE cluster = 'reelo_cluster';"

# Expected: 2 rows, one per replica
#   reelo_cluster  1  1  0  1  172.31.46.46  ...  is_local=1
#   reelo_cluster  1  1  0  2  172.31.45.2   ...  is_local=0
```

---

## 5. Table Migration to Replicated Engines

### 5.1 Engine Mapping

| Original Engine | Replicated Engine |
|---|---|
| `MergeTree` | `ReplicatedMergeTree('/path/', '{replica}')` |
| `ReplacingMergeTree(ver)` | `ReplicatedReplacingMergeTree('/path/', '{replica}', ver)` |
| `SummingMergeTree(col)` | `ReplicatedSummingMergeTree('/path/', '{replica}', col)` |
| `AggregatingMergeTree()` | `ReplicatedAggregatingMergeTree('/path/', '{replica}')` |

### 5.2 Manual Migration Pattern (Single Table)

```sql
-- Step 1: Rename the original as a backup
RENAME TABLE db.table_name TO db.table_name_migration_backup;

-- Step 2: Create the replicated version
SET allow_experimental_object_type = 1;
CREATE TABLE db.table_name ( /* ...same columns... */ )
ENGINE = ReplicatedReplacingMergeTree(
  '/clickhouse/tables/reelo_cluster/01/db/table_name',
  '{replica}',
  version_column
)
PARTITION BY ...
ORDER BY ...
SETTINGS index_granularity = 8192;

-- Step 3: Copy the data
INSERT INTO db.table_name SELECT * FROM db.table_name_migration_backup;

-- Step 4: Verify the counts match
SELECT
  (SELECT count() FROM db.table_name_migration_backup) AS old_count,
  (SELECT count() FROM db.table_name)                  AS new_count;

-- Step 5: Create the empty replica on Node 2 (it auto-syncs)
--   Run the same CREATE TABLE on Node 2 with NO INSERT

-- Step 6: Verify the sync, then drop the backup
DROP TABLE db.table_name_migration_backup;
```

### 5.3 Automated Migration Script

Script path:

```
https://github.com/goldybawa48/clickhouse-migration-stand-alone-to-cluster-mode/blob/main/migration_script.py
```

Install the Python driver on Node 1:

```bash
pip3 install clickhouse-driver
```

Save the script to `/home/ubuntu/migrate_replication.py` and set these key values:

```python
NODE1    = '<Node1_IP>'
NODE2    = '<Node2_IP>'
PASSWORD = 'YOUR_PASSWORD'
DATABASE = 'YOUR_DATABASE'
CLUSTER  = 'CLUSTER_NAME'

# Tables to skip (already migrated, views, junk)
SKIP_TABLES = { 'already_done_table', ... }
```

The script will:

1. Automatically filter out tables that are already `Replicated`.
2. Filter out `_rep_backup` and `.inner_id.*` tables.
3. Rename → create replicated → copy data → create on Node 2 → verify sync.
4. Automatically roll back the rename if the `CREATE` fails.

Run the script:

```bash
python3 /home/ubuntu/migrate_replication.py
```

---

## 6. Verification Checklist

### 6.1 Post-Migration Verification Queries

```sql
-- Confirm all tables now use Replicated engines
SELECT name, engine, total_rows
FROM system.tables
WHERE database = 'reelo_development'
  AND engine LIKE 'Replicated%'
ORDER BY name;

-- Check replication health — every row must show:
-- total_replicas = 2, active_replicas = 2, queue_size = 0
SELECT table, is_leader, total_replicas, active_replicas, queue_size
FROM system.replicas
WHERE database = 'reelo_development'
ORDER BY table;
```

---

## 7. Create the Network Load Balancer (NLB)

### 7.1 Steps

1. Go to **Load Balancers** in the AWS console.
2. Create a Network Load Balancer.
3. Set its scheme to **internal**.
4. Create target groups for ports 8123 and 9000, and register both nodes.

![AWS Network Load Balancer Configuration](https://github.com/user-attachments/assets/64ea519c-6275-4df3-9738-c0c6f573688d)

### 7.2 Failover Test

Run the following query to confirm connectivity through the NLB:

```bash
clickhouse-client --host 'NLB_DNS' --password 'PASSWORD' \
  --query "SHOW TABLES FROM your_database;"
```

To validate failover:

1. Run the command several times and observe which node serves the response.
2. Stop the ClickHouse service on the node currently serving traffic.
3. Keep running the command in a loop.
4. If the command continues to return output after the service is stopped, the NLB is correctly routing around the failed node — failover is working.

Once failover is confirmed, the cluster is ready to be mapped to the application.

---

## 8. Cut Over Traffic from Standalone to the Cluster

### 8.1 Update the Application Environment

1. Open the application's `.env` file and update the ClickHouse credentials, replacing the standalone host with the `NLB_DNS`.
2. Restart the application, then test the full workflow end to end from the application side to confirm everything works as expected.

---