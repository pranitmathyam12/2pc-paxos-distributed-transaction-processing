# ShardLedger

**ShardLedger** is a fault-tolerant distributed transaction processing system that supports
**strongly consistent, sharded transactions**. It combines **Multi-Paxos** for replication with
**Two-Phase Commit (2PC)** for atomic cross-shard transactions, closely modeling the core
architecture used in modern distributed databases and financial systems.

The system is designed to be **crash-resilient, deterministic, observable, and easy to run locally**.

---

## Highlights

- Strong consistency (linearizability) using **Multi-Paxos**
- Fault tolerance under leader and replica failures
- Sharded architecture with replicated clusters
- Atomic cross-shard transactions via **Two-Phase Commit**
- Write-ahead logging (WAL) for safe aborts and recovery
- Dynamic, workload-driven resharding
- Built-in benchmarking (throughput & latency)
- Rich runtime inspection (logs, DB state, views)

---

## System Architecture

- **9 nodes** organized into **3 clusters**
- Each cluster owns **one data shard**, replicated across 3 nodes
- Each cluster independently elects a Paxos leader
- Clients route requests using a shard map
- Cross-shard transactions are coordinated using 2PC on top of Paxos

There is **no shared memory** between nodes. All coordination happens via RPC-style messaging.

---

## Transaction Types

### Read-Only
- Served directly by the shard leader
- No consensus required

### Intra-Shard Transfers
- Fully replicated using Multi-Paxos
- Ordered, deterministic execution

### Cross-Shard Transfers
- Atomic across shards using Two-Phase Commit
- Paxos-backed durability for both prepare and commit phases
- WAL-based rollback on aborts

The system guarantees **exactly-once execution** and deterministic recovery.

---

## Tech Stack

- **Language**: Java 21
- **Build Tool**: Gradle
- **Architecture**: Multi-process replicated state machines
- **Communication**: RPC (gRPC-style)
- **Persistence**: Replicated logs + write-ahead logging (WAL)

---

## Getting Started

### Prerequisites
- Java 21+
- macOS or Linux
- Ports `51051–51059` available on `127.0.0.1`

---

### Build
```bash
./gradlew clean build

## Run Nodes Individually (Optional)

Run each command in a separate terminal:

```bash
./gradlew :node:runNode1
./gradlew :node:runNode2
./gradlew :node:runNode3
./gradlew :node:runNode4
./gradlew :node:runNode5
./gradlew :node:runNode6
./gradlew :node:runNode7
./gradlew :node:runNode8
./gradlew :node:runNode9
```

### Run the Client
```bash
./gradlew :client:runClient
```

The client spawns multiple workers and executes transaction workloads defined via CSV files.

### Advancing Between Workloads

After completing a workload set, the client pauses:

```text
Set N complete. Press Enter to continue...
client>
```

Press Enter to proceed to the next set.

## Runtime Inspection Commands

Available from the client console at any time:

- `db` — print balances from all nodes
- `log` — print replicated logs
- `status <seq>` — transaction status per node
- `view` — leader election history
- `verify` — consistency check across replicas
- `reshard plan [n] [tol] [seeds]` — generate a new shard mapping
- `reshard apply` — apply the new shard mapping
- `help` — list commands
- `quit` — exit client

These commands work even if nodes are temporarily failed between workloads.

## Resharding Workflow

ShardLedger supports offline, workload-driven resharding to reduce cross-shard traffic and balance load.

### Steps

1. **Run workloads**
   Transaction history is recorded automatically.

2. **Generate a plan**
   ```text
   client> reshard plan [n] [tol] [seeds]
   ```
   - `n` (default: recent transaction count)
   - `tol` (balance tolerance)
   - `seeds` (multi-start optimization runs)

3. **Stop client and nodes**
   No traffic during migration.

4. **Run offline migration**
   ```bash
   ./gradlew :node:runOfflineMigrator \
     -PmigrNew=client/shard-map.new.json \
     -PmigrOld=client/shard-map.json
   ```

5. **Restart nodes**
   ```bash
   ./gradlew :node:runNodes
   ```

6. **Restart client**
   ```bash
   ./gradlew :client:runClient
   ```

7. **Apply the new mapping**
   ```text
   client> reshard apply
   ```
   New transactions will now follow the updated shard assignment.

## Benchmarking

ShardLedger reports:
- Throughput (transactions per second)
- End-to-end latency

Benchmarks support configurable:
- Read vs write ratio
- Intra-shard vs cross-shard ratio
- Uniform or skewed (hot-key) access patterns

## Customization

### Use a Custom Workload
```bash
./gradlew :client:runClient -PpaxosClientCsv=path/to/workload.csv
```

### Change Node Host
```bash
./gradlew :node:runNode1 -PpaxosNodeHost=192.168.1.100
```

### Override Node Addresses
```bash
./gradlew :client:runClient \
  -PpaxosNodes="n1=127.0.0.1:51051,n2=127.0.0.1:51052,n3=127.0.0.1:51053,\
               n4=127.0.0.1:51054,n5=127.0.0.1:51055,n6=127.0.0.1:51056,\
               n7=127.0.0.1:51057,n8=127.0.0.1:51058,n9=127.0.0.1:51059"
```

## Stopping the System

- **Client**: type `quit` or press `Ctrl+C`
- **Nodes**: press `Ctrl+C` in each node terminal

## Troubleshooting

- **Ports in use**: ensure ports `51051–51059` are free
- **Wrong Java version**: verify with `java -version` (must be 21)
- **Inconsistent state**: run `verify` from the client console

## Why ShardLedger

ShardLedger demonstrates real-world distributed systems engineering:
- Consensus and leader election
- Replicated state machines
- Atomic transactions across shards
- Failure handling and recovery
- Performance measurement and observability

The design closely mirrors systems used in distributed databases, payment platforms, and cloud infrastructure.
