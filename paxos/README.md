# Fall 25 Distributed Systems – Project 1 (Paxos)

A simple local demo of a Paxos-based replicated service with ten client processes and five node processes.

## Prerequisites
- **Java 21** (the Gradle wrapper will handle Gradle itself)
- **macOS/Linux shell** (commands below use `./gradlew`)
- **Ports 51051–51055** available on `127.0.0.1`

## Build
```bash
./gradlew clean build
```

## Run the five nodes
Open five separate terminals, one per node, and run the following from the repository root:

- Terminal 1:
```bash
./gradlew :node:runNode1
```
- Terminal 2:
```bash
./gradlew :node:runNode2
```
- Terminal 3:
```bash
./gradlew :node:runNode3
```
- Terminal 4:
```bash
./gradlew :node:runNode4
```
- Terminal 5:
```bash
./gradlew :node:runNode5
```

These start nodes `n1`–`n5` on `127.0.0.1:51051`–`51055`.

## Run the client
Open a new terminal and run:
```bash
./gradlew :client:runClient
```
This launches a single client process that spawns 10 client workers (A–J) and executes CSV-defined test sets.

## Advancing between sets
When a set finishes you will see a prompt like:
```
Set N complete. Press Enter to continue...
client>
```
Press **Enter** (on a blank line at the `client>` prompt) to proceed to the next set.

## Client console commands
You can inspect cluster state any time during or after execution:
- **log** – Print transaction log from all nodes
- **db** – Print database/balances from all nodes
- **status <seq>** – Print status of a sequence number across all nodes
- **view** – Print view history (leader elections)
- **verify** – Verify database consistency across nodes
- **help** – Show help
- **quit** – Exit the client

Note: These inspection commands work even when nodes are temporarily inactive/frozen between sets.

## Customization (optional)
- Change CSV file for the client:
```bash
./gradlew :client:runClient -PpaxosClientCsv=path/to/your.csv
```
- Run nodes against a different host (defaults to 127.0.0.1):
```bash
./gradlew :node:runNode1 -PpaxosNodeHost=192.168.1.100
```
- Override client node targets:
```bash
./gradlew :client:runClient -PpaxosNodes="n1=127.0.0.1:51051,n2=127.0.0.1:51052,n3=127.0.0.1:51053,n4=127.0.0.1:51054,n5=127.0.0.1:51055"
```

## Stopping
- **Client**: type `quit` at `client>` or press Ctrl+C.
- **Nodes**: press Ctrl+C in each node terminal.

## Troubleshooting
- **Port in use**: ensure ports 51051–51055 are free or adjust ports in Gradle files.
- **Java version**: verify `java -version` reports 21.
