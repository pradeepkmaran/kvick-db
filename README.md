<div align="center">

# KVick-DB

**A high-performance, distributed, leaderless in-memory key-value store built in C++17**

Dynamo-style Architecture · Vector Clocks · SWIM Gossip · Consistent Hashing · Event-driven I/O

[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg)](https://isocpp.org/std/the-standard)
[![Docker](https://img.shields.io/badge/docker-ready-2496ED.svg)](Dockerfile)

</div>

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start (Docker)](#quick-start-docker)
- [Protocol & Usage](#protocol--usage)
- [Benchmarking](#benchmarking)
- [Configuration Reference](#configuration-reference)
- [Building from Source](#building-from-source)
- [Internals](#internals)
- [Performance Characteristics](#performance-characteristics)

---

## Overview

KVick-DB is a **leaderless, Dynamo-inspired** distributed key-value store designed for high availability and partition tolerance (**AP** in CAP terms). Unlike leader-based consensus systems (Raft/Paxos), every node in KVick-DB can independently coordinate reads and writes, eliminating single points of failure and leader bottlenecks.

### Key Features

| Feature | Description |
|---|---|
| **Leaderless Consensus** | Every node is a peer — no leader election, no single point of failure |
| **Vector Clocks** | Causal history tracking ensures zero data loss during concurrent writes |
| **Tunable Consistency** | Configurable `N`, `W`, `R` quorum parameters per request (default: N=3, W=2, R=2) |
| **Consistent Hashing** | 128-vnode hash ring with MurmurHash3 for balanced key partitioning |
| **Conflict Resolution** | Concurrent writes create siblings, returned to the client for resolution |
| **Read Repair** | Coordinator heals stale replicas automatically during read operations |
| **SWIM Gossip** | Failure detection with suspect phase, indirect pings, and piggybacked updates |
| **Sharded Storage** | 64-shard concurrent map with shared mutexes for ultra-low latency RAM lookups |
| **Event-driven I/O** | epoll-based TCP server with thread pool for handling client connections |
| **Binary Persistence** | KV03 format snapshots every 30s and on graceful shutdown |

---

## Architecture

```
                          ┌──────────────────────────────┐
                          │         Client (TCP)         │
                          └──────────┬───────────────────┘
                                     │  SET / GET / DEL
                                     ▼
                          ┌──────────────────────────────┐
                          │     Coordinator Node         │
                          │  ┌────────────────────────┐  │
                          │  │  Consistent Hash Ring  │  │
                          │  │   (128 vnodes/node)    │  │
                          │  └──────────┬─────────────┘  │
                          │             │                │
                          │  ┌──────────▼─────────────┐  │
                          │  │  Quorum Assembly       │  │
                          │  │  W writes / R reads    │  │
                          │  └──────────┬─────────────┘  │
                          └─────────────┼────────────────┘
                       ┌────────────────┼────────────────┐
                       ▼                ▼                ▼
               ┌────────────────┐ ┌────────────────┐ ┌────────────────┐
               │    Node 1      │ │    Node 2      │ │    Node 3      │
               │   (gRPC)       │ │   (gRPC)       │ │   (gRPC)       │
               │  ┌──────────┐  │ │  ┌──────────┐  │ │  ┌──────────┐  │
               │  │ KVic k   │  │ │  │ KVick    │  │ │  │ KVick    │  │
               │  │ Store    │  │ │  │ Store    │  │ │  │ Store    │  │
               │  │(64-shard)│  │ │  │(64-shard)│  │ │  │(64-shard)│  │
               │  └──────────┘  │ │  └──────────┘  │ │  └──────────┘  │
               └────────────────┘ └────────────────┘ └────────────────┘
                       ▲                ▲                ▲
                       └────────────────┼────────────────┘
                                  SWIM Gossip
                              (health + membership)
```

**Request flow:**
1. Client sends a command (SET/GET/DEL) over TCP to any node.
2. The receiving node acts as the coordinator and hashes the key to determine the N replica nodes.
3. For writes: the coordinator replicates to N nodes via gRPC and waits for W acknowledgements.
4. For reads: the coordinator queries N nodes via gRPC and waits for R responses, then reconciles using vector clock dominance.
5. Stale replicas are healed asynchronously via read repair.

---

## Quick Start (Docker)

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (v20.10+)
- [Docker Compose](https://docs.docker.com/compose/install/) (v2.0+)

### 1. Build the Docker image

```bash
docker build -t kvick-db .
```

### 2. Start a 3-node cluster

```bash
docker compose up -d
```

This starts three nodes with the following port mapping:

| Node | TCP (Client) | gRPC (Internal) |
|------|-------------|-----------------|
| node1 | `localhost:5000` | `localhost:50051` |
| node2 | `localhost:5001` | `localhost:50052` |
| node3 | `localhost:5002` | `localhost:50053` |

### 3. Verify the cluster is running

```bash
# Check all containers are up
docker compose ps

# Check cluster health via the INFO command
echo "INFO" | nc -q1 localhost 5000
```

Expected output:
```
node:node1 peers:3 advertise:node1:50051
```

### 4. Try some commands

```bash
# Write a key (to any node — it coordinates replication automatically)
printf "SET val1 100\n" | nc -N localhost 5000

# Read the key (from a different node to verify replication)
printf "GET val1\n" | nc -N localhost 5001

# Delete a key (causal tombstone)
printf "DEL val1\n" | nc -N localhost 5002
```

### 5. Stop the cluster

```bash
# Graceful shutdown (data is persisted to volumes)
docker compose down

# Full cleanup (removes persisted data volumes)
docker compose down -v
```

---

## Protocol & Usage

Connect via **TCP** (default port `5000`). Each command is a newline-terminated string.

### SET

```
SET <key> <value> [W=n] [context={"node":counter,...}]
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `key` | Yes | — | The key to store |
| `value` | Yes | — | The value to associate with the key |
| `W=n` | No | `2` | Write quorum — number of replicas that must acknowledge |
| `context={...}` | No | — | Causal context from a prior GET (for conflict resolution) |

**Examples:**

```bash
# Simple write
printf "SET mykey myvalue\n" | nc -N localhost 5000

# Write with explicit quorum
printf "SET mykey myvalue W=3\n" | nc -N localhost 5000

# Write with causal context (conflict resolution)
printf 'SET mykey newvalue context={"node1":5}\n' | nc -N localhost 5000
```

**Response:** `OK {"node1":6}` (the updated vector clock)

### GET

```
GET <key> [R=n]
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `key` | Yes | — | The key to retrieve |
| `R=n` | No | `2` | Read quorum — number of replicas that must respond |

**Example:**

```bash
printf "GET mykey\n" | nc -N localhost 5000
```

**Response (single value):**
```
myvalue context={"node1":6}
```

**Response (conflict — concurrent writes detected):**
```
valueA context={"node1":1} | valueB context={"node2":1}
```

> When siblings are returned, the client should resolve the conflict and write back with the appropriate `context` parameter.

### DEL

```
DEL <key> [W=n] [context={...}]
```

Inserts a **causal tombstone** — the key is logically deleted across replicas.

### INFO

```
INFO
```

Returns the node's identity, peer count, and advertise address.

---

## Benchmarking

KVick-DB includes a professional Python-based benchmarking utility to measure sustained throughput, latency distributions, and load balancing across the cluster.

### Running the Benchmark

Ensure your cluster is running (`docker compose up -d`), then run the script directly from your host:

```bash
# Basic run (8 threads, 10,000 ops)
python3 src/test/benchmark.py --host localhost --port 5000

# High-concurrency run
python3 src/test/benchmark.py --threads 16 --ops 100000
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--host` | `localhost` | The coordinator node address |
| `--port` | `5001` | The coordinator's TCP port |
| `--threads`| `8` | Number of concurrent client threads |
| `--ops` | `10000` | Total operations to perform per phase |

### Example Output

```text
=== SET (W=2) ===
Sample Mapping: SET bench_key_0 -> Quorum: node1, node2
Throughput:  3418 ops/sec
Latency (ms):
  Avg:       2.32
  P50:       1.86
  P95:       5.32
  P99:       9.12

Node Distribution (Quorum Appearances):
  node1     :   6650 hits (33.3%)
  node2     :   6680 hits (33.4%)
  node3     :   6670 hits (33.3%)
```

### Metrics Explained

- **Throughput**: Total operations completed per second by the coordinator.
- **P99 Latency**: The response time for the slowest 1% of requests, capturing network and gRPC jitter.
- **Node Distribution**: Shows how effectively **Consistent Hashing** is spreading the keys across physical servers. Each "hit" represents a node being part of a key's quorum (W or R).

---

## Configuration Reference

KVick-DB supports configuration via environment variables and CLI flags.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `5000` | TCP port for client connections |
| `GRPC_PORT` | `50051` | gRPC port for inter-node communication |
| `NODE_ID` | `node1` | Unique identifier for this node |
| `SEED_NODES` | (empty) | Comma-separated list of seed nodes (e.g. `node1:50051,node2:50051`) |
| `ADVERTISE_ADDRESS` | (auto) | Address advertised to peers (e.g. `node1:50051`) |
| `DATA_DIR` | `/app/data` | Directory for persistence snapshots |

### CLI Flags

```
--port <n>           TCP listen port
--id <name>          Node ID
--grpc <addr:port>   gRPC bind address
--advertise <addr>   gRPC address advertised to peers
--seed <addr>        Seed node address (can be repeated)
--data-dir <path>    Data persistence directory
```

---

## Building from Source

### Requirements
`cmake >= 3.14`, `g++ (C++17)`, `libgrpc++-dev`, `libprotobuf-dev`, `libasio-dev`, `libssl-dev`, `zlib1g-dev`.

### Build

```bash
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

---

## Internals

### Data Persistence
Each node writes periodic snapshots to `<DATA_DIR>/kvick_data_<node_id>.bin` using the **KV03** binary format. It stores all sibling versions and vector clocks. Snapshots occur every 30 seconds and on graceful shutdown.

### Consistency & Reconciliation
When a coordinator receives a `GET`, it queries `R` replicas in parallel. It gathering all siblings and performs a **dominance check** to resolve versions. Stale replicas are healed via asynchronous **read repair**.

---

## Performance Characteristics

Benchmarked on a 3-node cluster (Docker, localhost):

| Operation | Avg Latency | Throughput |
|-----------|------------|------------|
| `SET (W=2)` | **~0.18 ms** | **~5,500 ops/sec** |
| `GET (R=2)` | **~0.17 ms** | **~5,800 ops/sec** |

---

## Project Structure

```
kvick-db/
├── Dockerfile                    # Multi-stage Docker build
├── docker-compose.yaml           # 3-node cluster definition
├── src/
│   ├── main.cpp                  # Entry point
│   ├── core/                     # Storage engine & vector clocks
│   ├── network/                  # TCP/epoll server & gRPC/Quorum logic
│   ├── utils/                    # Hash & Serialization
│   └── test/                     # Benchmark tool
```
