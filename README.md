# KVick-DB

**A high-performance, distributed, in-memory key-value store** built in C++17.  
Raft consensus · SWIM gossip · Sharded RAM storage · Event-driven networking

---

## Performance

Benchmarked on a single node (Docker, localhost), sequential request/response:

| Operation | Latency | Throughput |
|---|---|---|
| `SET` | **0.266 ms** | **3,756 ops/sec** |
| `GET` | **0.140 ms** | **7,120 ops/sec** |

> GET is 2× faster than SET — SET goes through Raft consensus, GET is a pure RAM lookup.

### How KvickDB compares

| System | Write Throughput | Notes |
|---|---|---|
| **KvickDB** | **~3,800 ops/sec** | Raft consensus, in-memory |
| etcd | ~10,000 ops/sec | Raft consensus, production-grade |
| Zookeeper | ~10,000 ops/sec | ZAB consensus |
| CockroachDB | ~5,000 ops/sec | Raft + SQL overhead |
| Redis (single) | ~100,000 ops/sec | No consensus, single-threaded |

KvickDB sits in the **etcd/Zookeeper tier** — the bottleneck is intentional. Every write is linearizable through Raft, which means strong consistency guarantees that Redis simply doesn't offer.

### Run the benchmark yourself

```bash
# SET throughput
python3 -c "
import socket, time
s = socket.socket()
s.connect(('localhost', 8081))
start = time.time()
for i in range(10000):
    s.sendall(f'SET key{i} value{i}\n'.encode())
    s.recv(64)
elapsed = time.time() - start
print(f'10000 SETs in {elapsed:.2f}s')
print(f'avg latency: {(elapsed/10000)*1000:.3f}ms')
print(f'throughput:  {10000/elapsed:.0f} ops/sec')
s.close()
"

# GET throughput
python3 -c "
import socket, time
s = socket.socket()
s.connect(('localhost', 8081))
s.sendall(b'SET bench testvalue\n')
s.recv(64)
start = time.time()
for i in range(10000):
    s.sendall(b'GET bench\n')
    s.recv(64)
elapsed = time.time() - start
print(f'10000 GETs in {elapsed:.2f}s')
print(f'avg latency: {(elapsed/10000)*1000:.3f}ms')
print(f'throughput:  {10000/elapsed:.0f} ops/sec')
s.close()
"
```

---

## Architecture

```
Client (TCP)
     │
     ▼
KVickServer (epoll + thread pool)
     │
     ├─► GET ──► Sharded RAM map (shared_lock, ~100ns lookup)
     │
     └─► SET / DEL
           │
           ├─► Leader ──► RaftManager.proposeWrite()
           │                    │
           │              NuRaft (Raft consensus)
           │                    │
           │              KVickStateMachine.commit()
           │                    │
           │              KVick store (apply to RAM)
           │
           └─► Follower ──► gRPC proxy ──► Leader node
                                   │
                         ClusterManager
                        (SWIM gossip + hash ring)
```

### Storage model

All data lives in RAM — a 64-shard concurrent hash map with `shared_mutex` per shard. Reads never block each other. Writes lock only their own shard.

Disk is touched in exactly three situations:
- **Every 30 seconds** — async snapshot flushed to `kvick_data_<node_id>.bin`
- **On shutdown** — final snapshot written before exit
- **On startup** — snapshot loaded once into RAM, then disk is never read again

The Raft log (`inmem_log_store`) is also in RAM, giving crash recovery through log replay on restart.

---

## Core Features

- **Linearizable writes** — all SET/DEL go through the Raft leader and are replicated to a quorum before responding
- **Low-latency reads** — served from each node's local RAM copy, no Raft involvement
- **SWIM gossip** — indirect probing with two-phase suspect→dead failure detection, no central coordinator
- **Consistent hashing** — 128 virtual nodes per peer for even keyspace distribution
- **Transparent proxying** — writes to a follower are automatically forwarded to the leader via gRPC
- **Portable hashing** — MurmurHash3 (32-bit) for deterministic cross-platform key distribution
- **Binary persistence** — `KV02` format with full type fidelity (`int64`, `double`, `bool`, `string`, `[list]`)
- **Graceful shutdown** — SIGINT/SIGTERM handlers persist data and drain connections cleanly

---

## Build

### Docker (recommended)

```bash
docker build -t kvick-db .
```

### Requirements (manual build)

- `cmake >= 3.14`, `g++` with C++17
- `libgrpc++-dev`, `libprotobuf-dev`, `protobuf-compiler-grpc`, `protobuf-compiler`
- `libasio-dev`, `libssl-dev`, `zlib1g-dev`, `pkg-config`
- [NuRaft](https://github.com/eBay/NuRaft) — built and installed automatically via Docker

---

## Running a Cluster

Each node needs a unique ID, a TCP port (clients), a gRPC port (gossip + proxy), and a Raft port (consensus).

### Single node

```bash
docker run -it --rm \
  -p 8081:8081 -p 50051:50051 -p 10051:10051 \
  -v kvick-node1-data:/app/data \
  -e PORT=8081 -e NODE_ID=node1 \
  -e GRPC_PORT=50051 -e RAFT_PORT=10051 \
  kvick-db
```

### Multi-node cluster (Docker Compose)

```yaml
version: '3.8'

services:
  seed:
    image: kvick-db
    container_name: kvick-seed
    # Exposing ports for the seed node so the host can send KV client commands to it
    ports: ["8080:8080"]
    volumes: [kvick-seed-data:/app/data]
    environment:
      PORT: 8080
      GRPC_PORT: 50051
      RAFT_PORT: 10051
      SEED_NODES: kvick-seed:50051
      ADVERTISE_ADDRESS: kvick-seed:50051
    networks: [kvick-net]

  worker:
    image: kvick-db
    deploy:
      replicas: 4 # Scale natively mapping directly out to the swarm/docker engine
    environment:
      PORT: 8080
      GRPC_PORT: 50051
      RAFT_PORT: 10051
      SEED_NODES: kvick-seed:50051
    # We omit ADVERTISE_ADDRESS and NODE_ID so it dynamically picks up the generated hostname
    # We do NOT map ports dynamically to avoid host collision when scaling
    depends_on: [seed]
    networks: [kvick-net]

volumes:
  kvick-seed-data:

networks:
  kvick-net:
    driver: bridge
```

```bash
docker compose up
```

---

## Client Protocol

Plain-text TCP — works with `nc`, `telnet`, or any socket library.

| Command | Description |
|---|---|
| `SET <key> <value>` | Store a value. Routed to Raft leader. |
| `GET <key>` | Retrieve a value. Served locally from RAM. |
| `DEL <key>` | Delete a key. Routed to Raft leader. |

**Supported value types:** `int64`, `double`, `bool`, `string`, `[list]`

```bash
echo "SET counter 42"         | nc localhost 8081
echo "SET ratio 3.14"         | nc localhost 8081
echo "SET flag true"          | nc localhost 8081
echo "SET name hello"         | nc localhost 8081
echo "SET scores [1,2,3]"     | nc localhost 8081
echo "GET counter"            | nc localhost 8081
echo "GET counter"            | nc localhost 8082   # works from any node
echo "DEL flag"               | nc localhost 8081
```

---

## Consistency Model

**Writes** are linearizable — SET/DEL go through the Raft leader, which replicates to a majority before responding. No write is acknowledged until committed.

**Reads** are served locally from each node's replicated RAM store. A follower may lag a few milliseconds behind the leader. For strictly linearizable reads, send GETs to the leader node.

---

## Data Persistence

Each node writes to its data directory:

| File | Description |
|---|---|
| `kvick_data_<node_id>.bin` | Binary snapshot (`KV02` format), flushed every 30s and on shutdown |
| `raft_config.bin` | Raft cluster configuration |
| `raft_state.bin` | Raft server state (term, vote) |

Mount a named volume to persist data across container restarts:
```bash
-v kvick-node1-data:/app/data
```

---

## Internals

### Write path latency breakdown

```
SET key value
  └─► TCP recv                     ~0.01ms
  └─► Raft proposeWrite()
        └─► Leader serializes log entry
        └─► Replicates to followers (heartbeat interval: 100ms)
        └─► Majority ack → commit   ~0.20ms  ← dominates
  └─► KVickStateMachine::commit()
        └─► KVick::set() → shard lock → unordered_map insert  ~0.001ms
  └─► TCP send "OK"                ~0.01ms
─────────────────────────────────────────
Total                              ~0.26ms
```

### Why GET is 2× faster than SET

GET bypasses Raft entirely. It acquires a `shared_lock` on one of 64 shards, does an `unordered_map` lookup, and returns. The pure lookup is under 1µs — the remaining 0.14ms is TCP round-trip and kernel overhead.

### Sharded map design

64 shards, each with its own `shared_mutex`. Readers on different shards never block each other. The shard index is `MurmurHash3(key) % 64`, giving even distribution across shards.