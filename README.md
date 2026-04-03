# KVick-DB: Distributed Key-Value Store

KVick-DB is a high-performance, resilient, and distributed Key-Value store built in C++. It features Raft consensus for consistency, SWIM gossip for cluster membership, and consistent hashing for partitioning.

## Core Features

- **Distributed Consensus**: Leader election and strongly consistent writes via Raft (NuRaft).
- **Partitioning**: Consistent hashing with 128 virtual nodes to balance the keyspace.
- **Cluster Membership**: SWIM gossip protocol for dynamic node discovery and failure detection.
- **High Performance**:
  - **Lock-Free Reads**: Atomic map swapping with `std::shared_ptr` COW for non-blocking reads.
  - **Sharded Map**: Sharding across 64 mutexes to eliminate write contention.
  - **Event-Driven Networking**: `epoll`-based event loop with a worker thread pool.
  - **Pipelining**: Multiple commands processed in a single TCP stream.
- **Persistence**: Binary snapshotting (`KV01` format) and Write-Ahead Log (WAL) for crash resilience.
- **Transparent Proxying**: Requests to a node that doesn't own a key are automatically forwarded via gRPC to the correct node.

## Build Requirements

- `cmake` >= 3.14
- `g++` with C++17 support
- `libgrpc++-dev`, `libprotobuf-dev`, `protobuf-compiler-grpc`, `protobuf-compiler`
- `libasio-dev`
- `libssl-dev`, `zlib1g-dev`
- [NuRaft](https://github.com/eBay/NuRaft) (built and installed automatically via Docker)

## Build with Docker

```bash
sudo docker build -t kvick-db .
```

> **Note:** On Linux, prefix `docker` commands with `sudo` unless your user is in the `docker` group.  
> To add yourself: `sudo usermod -aG docker $USER` (then log out and back in).

## Running a Cluster

Each node needs a unique ID, TCP port, and gRPC port. Data is persisted to `/app/data/` inside the container — mount a volume if you want persistence across restarts.

### Start Node 1 (Seed Node)

```bash
sudo docker run -it --rm \
  -p 8081:8081 -p 50051:50051 \
  -v kvick-node1-data:/app/data \
  -e PORT=8081 -e NODE_ID=node1 -e GRPC_PORT=50051 \
  kvick-db
```

### Start Node 2 (Joining Node 1)

```bash
sudo docker run -it --rm \
  -p 8082:8082 -p 50052:50052 \
  -v kvick-node2-data:/app/data \
  -e PORT=8082 -e NODE_ID=node2 -e GRPC_PORT=50052 \
  -e SEED_NODES=<node1-address>:50051 \
  kvick-db
```

**Resolving Node 1's address:**

| Environment | Address to use |
|---|---|
| Docker Desktop (Mac/Windows) | `host.docker.internal` |
| Linux (host network) | `172.17.0.1` (default Docker bridge gateway) |
| Docker Compose | service name, e.g. `node1` |

## Client Protocol

KVick uses a plain-text protocol over TCP, compatible with `nc` or `telnet`.

### Commands

| Command | Description |
|---|---|
| `SET <key> <value>` | Store a value. Type is inferred automatically. |
| `GET <key>` | Retrieve a value. |
| `DEL <key>` | Delete a key. |

**Supported value types:** `int`, `double`, `bool`, `string`, `[list]`

```bash
# Examples
echo "SET counter 42"          | nc localhost 8081
echo "SET ratio 3.14"          | nc localhost 8081
echo "SET flag true"           | nc localhost 8081
echo "SET name 'hello world'"  | nc localhost 8081
echo "SET scores [1,2,3]"      | nc localhost 8081
echo "GET counter"             | nc localhost 8081
echo "DEL flag"                | nc localhost 8081
```

## Data Persistence

Each node writes two files to `/app/data/`:

- `kvick_data_<node_id>.bin` — Binary snapshot in `KV01` format, flushed every 30 seconds and on shutdown.
- `kvick_<node_id>.wal` — Write-Ahead Log, replayed on startup before the snapshot is loaded.

Mount a named volume (`-v kvick-node1-data:/app/data`) to persist data across container restarts.

## Architecture

```
Client (TCP)
     │
     ▼
KVickServer (epoll + thread pool)
     │
     ├─► Local key? ──► KVick store (sharded map, COW reads)
     │
     └─► Remote key? ──► gRPC proxy ──► Correct node
                                │
                          ClusterManager
                         (SWIM gossip + consistent hash ring)
                                │
                          NuRaft (Raft consensus)
```

- **Consistent Hashing**: 128 virtual nodes per peer distribute the keyspace evenly.
- **Non-Blocking Snapshots**: Snapshots capture an immutable view of each shard, so disk I/O never blocks concurrent writes.
- **SWIM Gossip**: Nodes periodically probe peers and use indirect ping-req for robust failure detection without a central coordinator.