# KVick-DB: Distributed Key-Value Store

KVick-DB is a high-performance, resilient, and distributed Key-Value store built with C++. It features Raft consensus for consistency, SWIM gossip for cluster membership, and consistent hashing for partitioning.

## Core Features
*   **Distributed Consensus**: Leader election and strongly consistent writes via Raft (NuRaft).
*   **Partitioning**: Consistent hashing with 128 virtual nodes to balance the keyspace.
*   **Cluster Membership**: SWIM gossip protocol for dynamic node discovery and failure detection.
*   **High Performance**:
    *   **Lock-Free Reads**: Uses atomic map swapping with `std::shared_ptr` COW for non-blocking reads.
    *   **Sharded Map**: Sharding across 64 mutexes to eliminate write contention.
    *   **Event-Driven Networking**: `epoll`-based event loop with a worker thread pool.
    *   **Pipelining**: Support for processing multiple commands in a single TCP stream.
*   **Persistence**: Binary snapshotting and Write-Ahead Log (WAL) for crash resilience.

## Build Requirements
*   `cmake` (>= 3.14)
*   `g++` (C++17)
*   `gRPC` & `Protobuf`
*   `OpenSSL` & `ZLIB`

## Build with Docker

```bash
docker build -t kvick-db .
```

## Running a Cluster

You can run multiple nodes locally using Docker. Each node requires a unique ID and gRPC address.

### Start Node 1 (Seed Node)
```bash
docker run -it --rm \
  -p 8081:8081 -p 50051:50051 \
  -e PORT=8081 -e NODE_ID=node1 -e GRPC_PORT=50051 \
  kvick-db
```

### Start Node 2 (Joining Node 1)
```bash
# Note: On Docker Desktop use host.docker.internal to reach the first node
docker run -it --rm \
  -p 8082:8082 -p 50052:50052 \
  -e PORT=8082 -e NODE_ID=node2 -e GRPC_PORT=50052 \
  -e SEED_NODES=host.docker.internal:50051 \
  kvick-db
```

## Client Protocol
KVick uses a simple plain-text protocol over TCP.

### Commands:
*   `SET <key> <value>`: Store a typed value (e.g., `SET mykey 123` stores an int, `SET mykey [1,2,3]` stores a vector).
*   `GET <key>`: Retrieve the value.
*   `DEL <key>`: Delete a key.

### Example with `nc`:
```bash
echo "SET greeting 'hello world'" | nc localhost 8081
echo "GET greeting" | nc localhost 8081
```

## Architecture Details
*   **Server-Side Proxying**: If you connect to a node that doesn't own a key, it will transparently proxy your request to the correct node using gRPC and return the result.
*   **Binary Format**: Snapshots use a custom binary format (`KV01`) for faster loading and space efficiency.
*   **Non-Blocking Persistence**: Snapshots are taken by capturing immutable views of shard maps, ensuring zero impact on concurrent write performance during disk I/O.
