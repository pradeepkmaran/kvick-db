#!/usr/bin/env python3
import socket
import time
import threading
import statistics
import argparse
import re
from concurrent.futures import ThreadPoolExecutor

class KVickBench:
    def __init__(self, host, port, threads, ops):
        self.host = host
        self.port = port
        self.threads = threads
        self.ops = ops
        self.latencies = []
        self.node_counts = {}
        self.lock = threading.Lock()
        self.total_success = 0
        self.sample_mapping = None

    def run_worker(self, ops_per_thread, cmd_template):
        worker_latencies = []
        worker_nodes = {}
        success_count = 0
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
            
            for i in range(ops_per_thread):
                cmd = cmd_template.format(i) + "\n"
                
                start = time.perf_counter()
                sock.sendall(cmd.encode())
                
                resp = b""
                while b"\n" not in resp:
                    chunk = sock.recv(1024)
                    if not chunk: break
                    resp += chunk
                
                end = time.perf_counter()
                
                if resp:
                    resp_str = resp.decode()
                    worker_latencies.append((end - start) * 1000) # ms
                    success_count += 1
                    
                    # Extract node IDs from context={"node1":1, ...}
                    found_nodes = re.findall(r'"([^"]+)":', resp_str)
                    for node in found_nodes:
                        worker_nodes[node] = worker_nodes.get(node, 0) + 1
                    
                    # Capture the first successful mapping for display
                    if self.sample_mapping is None and "bench_key_0" in cmd:
                        self.sample_mapping = (cmd.strip(), found_nodes)
            
            sock.close()
        except Exception as e:
            print(f"Worker error: {e}")

        with self.lock:
            self.latencies.extend(worker_latencies)
            self.total_success += success_count
            for node, count in worker_nodes.items():
                self.node_counts[node] = self.node_counts.get(node, 0) + count

    def print_stats(self, title, total_time):
        if not self.latencies:
            print(f"\n=== {title} ===\nNo data collected.")
            return

        self.latencies.sort()
        avg_lat = sum(self.latencies) / len(self.latencies)
        p50 = statistics.median(self.latencies)
        p95 = self.latencies[int(len(self.latencies) * 0.95)]
        p99 = self.latencies[int(len(self.latencies) * 0.99)]
        throughput = self.total_success / total_time

        print(f"\n=== {title} ===")
        if self.sample_mapping:
            key_cmd, nodes = self.sample_mapping
            print(f"Sample Mapping: {key_cmd} -> Quorum: {', '.join(nodes)}")
            self.sample_mapping = None # Clear for next phase

        print(f"Throughput:  {throughput:.2f} ops/sec")
        print(f"Latency (ms):")
        print(f"  Avg:       {avg_lat:.3f}")
        print(f"  P50:       {p50:.3f}")
        print(f"  P95:       {p95:.3f}")
        print(f"  P99:       {p99:.3f}")
        
        print(f"Node Distribution (Quorum Appearances):")
        total_hits = sum(self.node_counts.values())
        for node in sorted(self.node_counts.keys()):
            count = self.node_counts[node]
            pct = (count / total_hits) * 100 if total_hits > 0 else 0
            print(f"  {node:10}: {count:6} hits ({pct:4.1f}%)")
        self.node_counts = {} # Reset for next phase

    def run(self):
        print(f"Benchmarking KVick-DB at {self.host}:{self.port}")
        print(f"Threads: {self.threads}, Ops per phase: {self.ops}")

        ops_per_thread = self.ops // self.threads

        # --- SET Phase ---
        self.latencies = []
        self.total_success = 0
        start_time = time.perf_counter()
        
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            for _ in range(self.threads):
                executor.submit(self.run_worker, ops_per_thread, "SET bench_key_{0} bench_val_{0}")
        
        end_time = time.perf_counter()
        self.print_stats("SET (W=2)", end_time - start_time)

        # --- GET Phase ---
        self.latencies = []
        self.total_success = 0
        start_time = time.perf_counter()
        
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            for _ in range(self.threads):
                executor.submit(self.run_worker, ops_per_thread, "GET bench_key_{}")
        
        end_time = time.perf_counter()
        self.print_stats("GET (R=2)", end_time - start_time)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KVick-DB Python Benchmark")
    parser.add_argument("--host", default="localhost", help="Host address (default: localhost)")
    parser.add_argument("--port", type=int, default=5001, help="Port (default: 5001)")
    parser.add_argument("--threads", type=int, default=8, help="Number of concurrent threads (default: 8)")
    parser.add_argument("--ops", type=int, default=10000, help="Total operations per phase (default: 10000)")
    
    args = parser.parse_args()
    
    bench = KVickBench(args.host, args.port, args.threads, args.ops)
    bench.run()
