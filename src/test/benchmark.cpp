#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <thread>
#include <atomic>
#include <mutex>
#include <algorithm>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iomanip>
#include <netdb.h>

struct BenchResult {
    size_t total_ops;
    std::vector<double> latencies; // in milliseconds
    double total_time;
};

class BenchClient {
public:
    BenchClient(std::string host, int port) : host_(host), port_(port), sock_(-1) {}

    bool connect() {
        struct addrinfo hints{}, *res;
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        if (getaddrinfo(host_.c_str(), std::to_string(port_).c_str(), &hints, &res) != 0) return false;

        sock_ = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
        if (sock_ < 0) { freeaddrinfo(res); return false; }

        if (::connect(sock_, res->ai_addr, res->ai_addrlen) < 0) {
            close(sock_);
            freeaddrinfo(res);
            return false;
        }
        freeaddrinfo(res);
        return true;
    }

    void close_conn() {
        if (sock_ != -1) close(sock_);
    }

    bool send_cmd(const std::string& cmd) {
        std::string full_cmd = cmd + "\n";
        ssize_t total_sent = 0;
        while (total_sent < (ssize_t)full_cmd.size()) {
            ssize_t sent = send(sock_, full_cmd.c_str() + total_sent, full_cmd.size() - total_sent, 0);
            if (sent <= 0) return false;
            total_sent += sent;
        }

        char buf[4096];
        ssize_t r = recv(sock_, buf, sizeof(buf), 0);
        return r > 0;
    }

private:
    std::string host_;
    int port_;
    int sock_;
};

void run_worker(std::string host, int port, int ops_per_thread, std::string cmd_template, BenchResult& res) {
    BenchClient client(host, port);
    if (!client.connect()) return;

    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < ops_per_thread; ++i) {
        std::string cmd = cmd_template;
        size_t pos = cmd.find("{}");
        if (pos != std::string::npos) {
            cmd.replace(pos, 2, std::to_string(i));
        }

        auto op_start = std::chrono::high_resolution_clock::now();
        if (!client.send_cmd(cmd)) break;
        auto op_end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double, std::milli> diff = op_end - op_start;
        res.latencies.push_back(diff.count());
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> total = end - start;
    res.total_time = total.count();
    res.total_ops = res.latencies.size();
    client.close_conn();
}

void print_stats(const std::string& title, std::vector<BenchResult>& results) {
    size_t total_ops = 0;
    std::vector<double> all_latencies;
    double max_time = 0;

    for (auto& r : results) {
        total_ops += r.total_ops;
        all_latencies.insert(all_latencies.end(), r.latencies.begin(), r.latencies.end());
        max_time = std::max(max_time, r.total_time);
    }

    if (all_latencies.empty()) return;

    std::sort(all_latencies.begin(), all_latencies.end());
    double p50 = all_latencies[all_latencies.size() * 0.5];
    double p95 = all_latencies[all_latencies.size() * 0.95];
    double p99 = all_latencies[all_latencies.size() * 0.99];
    double avg = 0;
    for (double l : all_latencies) avg += l;
    avg /= all_latencies.size();

    std::cout << "\n=== " << title << " ===\n";
    std::cout << "Throughput: " << std::fixed << std::setprecision(0) << (total_ops / max_time) << " ops/sec\n";
    std::cout << "Latency (ms): \n";
    std::cout << "  Avg: " << std::fixed << std::setprecision(3) << avg << "\n";
    std::cout << "  p50: " << p50 << "\n";
    std::cout << "  p95: " << p95 << "\n";
    std::cout << "  p99: " << p99 << "\n";
}

int main(int argc, char** argv) {
    std::string host = "localhost";
    int port = 5000;
    int threads = 4;
    int ops = 10000;

    if (argc > 1) host = argv[1];
    if (argc > 2) port = std::stoi(argv[2]);
    if (argc > 3) threads = std::stoi(argv[3]);
    if (argc > 4) ops = std::stoi(argv[4]);

    std::cout << "Benchmarking KVick-DB at " << host << ":" << port << " with " << threads << " threads, " << ops << " total ops\n";

    int ops_per_thread = ops / threads;
    std::vector<BenchResult> set_results(threads);
    std::vector<std::thread> workers;

    // SET Benchmark
    for (int i = 0; i < threads; ++i) {
        workers.emplace_back(run_worker, host, port, ops_per_thread, "SET key_{} val_{}", std::ref(set_results[i]));
    }
    for (auto& t : workers) t.join();
    print_stats("SET (W=2)", set_results);

    // GET Benchmark
    workers.clear();
    std::vector<BenchResult> get_results(threads);
    for (int i = 0; i < threads; ++i) {
        workers.emplace_back(run_worker, host, port, ops_per_thread, "GET key_{}", std::ref(get_results[i]));
    }
    for (auto& t : workers) t.join();
    print_stats("GET (R=2)", get_results);

    return 0;
}
