#include "network/KVickServer.hpp"
#include <iostream>
#include <vector>
#include <string>
#include <cstdlib>
#include <sstream>

// Helper: get env var with fallback
std::string getEnv(const char* key, const std::string& def = "") {
    const char* val = std::getenv(key);
    return val ? std::string(val) : def;
}

// Helper: split comma-separated string
std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> result;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        if (!item.empty()) result.push_back(item);
    }
    return result;
}

int main(int argc, char* argv[]) {
    // ---- Defaults ----
    int port = 5000;
    std::string node_id = "node1";
    std::string grpc_address = "0.0.0.0:50051";
    std::string advertise_address = "";
    int raft_port = 10051;
    std::string data_dir = "/app/data";
    std::vector<std::string> seed_nodes;

    // ---- ENV overrides ----
    port = std::stoi(getEnv("PORT", std::to_string(port)));
    node_id = getEnv("NODE_ID", node_id);

    std::string grpc_port_env = getEnv("GRPC_PORT");
    if (!grpc_port_env.empty()) {
        grpc_address = "0.0.0.0:" + grpc_port_env;
    }

    advertise_address = getEnv("ADVERTISE_ADDRESS", advertise_address);

    raft_port = std::stoi(getEnv("RAFT_PORT", std::to_string(raft_port)));
    data_dir = getEnv("DATA_DIR", data_dir);

    std::string seeds_env = getEnv("SEED_NODES");
    if (!seeds_env.empty()) {
        seed_nodes = split(seeds_env, ',');
    }

    // ---- CLI overrides (highest priority) ----
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            port = std::stoi(argv[++i]);
        } else if (arg == "--id" && i + 1 < argc) {
            node_id = argv[++i];
        } else if (arg == "--grpc" && i + 1 < argc) {
            grpc_address = argv[++i];
        } else if (arg == "--advertise" && i + 1 < argc) {
            advertise_address = argv[++i];
        } else if (arg == "--raft-port" && i + 1 < argc) {
            raft_port = std::stoi(argv[++i]);
        } else if (arg == "--data-dir" && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (arg == "--seed" && i + 1 < argc) {
            seed_nodes.push_back(argv[++i]);
        }
    }

    // ---- Final fallback ----
    if (advertise_address.empty()) {
        advertise_address = grpc_address;
    }

    // ---- Debug print (important for verification) ----
    std::cout << "[CONFIG]"
              << " node_id=" << node_id
              << " port=" << port
              << " grpc=" << grpc_address
              << " advertise=" << advertise_address
              << " raft_port=" << raft_port
              << " seeds=";

    for (const auto& s : seed_nodes) std::cout << s << " ";
    std::cout << std::endl;

    try {
        KVickServer server(
            port,
            node_id,
            grpc_address,
            advertise_address,
            raft_port,
            seed_nodes,
            data_dir
        );
        server.start();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}