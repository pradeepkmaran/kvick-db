#include "KVickServer.hpp"
#include <iostream>
#include <vector>
#include <string>

int main(int argc, char* argv[]) {
    int port = 8080;
    std::string node_id = "node1";
    std::string grpc_address = "0.0.0.0:50051";
    int raft_port = 10051;
    std::string data_dir = "/app/data";
    std::vector<std::string> seed_nodes;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            port = std::stoi(argv[++i]);
        } else if (arg == "--id" && i + 1 < argc) {
            node_id = argv[++i];
        } else if (arg == "--grpc" && i + 1 < argc) {
            grpc_address = argv[++i];
        } else if (arg == "--raft-port" && i + 1 < argc) {
            raft_port = std::stoi(argv[++i]);
        } else if (arg == "--data-dir" && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (arg == "--seed" && i + 1 < argc) {
            seed_nodes.push_back(argv[++i]);
        }
    }

    try {
        KVickServer server(port, node_id, grpc_address, raft_port, seed_nodes, data_dir);
        server.start();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
