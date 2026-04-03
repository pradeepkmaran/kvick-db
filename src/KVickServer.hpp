#ifndef KVICK_SERVER_HPP
#define KVICK_SERVER_HPP

#include "KVick.hpp"
#include "ClusterManager.hpp"
#include <thread>
#include <cstring>
#include <stdexcept>
#include <string>
#include <sstream>
#include <vector>
#include <queue>
#include <condition_variable>
#include <mutex>
#include <atomic>

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>

#include <grpcpp/grpcpp.h>

using socket_t = int;
inline void socket_close(socket_t s) { ::close(s); }

namespace kvick {

class KVProxyServiceImpl : public KVProxyService::Service {
public:
    KVProxyServiceImpl(KVick* store) : store_(store) {}

    grpc::Status ProxyCommand(grpc::ServerContext* context, const KVRequest* request, KVResponse* response) override {
        try {
            if (request->op() == "GET") {
                auto val = store_->get(request->key());
                std::ostringstream oss;
                std::visit([&oss](auto&& x) {
                    using T = std::decay_t<decltype(x)>;
                    if constexpr (std::is_same_v<T, std::string>) oss << x;
                    else if constexpr (std::is_same_v<T, int>) oss << std::to_string(x);
                    else if constexpr (std::is_same_v<T, double>) oss << std::to_string(x);
                    else if constexpr (std::is_same_v<T, bool>) oss << (x ? "true" : "false");
                    else if constexpr (std::is_same_v<T, std::vector<std::string>> ||
                                       std::is_same_v<T, std::vector<int>> ||
                                       std::is_same_v<T, std::vector<double>> ||
                                       std::is_same_v<T, std::vector<bool>>) {
                        oss << "[";
                        for (size_t i = 0; i < x.size(); ++i) {
                            if constexpr (std::is_same_v<T, std::vector<bool>>) oss << (x[i] ? "true" : "false");
                            else if constexpr (std::is_same_v<T, std::vector<int>> || std::is_same_v<T, std::vector<double>>) oss << std::to_string(x[i]);
                            else oss << x[i];
                            if (i + 1 < x.size()) oss << ", ";
                        }
                        oss << "]";
                    }
                }, *val);
                response->set_success(true);
                response->set_result(oss.str());
            } else if (request->op() == "SET") {
                store_->set(request->key(), KVick::parseLiteral(request->value()));
                response->set_success(true);
                response->set_result("OK");
            } else if (request->op() == "DEL") {
                bool deleted = store_->del(request->key());
                response->set_success(deleted);
                response->set_result(deleted ? "OK" : "ERR Not Found");
            } else {
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Unknown operation");
            }
        } catch (const std::exception& e) {
            response->set_success(false);
            response->set_result(e.what());
        }
        return grpc::Status::OK;
    }
private:
    KVick* store_;
};

class GossipServiceImpl : public GossipService::Service {
public:
    GossipServiceImpl(ClusterManager* cluster) : cluster_(cluster) {}

    grpc::Status Ping(grpc::ServerContext* context, const PingMessage* request, AckMessage* response) override {
        return cluster_->Ping(context, request, response);
    }
    grpc::Status PingReq(grpc::ServerContext* context, const PingReqMessage* request, AckMessage* response) override {
        return cluster_->PingReq(context, request, response);
    }
private:
    ClusterManager* cluster_;
};

} // namespace kvick


class KVickServer : public KVick {
public:
    KVickServer(int port, const std::string& node_id, const std::string& grpc_address, const std::vector<std::string>& seed_nodes);
    void start();
    ~KVickServer();

private:
    struct Connection {
        int fd;
        std::string buffer;
        std::mutex mutex;
    };

    int port_;
    std::string node_id_;
    std::string grpc_address_;
    int epoll_fd_;
    int server_fd_;
    std::atomic<bool> stop_flag_{false};

    std::vector<std::thread> workers_;
    std::queue<int> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable cv_;

    std::unordered_map<int, std::shared_ptr<Connection>> connections_;
    std::mutex connections_mutex_;

    std::unique_ptr<kvick::ClusterManager> cluster_manager_;
    std::unique_ptr<grpc::Server> grpc_server_;
    kvick::KVProxyServiceImpl proxy_service_;
    kvick::GossipServiceImpl gossip_service_;

    void worker_loop();
    void handleClientCommand(int sock);
    void setNonBlocking(int sock);
    
    std::string proxyRequest(const std::string& target_node_id, const std::string& op, const std::string& key, const std::string& val = "");
};

#endif
