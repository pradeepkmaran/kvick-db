#ifndef KVICK_SERVER_HPP
#define KVICK_SERVER_HPP

#include "KVick.hpp"
#include "ClusterManager.hpp"
#include "RaftManager.hpp"
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
#include <csignal>

#include <grpcpp/grpcpp.h>

using socket_t = int;
inline void socket_close(socket_t s) { ::close(s); }

namespace kvick {

class KVProxyServiceImpl : public KVProxyService::Service {
public:
    KVProxyServiceImpl(KVick* store, RaftManager* raft, ClusterManager* cluster)
        : store_(store), raft_(raft), cluster_(cluster) {}

    grpc::Status ProxyCommand(grpc::ServerContext* context, const KVRequest* request,
                              KVResponse* response) override;
    grpc::Status JoinCluster(grpc::ServerContext* context, const JoinRequest* request,
                             JoinResponse* response) override;

private:
    KVick* store_;
    RaftManager* raft_;
    ClusterManager* cluster_;

    std::string formatValue(const KVick::ValueType& val);
};

class GossipServiceImpl : public GossipService::Service {
public:
    GossipServiceImpl(ClusterManager* cluster) : cluster_(cluster) {}

    grpc::Status Ping(grpc::ServerContext* context, const PingMessage* request,
                      AckMessage* response) override {
        return cluster_->Ping(context, request, response);
    }
    grpc::Status PingReq(grpc::ServerContext* context, const PingReqMessage* request,
                         AckMessage* response) override {
        return cluster_->PingReq(context, request, response);
    }

private:
    ClusterManager* cluster_;
};

} // namespace kvick


class KVickServer {
public:
    KVickServer(int port, const std::string& node_id, const std::string& grpc_address,
                const std::string& advertise_address,
                int raft_port, const std::vector<std::string>& seed_nodes,
                const std::string& data_dir);
    void start();
    ~KVickServer();

    static std::atomic<bool>& globalStopFlag() {
        static std::atomic<bool> flag{false};
        return flag;
    }

private:
    struct Connection {
        int fd;
        std::string buffer;
        std::mutex mutex;
    };

    int port_;
    std::string node_id_;
    std::string grpc_address_;
    std::string advertise_address_;
    int raft_port_;
    std::string data_dir_;
    int epoll_fd_;
    int server_fd_;
    int signal_pipe_[2]{-1, -1}; // Self-pipe for signal wakeup

    KVick store_;

    std::vector<std::thread> workers_;
    std::queue<int> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable cv_;

    std::unordered_map<int, std::shared_ptr<Connection>> connections_;
    std::mutex connections_mutex_;

    std::unique_ptr<kvick::ClusterManager> cluster_manager_;
    std::unique_ptr<kvick::RaftManager> raft_manager_;
    std::unique_ptr<grpc::Server> grpc_server_;
    std::unique_ptr<kvick::KVProxyServiceImpl> proxy_service_;
    std::unique_ptr<kvick::GossipServiceImpl> gossip_service_;

    // Cached gRPC channels for proxy requests
    std::unordered_map<std::string, std::shared_ptr<kvick::KVProxyService::Stub>> proxy_stubs_;
    std::mutex stubs_mutex_;

    void worker_loop();
    void handleClientCommand(int sock);
    void setNonBlocking(int sock);

    std::shared_ptr<kvick::KVProxyService::Stub> getProxyStub(const std::string& address);
    std::string proxyToAddress(const std::string& address, const std::string& op,
                               const std::string& key, const std::string& val = "");

    // Safe socket write with partial-write handling
    bool sendResponse(int sock, const std::string& data);

    std::string formatValue(const KVick::ValueType& val);

    static void signalHandler(int sig);
    void setupSignals();
    void requestJoinRaftCluster();
};

#endif
