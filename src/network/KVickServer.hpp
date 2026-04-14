#ifndef KVICK_SERVER_HPP
#define KVICK_SERVER_HPP

#include "../core/KVick.hpp"
#include "ClusterManager.hpp"
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
#include <variant>

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
    KVProxyServiceImpl(KVick* store, ClusterManager* cluster)
        : store_(store), cluster_(cluster) {}

    grpc::Status ProxyCommand(grpc::ServerContext* context, const KVRequest* request,
                              KVResponse* response) override;
    
    grpc::Status ReplicateWrite(grpc::ServerContext* context, const ReplicateWriteRequest* request,
                               KVResponse* response) override;
    
    grpc::Status QuorumRead(grpc::ServerContext* context, const QuorumReadRequest* request,
                           QuorumReadResponse* response) override;

private:
    KVick* store_;
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
                const std::vector<std::string>& seed_nodes,
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
    std::unique_ptr<grpc::Server> grpc_server_;
    std::unique_ptr<kvick::KVProxyServiceImpl> proxy_service_;
    std::unique_ptr<kvick::GossipServiceImpl> gossip_service_;

    // Cached gRPC channels for proxy/replication requests
    std::unordered_map<std::string, std::shared_ptr<kvick::KVProxyService::Stub>> proxy_stubs_;
    std::mutex stubs_mutex_;

    void worker_loop();
    void handleClientCommand(int sock);
    void setNonBlocking(int sock);

    std::shared_ptr<kvick::KVProxyService::Stub> getProxyStub(const std::string& address);

    // Coordinator logic
    struct VectorClock {
        std::unordered_map<std::string, uint32_t> clock;
        
        bool dominates(const VectorClock& other) const {
            bool strictly_greater = false;
            for (const auto& [node, counter] : other.clock) {
                auto it = clock.find(node);
                if (it == clock.end() || it->second < counter) return false;
                if (it->second > counter) strictly_greater = true;
            }
            // If other has everything we have (or less) and we have something more
            if (!strictly_greater) {
                for (const auto& [node, counter] : clock) {
                    if (other.clock.find(node) == other.clock.end()) {
                        strictly_greater = true;
                        break;
                    }
                }
            }
            return strictly_greater;
        }

        bool operator==(const VectorClock& other) const {
            return clock == other.clock;
        }

        static VectorClock fromProto(const kvick::VersionedValue& v) {
            VectorClock vc;
            for (const auto& entry : v.clock()) {
                vc.clock[entry.node_id()] = entry.counter();
            }
            return vc;
        }

        void toProto(kvick::VersionedValue* v) const {
            for (const auto& [node, counter] : clock) {
                auto entry = v->add_clock();
                entry->set_node_id(node);
                entry->set_counter(counter);
            }
        }
        
        std::string toString() const {
            std::ostringstream oss;
            oss << "{";
            bool first = true;
            for (const auto& [node, counter] : clock) {
                if (!first) oss << ", ";
                oss << "\"" << node << "\":" << counter;
                first = false;
            }
            oss << "}";
            return oss.str();
        }

        static VectorClock fromJSON(const std::string& json);
    };

    struct QuorumResult {
        std::string value;
        VectorClock clock;
        bool is_tombstone;
    };

    // Safe socket write with partial-write handling
    bool sendResponse(int sock, const std::string& data);

    std::string formatValue(const KVick::ValueType& val);

    static void signalHandler(int sig);
    void setupSignals();

    bool is_seed_;
};

#endif
