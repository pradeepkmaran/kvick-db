#include "KVickServer.hpp"
#include "../utils/Hash.hpp"
#include <iostream>
#include <sstream>
#include <cerrno>
#include <cstring>

// ---- Signal handling ----

void KVickServer::signalHandler(int sig) {
    globalStopFlag() = true;
}

void KVickServer::setupSignals() {
    // Ignore SIGPIPE — short-lived clients (echo|nc) closing early must not kill the server
    signal(SIGPIPE, SIG_IGN);

    struct sigaction sa{};
    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    // Self-pipe trick for waking epoll
    if (pipe(signal_pipe_) == 0) {
        setNonBlocking(signal_pipe_[0]);
        setNonBlocking(signal_pipe_[1]);
    }
}

// ---- gRPC Service Implementations ----

namespace kvick {

std::string KVProxyServiceImpl::formatValue(const KVick::ValueType& val) {
    return std::visit([](auto&& x) -> std::string {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_same_v<T, std::string>) return x;
        else if constexpr (std::is_same_v<T, int64_t>) return std::to_string(x);
        else if constexpr (std::is_same_v<T, double>) return std::to_string(x);
        else if constexpr (std::is_same_v<T, bool>) return x ? "true" : "false";
        else if constexpr (std::is_same_v<T, std::vector<std::string>> ||
                           std::is_same_v<T, std::vector<int64_t>> ||
                           std::is_same_v<T, std::vector<double>> ||
                           std::is_same_v<T, std::vector<bool>>) {
            std::ostringstream oss;
            oss << "[";
            for (size_t i = 0; i < x.size(); ++i) {
                if constexpr (std::is_same_v<T, std::vector<bool>>) oss << (x[i] ? "true" : "false");
                else if constexpr (std::is_same_v<T, std::vector<int64_t>> || std::is_same_v<T, std::vector<double>>) oss << std::to_string(x[i]);
                else oss << x[i];
                if (i + 1 < x.size()) oss << ", ";
            }
            oss << "]";
            return oss.str();
        }
        return "";
    }, val);
}

grpc::Status KVProxyServiceImpl::ProxyCommand(grpc::ServerContext* context,
                                               const KVRequest* request,
                                               KVResponse* response) {
    try {
        if (request->op() == "GET") {
            auto val = store_->get(request->key());
            response->set_success(true);
            response->set_result(formatValue(*val));
        } else if (request->op() == "SET" || request->op() == "DEL") {
            // All writes go through Raft
            std::string command;
            if (request->op() == "SET") {
                command = "SET " + request->key() + " " + request->value();
            } else {
                command = "DEL " + request->key();
            }

            if (raft_ && raft_->isLeader()) {
                auto result = raft_->proposeWrite(command);
                response->set_success(result.success);
                response->set_result(result.message);
            } else {
                // Not leader — tell caller who the leader is
                response->set_success(false);
                response->set_result("ERR Not leader");
                if (raft_ && cluster_) {
                    int32_t leader_id = raft_->getLeaderId();
                    std::string leader_addr = cluster_->getAddressByServerId(leader_id);
                    response->set_leader_address(leader_addr);
                }
            }
        } else {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Unknown operation");
        }
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_result(e.what());
    }
    return grpc::Status::OK;
}

grpc::Status KVProxyServiceImpl::JoinCluster(grpc::ServerContext* context,
                                              const JoinRequest* request,
                                              JoinResponse* response) {
    if (!raft_ || !raft_->isLeader()) {
        response->set_success(false);
        response->set_error("Not the leader");
        return grpc::Status::OK;
    }

    bool ok = raft_->addServer(request->server_id(), request->raft_endpoint());
    response->set_success(ok);
    if (!ok) response->set_error("Failed to add server to Raft cluster");

    std::cout << "[Raft] Join request from " << request->node_id()
              << " (server_id=" << request->server_id()
              << ", endpoint=" << request->raft_endpoint() << ")"
              << " — " << (ok ? "accepted" : "rejected") << std::endl;

    return grpc::Status::OK;
}

} // namespace kvick

// ---- KVickServer ----

KVickServer::KVickServer(int port, const std::string& node_id, const std::string& grpc_address,
                         const std::string& advertise_address,
                         int raft_port, const std::vector<std::string>& seed_nodes,
                         const std::string& data_dir)
    : port_(port), node_id_(node_id), grpc_address_(grpc_address),
      advertise_address_(advertise_address),
      raft_port_(raft_port), data_dir_(data_dir), epoll_fd_(-1), server_fd_(-1) {

    setupSignals();

    // Load snapshot from disk
    const std::string data_path = data_dir + "/kvick_data_" + node_id + ".bin";
    store_.loadFromFile(data_path);
    std::cout << "Loaded " << store_.size() << " keys from snapshot" << std::endl;

    store_.enableAutoPersist(data_path, 30);

    // Extract hostname from advertise_address ("host:port" -> "host")
    std::string advertise_host = advertise_address_;
    auto colon_pos = advertise_host.find(':');
    if (colon_pos != std::string::npos) {
        advertise_host = advertise_host.substr(0, colon_pos);
    }
    std::string raft_endpoint = advertise_host + ":" + std::to_string(raft_port);

    // Create Raft manager — use advertised hostname so peers can reach us
    raft_manager_ = std::make_unique<kvick::RaftManager>(node_id, raft_port, raft_endpoint, &store_, data_dir);

    // Create Cluster manager — use advertise_address so peers can actually reach us
    int32_t raft_server_id = kvick::hash::node_id_to_server_id(node_id);
    cluster_manager_ = std::make_unique<kvick::ClusterManager>(
        node_id, advertise_address_, raft_endpoint, raft_server_id, seed_nodes);

    // Create gRPC services
    proxy_service_ = std::make_unique<kvick::KVProxyServiceImpl>(
        &store_, raft_manager_.get(), cluster_manager_.get());
    gossip_service_ = std::make_unique<kvick::GossipServiceImpl>(cluster_manager_.get());

    is_seed_ = false;
    for (const auto& s : seed_nodes) {
        if (s == advertise_address_) {
            is_seed_ = true;
            break;
        }
    }

    // Start worker thread pool
    int num_workers = std::thread::hardware_concurrency();
    if (num_workers == 0) num_workers = 4;
    for (int i = 0; i < num_workers; ++i) {
        workers_.emplace_back(&KVickServer::worker_loop, this);
    }
}

KVickServer::~KVickServer() {
    globalStopFlag() = true;
    cv_.notify_all();

    if (raft_manager_) raft_manager_->stop();
    if (cluster_manager_) cluster_manager_->stop();
    if (grpc_server_) grpc_server_->Shutdown();

    for (auto& w : workers_) {
        if (w.joinable()) w.join();
    }

    if (epoll_fd_ != -1) close(epoll_fd_);
    if (server_fd_ != -1) close(server_fd_);
    if (signal_pipe_[0] != -1) { close(signal_pipe_[0]); close(signal_pipe_[1]); }

    store_.disableAutoPersist();
    store_.saveToFile(data_dir_ + "/kvick_data_" + node_id_ + ".bin");
    std::cout << "Data persisted on shutdown" << std::endl;
}

void KVickServer::setNonBlocking(int sock) {
    int flags = fcntl(sock, F_GETFL, 0);
    if (flags == -1) return;
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
}

void KVickServer::worker_loop() {
    while (!globalStopFlag()) {
        int sock;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            cv_.wait(lock, [this] { return globalStopFlag() || !task_queue_.empty(); });
            if (globalStopFlag() && task_queue_.empty()) return;

            sock = task_queue_.front();
            task_queue_.pop();
        }
        handleClientCommand(sock);
    }
}

// ---- gRPC channel pooling ----

#include <netdb.h>
#include <arpa/inet.h>

std::shared_ptr<kvick::KVProxyService::Stub> KVickServer::getProxyStub(const std::string& address) {
    std::lock_guard<std::mutex> lock(stubs_mutex_);
    auto it = proxy_stubs_.find(address);
    if (it != proxy_stubs_.end()) return it->second;

    std::string target_addr = address;
    auto colon = address.find(':');
    if (colon != std::string::npos) {
        std::string host = address.substr(0, colon);
        std::string port = address.substr(colon + 1);
        struct addrinfo hints{}, *res;
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        if (getaddrinfo(host.c_str(), port.c_str(), &hints, &res) == 0) {
            char ipstr[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &((struct sockaddr_in*)res->ai_addr)->sin_addr, ipstr, sizeof(ipstr));
            freeaddrinfo(res);
            target_addr = "ipv4:" + std::string(ipstr) + ":" + port;
        }
    }
    auto channel = grpc::CreateChannel(target_addr, grpc::InsecureChannelCredentials());
    auto stub = kvick::KVProxyService::NewStub(channel);
    proxy_stubs_[address] = std::move(stub);
    return proxy_stubs_[address];
}

std::string KVickServer::proxyToAddress(const std::string& address, const std::string& op,
                                         const std::string& key, const std::string& val) {
    auto stub = getProxyStub(address);

    kvick::KVRequest req;
    req.set_op(op);
    req.set_key(key);
    if (!val.empty()) req.set_value(val);

    kvick::KVResponse res;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));

    grpc::Status status = stub->ProxyCommand(&context, req, &res);

    if (status.ok()) {
        if (res.success()) {
            return res.result() + "\n";
        } else {
            // If we were told who the leader is, try forwarding there
            if (!res.leader_address().empty() && res.leader_address() != address) {
                return proxyToAddress(res.leader_address(), op, key, val);
            }
            return "ERR " + res.result() + "\n";
        }
    }
    return "ERR Proxy failed: " + status.error_message() + "\n";
}

// ---- Safe socket write ----

bool KVickServer::sendResponse(int sock, const std::string& data) {
    size_t total_sent = 0;
    while (total_sent < data.size()) {
        ssize_t sent = ::write(sock, data.c_str() + total_sent, data.size() - total_sent);
        if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Briefly yield and retry
                std::this_thread::yield();
                continue;
            }
            return false; // Real error
        }
        total_sent += sent;
    }
    return true;
}

// ---- Format value as string ----

std::string KVickServer::formatValue(const KVick::ValueType& val) {
    return std::visit([](auto&& x) -> std::string {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_same_v<T, std::string>) return x;
        else if constexpr (std::is_same_v<T, int64_t>) return std::to_string(x);
        else if constexpr (std::is_same_v<T, double>) return std::to_string(x);
        else if constexpr (std::is_same_v<T, bool>) return x ? "true" : "false";
        else if constexpr (std::is_same_v<T, std::vector<std::string>> ||
                           std::is_same_v<T, std::vector<int64_t>> ||
                           std::is_same_v<T, std::vector<double>> ||
                           std::is_same_v<T, std::vector<bool>>) {
            std::ostringstream oss;
            oss << "[";
            for (size_t i = 0; i < x.size(); ++i) {
                if constexpr (std::is_same_v<T, std::vector<bool>>) oss << (x[i] ? "true" : "false");
                else if constexpr (std::is_same_v<T, std::vector<int64_t>> || std::is_same_v<T, std::vector<double>>) oss << std::to_string(x[i]);
                else oss << x[i];
                if (i + 1 < x.size()) oss << ", ";
            }
            oss << "]";
            return oss.str();
        }
        return "";
    }, val);
}

// ---- Raft cluster join ----

void KVickServer::requestJoinRaftCluster() {
    // After SWIM gossip has discovered the cluster, ask leader to add us
    auto active = cluster_manager_->getActiveNodes();

    for (const auto& node : active) {
        if (node.node_id() == node_id_) continue;

        // Try to join via this node's gRPC
        auto stub = getProxyStub(node.address());
        kvick::JoinRequest req;
        req.set_server_id(kvick::hash::node_id_to_server_id(node_id_));
        // Use advertised hostname for Raft endpoint so leader can reach us
        std::string adv_host = advertise_address_;
        auto cp = adv_host.find(':');
        if (cp != std::string::npos) adv_host = adv_host.substr(0, cp);
        req.set_raft_endpoint(adv_host + ":" + std::to_string(raft_port_));
        req.set_node_id(node_id_);
        req.set_grpc_address(advertise_address_);

        kvick::JoinResponse res;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

        auto status = stub->JoinCluster(&context, req, &res);
        if (status.ok() && res.success()) {
            std::cout << "[Raft] Successfully joined cluster via " << node.node_id() << std::endl;
            return;
        }
    }
    std::cout << "[Raft] Could not join existing Raft cluster (may be the first node)" << std::endl;
}

// ---- Main start ----

void KVickServer::start() {
    // Start gRPC server
    grpc::ServerBuilder builder;
    builder.AddListeningPort(grpc_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(proxy_service_.get());
    builder.RegisterService(gossip_service_.get());
    grpc_server_ = builder.BuildAndStart();
    std::cout << "gRPC Server listening on " << grpc_address_ << std::endl;

    // Start SWIM gossip
    std::this_thread::sleep_for(std::chrono::seconds(2));
    cluster_manager_->start();


    // Start Raft
    raft_manager_->start(is_seed_);

    // Wait for SWIM to discover peers, with retries
    for (int attempt = 0; attempt < 10; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        if (cluster_manager_->getActiveNodes().size() > 1) {
            requestJoinRaftCluster();
            break;
        }
        std::cout << "[Startup] Waiting for peers... attempt " << attempt + 1 << std::endl;
    }

    // TCP server setup
    server_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ == -1) throw std::runtime_error("socket() failed");

    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port_);

    if (::bind(server_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0)
        throw std::runtime_error("bind() failed");

    if (::listen(server_fd_, SOMAXCONN) != 0)
        throw std::runtime_error("listen() failed");

    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ == -1) throw std::runtime_error("epoll_create1() failed");

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = server_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &ev) == -1)
        throw std::runtime_error("epoll_ctl() failed");

    // Add signal pipe to epoll for graceful shutdown
    if (signal_pipe_[0] != -1) {
        epoll_event sev{};
        sev.events = EPOLLIN;
        sev.data.fd = signal_pipe_[0];
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, signal_pipe_[0], &sev);
    }

    std::cout << "KVickServer TCP listening on port " << port_
              << " (Raft on " << raft_port_ << ")" << std::endl;

    const int MAX_EVENTS = 64;
    epoll_event events[MAX_EVENTS];

    while (!globalStopFlag()) {
        int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, 1000); // 1s timeout
        if (n < 0) {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < n; ++i) {
            if (events[i].data.fd == signal_pipe_[0]) {
                // Signal received — drain pipe and exit
                char buf[64];
                while (::read(signal_pipe_[0], buf, sizeof(buf)) > 0) {}
                globalStopFlag() = true;
                break;
            }

            if (events[i].data.fd == server_fd_) {
                sockaddr_in cli;
                socklen_t len = sizeof(cli);
                int cli_sock = ::accept(server_fd_, reinterpret_cast<sockaddr*>(&cli), &len);
                if (cli_sock == -1) continue;

                setNonBlocking(cli_sock);

                {
                    std::lock_guard<std::mutex> lock(connections_mutex_);
                    connections_[cli_sock] = std::make_shared<Connection>();
                    connections_[cli_sock]->fd = cli_sock;
                }

                epoll_event client_ev{};
                client_ev.events = EPOLLIN | EPOLLONESHOT | EPOLLET | EPOLLRDHUP;
                client_ev.data.fd = cli_sock;
                epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, cli_sock, &client_ev);
            } else {
                int client_sock = events[i].data.fd;
                if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    std::lock_guard<std::mutex> lock(connections_mutex_);
                    connections_.erase(client_sock);
                    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_sock, nullptr);
                    socket_close(client_sock);
                } else if (events[i].events & EPOLLIN) {
                    std::lock_guard<std::mutex> lock(queue_mutex_);
                    task_queue_.push(client_sock);
                    cv_.notify_one();
                }
            }
        }
    }

    if (grpc_server_) {
        grpc_server_->Shutdown();
    }
}

void KVickServer::handleClientCommand(int sock) {
    std::shared_ptr<Connection> conn;
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        auto it = connections_.find(sock);
        if (it == connections_.end()) return;
        conn = it->second;
    }

    std::lock_guard<std::mutex> conn_lock(conn->mutex);

    char buf[4096];
    ssize_t r = ::read(sock, buf, sizeof(buf));

    if (r <= 0) {
        if (r == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            epoll_event ev{};
            ev.events = EPOLLIN | EPOLLONESHOT | EPOLLET | EPOLLRDHUP;
            ev.data.fd = sock;
            epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, sock, &ev);
            return;
        }
        std::lock_guard<std::mutex> lock(connections_mutex_);
        connections_.erase(sock);
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, sock, nullptr);
        socket_close(sock);
        return;
    }

    conn->buffer.append(buf, r);

    size_t pos;
    while ((pos = conn->buffer.find('\n')) != std::string::npos) {
        std::string line = conn->buffer.substr(0, pos);
        conn->buffer.erase(0, pos + 1);
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string op, key, val_str;
        iss >> op >> key;

        // --- GET: try local first, fallback to Raft leader ---
        if (op == "GET") {
            try {
                auto v = store_.get(key);
                sendResponse(sock, formatValue(*v) + "\n");
            } catch (const std::exception& e) {
                // Key not found locally — if we're not the leader, try the leader
                // (it's guaranteed to have the latest committed data)
                bool found = false;
                for (int i = 0; i < 3; ++i) { // 3 retries for leader discovery
                    if (!raft_manager_->isLeader()) {
                        int32_t leader_id = raft_manager_->getLeaderId();
                        if (leader_id != -1) {
                            std::string leader_addr = cluster_manager_->getAddressByServerId(leader_id);
                            if (!leader_addr.empty() && leader_addr != advertise_address_) {
                                std::string res = proxyToAddress(leader_addr, "GET", key);
                                sendResponse(sock, res);
                                found = true;
                                break;
                            }
                        }
                        // Wait a bit for leader/cluster discovery
                        std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    } else {
                        // We ARE the leader and key doesn't exist
                        sendResponse(sock, "ERR " + std::string(e.what()) + "\n");
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    sendResponse(sock, "ERR " + std::string(e.what()) + " (Leader not found)\n");
                }
            }
        }
        // --- SET / DEL: go through Raft ---
        else if (op == "SET") {
            std::getline(iss, val_str);
            if (!val_str.empty() && val_str.front() == ' ') val_str.erase(0, 1);

            std::string command = "SET " + key + " " + val_str;

            if (raft_manager_->isLeader()) {
                auto result = raft_manager_->proposeWrite(command);
                sendResponse(sock, result.success ? "OK\n" : ("ERR " + result.message + "\n"));
            } else {
                // Forward to leader
                int32_t leader_id = raft_manager_->getLeaderId();
                std::string leader_addr = cluster_manager_->getAddressByServerId(leader_id);
                if (leader_addr.empty()) {
                    sendResponse(sock, "ERR No leader available\n");
                } else {
                    std::string res = proxyToAddress(leader_addr, "SET", key, val_str);
                    sendResponse(sock, res);
                }
            }
        } else if (op == "DEL") {
            std::string command = "DEL " + key;

            if (raft_manager_->isLeader()) {
                auto result = raft_manager_->proposeWrite(command);
                sendResponse(sock, result.success ? "OK\n" : ("ERR " + result.message + "\n"));
            } else {
                int32_t leader_id = raft_manager_->getLeaderId();
                std::string leader_addr = cluster_manager_->getAddressByServerId(leader_id);
                if (leader_addr.empty()) {
                    sendResponse(sock, "ERR No leader available\n");
                } else {
                    std::string res = proxyToAddress(leader_addr, "DEL", key);
                    sendResponse(sock, res);
                }
            }
        } else if (op == "INFO") {
            std::string role = raft_manager_->isLeader() ? "leader" : "follower";
            int32_t leader_id = raft_manager_->getLeaderId();
            auto active = cluster_manager_->getActiveNodes();
            std::string info = "node:" + node_id_ +
                               " role:" + role +
                               " peers:" + std::to_string(active.size()) +
                               " leader_id:" + std::to_string(leader_id) +
                               " advertise:" + advertise_address_ + "\n";
            sendResponse(sock, info);
        } else {
            sendResponse(sock, "ERR Unknown command\n");
        }
    }

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLONESHOT | EPOLLET | EPOLLRDHUP;
    ev.data.fd = sock;
    epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, sock, &ev);
}
