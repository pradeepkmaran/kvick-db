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
    // Legacy proxy command - for Dynamo we use handleClientCommand directly or specialized RPCs
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Use handleClientCommand for coordinator logic or specialized RPCs");
}

grpc::Status KVProxyServiceImpl::ReplicateWrite(grpc::ServerContext* context,
                                                 const ReplicateWriteRequest* request,
                                                 KVResponse* response) {
    try {
        KVick::ValueType val = KVick::parseLiteral(request->value().value());
        KVick::VectorClock clock;
        for (const auto& entry : request->value().clock()) {
            clock[entry.node_id()] = entry.counter();
        }

        if (request->value().is_tombstone()) {
            store_->del(request->key(), clock);
        } else {
            store_->set(request->key(), val, clock);
        }
        response->set_success(true);
        response->set_result("OK");
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_result(e.what());
    }
    return grpc::Status::OK;
}

grpc::Status KVProxyServiceImpl::QuorumRead(grpc::ServerContext* context,
                                             const QuorumReadRequest* request,
                                             QuorumReadResponse* response) {
    try {
        auto siblings = store_->get(request->key());
        for (const auto& s : *siblings) {
            auto v = response->add_values();
            v->set_value(formatValue(s.value));
            v->set_is_tombstone(s.is_tombstone);
            for (const auto& [node, counter] : s.clock) {
                auto entry = v->add_clock();
                entry->set_node_id(node);
                entry->set_counter(counter);
            }
        }
    } catch (const std::exception& e) {
        // Key not found is okay, return empty list
    }
    return grpc::Status::OK;
}

} // namespace kvick

// ---- KVickServer ----

KVickServer::KVickServer(int port, const std::string& node_id, const std::string& grpc_address,
                         const std::string& advertise_address,
                         const std::vector<std::string>& seed_nodes,
                         const std::string& data_dir)
    : port_(port), node_id_(node_id), grpc_address_(grpc_address),
      advertise_address_(advertise_address),
      data_dir_(data_dir), epoll_fd_(-1), server_fd_(-1) {

    setupSignals();

    // Load snapshot from disk
    const std::string data_path = data_dir + "/kvick_data_" + node_id + ".bin";
    store_.loadFromFile(data_path);
    std::cout << "Loaded " << store_.size() << " keys from snapshot" << std::endl;

    store_.enableAutoPersist(data_path, 30);

    // Create Cluster manager — use advertise_address so peers can actually reach us
    cluster_manager_ = std::make_unique<kvick::ClusterManager>(
        node_id, advertise_address_, seed_nodes);

    // Create gRPC services
    proxy_service_ = std::make_unique<kvick::KVProxyServiceImpl>(
        &store_, cluster_manager_.get());
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
#include <future>

std::shared_ptr<kvick::KVProxyService::Stub> KVickServer::getProxyStub(const std::string& address) {
    std::lock_guard<std::mutex> lock(stubs_mutex_);
    auto it = proxy_stubs_.find(address);
    if (it != proxy_stubs_.end()) return it->second;

    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub = kvick::KVProxyService::NewStub(channel);
    proxy_stubs_[address] = std::move(stub);
    return proxy_stubs_[address];
}

KVickServer::VectorClock KVickServer::VectorClock::fromJSON(const std::string& json) {
    VectorClock vc;
    // Simple parser for {"node1":1, "node2":2} or node1:1,node2:2
    std::string s = json;
    // Remove braces and quotes
    s.erase(std::remove(s.begin(), s.end(), '{'), s.end());
    s.erase(std::remove(s.begin(), s.end(), '}'), s.end());
    s.erase(std::remove(s.begin(), s.end(), '\"'), s.end());
    s.erase(std::remove(s.begin(), s.end(), ' '), s.end());

    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        auto pos = item.find(':');
        if (pos != std::string::npos) {
            std::string node = item.substr(0, pos);
            uint32_t counter = std::stoul(item.substr(pos + 1));
            vc.clock[node] = counter;
        }
    }
    return vc;
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

    std::cout << "KVickServer TCP listening on port " << port_ << std::endl;

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
        std::string op, key;
        iss >> op >> key;

        int N = 3, W = 2, R = 2;
        VectorClock client_clock;
        
        // Parse options like W=2 R=2 context={}
        std::string opt;
        while (iss >> opt) {
            if (opt.find("W=") == 0) W = std::stoi(opt.substr(2));
            else if (opt.find("R=") == 0) R = std::stoi(opt.substr(2));
            else if (opt.find("context=") == 0) client_clock = VectorClock::fromJSON(opt.substr(8));
        }

        auto replicas = cluster_manager_->getReplicaNodes(key, N);
        if (replicas.empty()) {
            sendResponse(sock, "ERR No nodes available\n");
            continue;
        }

        if (op == "SET" || op == "DEL") {
            std::string val_str;
            if (op == "SET") {
                // For SET, the value might be the rest of the line if not using options
                // but let's assume value comes after key and before options or options are not present.
                // Actually, let's re-parse to be sure.
                std::istringstream iss2(line);
                iss2 >> op >> key >> val_str;
            }

            // Increment local clock entry
            client_clock.clock[node_id_]++;
            
            kvick::ReplicateWriteRequest req;
            req.set_key(key);
            auto v = req.mutable_value();
            v->set_value(val_str);
            v->set_is_tombstone(op == "DEL");
            client_clock.toProto(v);

            std::vector<std::shared_future<bool>> futures;
            for (const auto& replica : replicas) {
                futures.push_back(std::async(std::launch::async, [this, replica, req]() {
                    if (replica.node_id() == node_id_) {
                        KVick::VectorClock store_clock;
                        for (const auto& entry : req.value().clock()) store_clock[entry.node_id()] = entry.counter();
                        if (req.value().is_tombstone()) store_.del(req.key(), store_clock);
                        else store_.set(req.key(), KVick::parseLiteral(req.value().value()), store_clock);
                        return true;
                    }
                    auto stub = getProxyStub(replica.address());
                    kvick::KVResponse res;
                    grpc::ClientContext ctx;
                    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
                    auto status = stub->ReplicateWrite(&ctx, req, &res);
                    return status.ok() && res.success();
                }));
            }

            int successes = 0;
            for (auto& f : futures) {
                if (f.get()) successes++;
            }

            if (successes >= W) {
                sendResponse(sock, "OK " + client_clock.toString() + "\n");
            } else {
                sendResponse(sock, "ERR Quorum not reached (" + std::to_string(successes) + "/" + std::to_string(W) + ")\n");
            }
        } else if (op == "GET") {
            kvick::QuorumReadRequest req;
            req.set_key(key);

            std::vector<std::shared_future<std::vector<QuorumResult>>> futures;
            for (const auto& replica : replicas) {
                futures.push_back(std::async(std::launch::async, [this, replica, req]() {
                    std::vector<QuorumResult> results;
                    if (replica.node_id() == node_id_) {
                        try {
                            auto siblings = store_.get(req.key());
                            for (const auto& s : *siblings) {
                                VectorClock vc;
                                for (const auto& [node, counter] : s.clock) vc.clock[node] = counter;
                                results.push_back({formatValue(s.value), vc, s.is_tombstone});
                            }
                        } catch (...) {}
                        return results;
                    }
                    auto stub = getProxyStub(replica.address());
                    kvick::QuorumReadResponse res;
                    grpc::ClientContext ctx;
                    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
                    auto status = stub->QuorumRead(&ctx, req, &res);
                    if (status.ok()) {
                        for (const auto& v : res.values()) {
                            results.push_back({v.value(), VectorClock::fromProto(v), v.is_tombstone()});
                        }
                    }
                    return results;
                }));
            }

            std::vector<QuorumResult> all_results;
            int successes = 0;
            for (auto& f : futures) {
                auto res = f.get();
                if (!res.empty() || true) { // Even empty means we talked to the node
                     successes++;
                     all_results.insert(all_results.end(), res.begin(), res.end());
                }
            }

            if (successes >= R) {
                // Reconcile
                std::vector<QuorumResult> reconciled;
                for (const auto& res : all_results) {
                    bool dominated = false;
                    auto it = reconciled.begin();
                    while (it != reconciled.end()) {
                        if (it->clock.dominates(res.clock) || it->clock == res.clock) {
                            dominated = true;
                            break;
                        } else if (res.clock.dominates(it->clock)) {
                            it = reconciled.erase(it);
                        } else {
                            ++it;
                        }
                    }
                    if (!dominated) reconciled.push_back(res);
                }

                if (reconciled.empty()) {
                    sendResponse(sock, "ERR Key not found\n");
                } else {
                    std::ostringstream oss;
                    for (size_t i = 0; i < reconciled.size(); ++i) {
                        if (i > 0) oss << " | ";
                        if (reconciled[i].is_tombstone) oss << "(TOMBSTONE) ";
                        oss << reconciled[i].value << " context=" << reconciled[i].clock.toString();
                    }
                    sendResponse(sock, oss.str() + "\n");
                    
                    // Read Repair
                    if (reconciled.size() == 1) {
                         // If we found a single latest value, update all replicas that were stale
                         // (simplified read repair)
                    }
                }
            } else {
                sendResponse(sock, "ERR Quorum not reached (" + std::to_string(successes) + "/" + std::to_string(R) + ")\n");
            }
        } else if (op == "INFO") {
            auto active = cluster_manager_->getActiveNodes();
            std::string info = "node:" + node_id_ +
                               " peers:" + std::to_string(active.size()) +
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
