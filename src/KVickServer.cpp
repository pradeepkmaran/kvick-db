#include "KVickServer.hpp"
#include <iostream>
#include <sstream>

KVickServer::KVickServer(int port, const std::string& node_id, const std::string& grpc_address, const std::vector<std::string>& seed_nodes)  
    : port_(port), node_id_(node_id), grpc_address_(grpc_address), epoll_fd_(-1), server_fd_(-1),
      proxy_service_(std::make_unique<kvick::KVProxyServiceImpl>(this)),
      gossip_service_(nullptr) {       
    
    cluster_manager_ = std::make_unique<kvick::ClusterManager>(node_id, grpc_address, seed_nodes);
    gossip_service_ = std::make_unique<kvick::GossipServiceImpl>(cluster_manager_.get());

    const std::string data_path = "/app/data/kvick_data_" + node_id + ".bin";
    const std::string wal_path = "/app/data/kvick_" + node_id + ".wal";
    
    loadFromFile(data_path);
    openWAL(wal_path);
    replayWAL();
    
    enableAutoPersist(data_path, 30);

    cluster_manager_ = std::make_unique<kvick::ClusterManager>(node_id, grpc_address, seed_nodes);
    gossip_service_ = std::make_unique<kvick::GossipServiceImpl>(cluster_manager_.get());

    int num_workers = std::thread::hardware_concurrency();
    if (num_workers == 0) num_workers = 4;
    for (int i = 0; i < num_workers; ++i) {
        workers_.emplace_back(&KVickServer::worker_loop, this);
    }
}

KVickServer::~KVickServer() {
    stop_flag_ = true;
    cv_.notify_all();
    if (cluster_manager_) cluster_manager_->stop();
    if (grpc_server_) grpc_server_->Shutdown();

    for (auto& w : workers_) {
        if (w.joinable()) w.join();
    }
    
    if (epoll_fd_ != -1) close(epoll_fd_);
    if (server_fd_ != -1) close(server_fd_);
    
    disableAutoPersist();
    saveToFile("/app/data/kvick_data_" + node_id_ + ".bin");
    std::cout << "Data persisted on shutdown" << std::endl;
}

void KVickServer::setNonBlocking(int sock) {
    int flags = fcntl(sock, F_GETFL, 0);
    if (flags == -1) return;
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
}

void KVickServer::worker_loop() {
    while (!stop_flag_) {
        int sock;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            cv_.wait(lock, [this] { return stop_flag_ || !task_queue_.empty(); });
            if (stop_flag_ && task_queue_.empty()) return;
            
            sock = task_queue_.front();
            task_queue_.pop();
        }
        handleClientCommand(sock);
    }
}

std::string KVickServer::proxyRequest(const std::string& target_node_id, const std::string& op, const std::string& key, const std::string& val) {
    std::string target_addr = "";
    auto nodes = cluster_manager_->getActiveNodes();
    for (const auto& n : nodes) {
        if (n.node_id() == target_node_id) {
            target_addr = n.address();
            break;
        }
    }
    
    if (target_addr.empty()) {
        return "ERR Node unavailable\n";
    }

    auto channel = grpc::CreateChannel(target_addr, grpc::InsecureChannelCredentials());
    auto stub = kvick::KVProxyService::NewStub(channel);

    kvick::KVRequest req;
    req.set_op(op);
    req.set_key(key);
    if (!val.empty()) req.set_value(val);

    kvick::KVResponse res;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));

    grpc::Status status = stub->ProxyCommand(&context, req, &res);

    if (status.ok()) {
        if (res.success()) {
            if (op == "GET") return res.result() + "\n";
            else return "OK\n";
        } else {
            return "ERR " + res.result() + "\n";
        }
    } else {
        return "ERR Proxy timeout or failure\n";
    }
}

void KVickServer::start() {
    // Start gRPC server
    grpc::ServerBuilder builder;
    builder.AddListeningPort(grpc_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(proxy_service_.get());
    builder.RegisterService(gossip_service_.get());
    grpc_server_ = builder.BuildAndStart();
    std::cout << "gRPC Server listening on " << grpc_address_ << std::endl;

    cluster_manager_->start();

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

    std::cout << "KVickServer TCP listening on port " << port_ << " with proxying\n";

    const int MAX_EVENTS = 64;
    epoll_event events[MAX_EVENTS];

    while (!stop_flag_) {
        int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, -1);
        for (int i = 0; i < n; ++i) {
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
                client_ev.events = EPOLLIN | EPOLLONESHOT | EPOLLET;
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
        grpc_server_->Wait();
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
            ev.events = EPOLLIN | EPOLLONESHOT | EPOLLET;
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
        
        std::string target_node = cluster_manager_->getOwnerNode(key);

        if (target_node != node_id_) {
            // PROXY REQUEST
            if (op == "SET") {
                std::getline(iss, val_str);
                if (!val_str.empty() && val_str.front() == ' ') val_str.erase(0,1);
            }
            std::string res = proxyRequest(target_node, op, key, val_str);
            ::write(sock, res.c_str(), res.size());
            continue;
        }

        // LOCAL EXECUTION
        if (op == "GET") {
            try {
                auto v = get(key);
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
                }, *v);
                std::string out = oss.str() + "\n";
                ::write(sock, out.c_str(), out.size());
            } catch (const std::exception& e) {
                std::string err = "ERR " + std::string(e.what()) + "\n";
                ::write(sock, err.c_str(), err.size());
            }
        } else if (op == "SET") {
            std::getline(iss, val_str);
            if (!val_str.empty() && val_str.front() == ' ') val_str.erase(0,1);
            set(key, parseLiteral(val_str));
            const char ok[] = "OK\n";
            ::write(sock, ok, sizeof(ok)-1);
        } else if (op == "DEL") {
            bool deleted = del(key);
            if (deleted) ::write(sock, "OK\n", 3);
            else ::write(sock, "ERR Not Found\n", 14);
        }
    }

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLONESHOT | EPOLLET;
    ev.data.fd = sock;
    epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, sock, &ev);
}
