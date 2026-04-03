#include "KVick.hpp"
#include <fstream>
#include <thread>
#include <chrono>
#include <mutex>
#include <atomic>
#include <unistd.h>
#include <cstdio>
#include <fcntl.h>
#include <algorithm>

std::shared_ptr<KVick::ValueType> KVick::get(const std::string& key) const {
    size_t idx = getShardIndex(key);
    auto map = getShardMap(idx);
    auto it = map->find(key);
    if (it != map->end()) {
        return it->second;
    }
    throw std::runtime_error("Key not found: " + key);
}

bool KVick::exists(const std::string& key) const {
    size_t idx = getShardIndex(key);
    auto map = getShardMap(idx);
    return map->find(key) != map->end();
}

bool KVick::del(const std::string& key, bool use_wal) {
    if (use_wal) {
        std::lock_guard<std::mutex> wal_lock(wal_mutex);
        logToWAL("DEL", key);
    }
    
    size_t idx = getShardIndex(key);
    std::unique_lock<std::shared_mutex> lock(shards[idx].mutex);
    
    auto it = shards[idx].map_ptr->find(key);
    if (it == shards[idx].map_ptr->end()) return false;
    
    auto new_map = std::make_shared<ShardMap>(*shards[idx].map_ptr);
    new_map->erase(key);
    shards[idx].map_ptr = std::move(new_map);
    return true;
}

std::string KVick::getType(const std::string& key) const {
    auto val_ptr = get(key);
    
    return std::visit([](const auto& value) -> std::string {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::string>) return "string";
        else if constexpr (std::is_same_v<T, int>) return "int";
        else if constexpr (std::is_same_v<T, bool>) return "bool";
        else if constexpr (std::is_same_v<T, double>) return "double";
        else if constexpr (std::is_same_v<T, std::vector<std::string>>) return "vector<string>";
        else if constexpr (std::is_same_v<T, std::vector<int>>) return "vector<int>";
        else if constexpr (std::is_same_v<T, std::vector<bool>>) return "vector<bool>";
        else if constexpr (std::is_same_v<T, std::vector<double>>) return "vector<double>";
        else return "unknown";
    }, *val_ptr);
}

void KVick::print(const std::string& key) const {
    try {
        auto val_ptr = get(key);
        std::cout << key << ": ";
        printValue(*val_ptr);
        std::cout << std::endl;
    } catch (...) {
        std::cout << "Key '" << key << "' not found" << std::endl;
    }
}

void KVick::printAll() const {
    for (size_t i = 0; i < NUM_SHARDS; ++i) {
        auto map = getShardMap(i);
        for (const auto& [key, val_ptr] : *map) {
            std::cout << key << ": ";
            printValue(*val_ptr);
            std::cout << std::endl;
        }
    }
}

size_t KVick::size() const {
    size_t total = 0;
    for (size_t i = 0; i < NUM_SHARDS; ++i) {
        total += getShardMap(i)->size();
    }
    return total;
}

void KVick::clear() {
    for (size_t i = 0; i < NUM_SHARDS; ++i) {
        std::unique_lock<std::shared_mutex> lock(shards[i].mutex);
        shards[i].map_ptr = std::make_shared<ShardMap>();
    }
}

std::vector<std::string> KVick::keys() const {
    std::vector<std::string> result;
    for (size_t i = 0; i < NUM_SHARDS; ++i) {
        auto map = getShardMap(i);
        for (const auto& [key, val_ptr] : *map) {
            result.push_back(key);
        }
    }
    return result;
}

void KVick::printValue(const ValueType& value) const {
    std::visit([](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, bool>) std::cout << (v ? "true" : "false");
        else if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, int> || std::is_same_v<T, double>) std::cout << v;
        else if constexpr (std::is_same_v<T, std::vector<std::string>> || std::is_same_v<T, std::vector<int>> || std::is_same_v<T, std::vector<bool>> || std::is_same_v<T, std::vector<double>>) {
            std::cout << "[";
            for (size_t i = 0; i < v.size(); ++i) {
                if constexpr (std::is_same_v<T, std::vector<bool>>) std::cout << (v[i] ? "true" : "false");
                else std::cout << v[i];
                if (i + 1 < v.size()) std::cout << ", ";
            }
            std::cout << "]";
        }
    }, value);
}

KVick::ValueType KVick::parseLiteral(const std::string& val) {
    if (val.empty()) return val;
    
    // Vector parsing: [1,2,3] or ["a","b"]
    if (val.front() == '[' && val.back() == ']') {
        std::string inner = val.substr(1, val.length() - 2);
        std::vector<std::string> parts;
        std::stringstream ss(inner);
        std::string item;
        while (std::getline(ss, item, ',')) {
            size_t start = item.find_first_not_of(" ");
            size_t end = item.find_last_not_of(" ");
            if (start != std::string::npos) parts.push_back(item.substr(start, end - start + 1));
        }

        if (parts.empty()) return std::vector<std::string>{};

        // Try to infer type of vector
        bool all_int = true, all_double = true, all_bool = true;
        for (const auto& p : parts) {
            if (p != "true" && p != "false") all_bool = false;
            try { size_t pos; std::stoi(p, &pos); if (pos != p.length()) all_int = false; } catch (...) { all_int = false; }
            try { size_t pos; std::stod(p, &pos); if (pos != p.length()) all_double = false; } catch (...) { all_double = false; }
        }

        if (all_bool) {
            std::vector<bool> v; for (const auto& p : parts) v.push_back(p == "true");
            return v;
        }
        if (all_int) {
            std::vector<int> v; for (const auto& p : parts) v.push_back(std::stoi(p));
            return v;
        }
        if (all_double) {
            std::vector<double> v; for (const auto& p : parts) v.push_back(std::stod(p));
            return v;
        }
        
        // Default to vector<string>, stripping quotes if present
        std::vector<std::string> v;
        for (auto& p : parts) {
            if (p.size() >= 2 && p.front() == '"' && p.back() == '"') v.push_back(p.substr(1, p.length() - 2));
            else v.push_back(p);
        }
        return v;
    }

    // Scalar parsing
    if (val == "true") return true;
    if (val == "false") return false;
    
    try {
        size_t pos;
        if (val.find('.') != std::string::npos) {
            double d = std::stod(val, &pos);
            if (pos == val.length()) return d;
        } else {
            int i = std::stoi(val, &pos);
            if (pos == val.length()) return i;
        }
    } catch (...) {}

    if (val.size() >= 2 && val.front() == '"' && val.back() == '"') return val.substr(1, val.length() - 2);
    return val;
}

namespace {
    enum class TypeTag : uint8_t { String = 0, Int = 1, Bool = 2, Double = 3, VecString = 4, VecInt = 5, VecBool = 6, VecDouble = 7 };
    void writeString(std::ostream& os, const std::string& s) {
        uint32_t len = static_cast<uint32_t>(s.size());
        os.write(reinterpret_cast<const char*>(&len), sizeof(len));
        os.write(s.data(), len);
    }
    std::string readString(std::istream& is) {
        uint32_t len = 0;
        is.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (is.eof()) return "";
        std::string s(len, '\0');
        is.read(&s[0], len);
        return s;
    }
}

bool KVick::saveToFile(const std::string& filename) const {
    std::vector<std::shared_ptr<ShardMap>> snapshots;
    snapshots.reserve(NUM_SHARDS);
    for (size_t i = 0; i < NUM_SHARDS; ++i) snapshots.push_back(getShardMap(i));
    
    std::string temp_filename = filename + ".tmp";
    {
        std::ofstream os(temp_filename, std::ios::binary);
        if (!os.is_open()) return false;
        os.write("KV01", 4);
        uint64_t total_count = 0;
        for (const auto& snap : snapshots) total_count += snap->size();
        os.write(reinterpret_cast<const char*>(&total_count), sizeof(total_count));
        
        for (const auto& snap : snapshots) {
            for (const auto& [key, val_ptr] : *snap) {
                writeString(os, key);
                std::visit([&os](const auto& v) {
                    using T = std::decay_t<decltype(v)>;
                    if constexpr (std::is_same_v<T, std::string>) { os.put(static_cast<char>(TypeTag::String)); writeString(os, v); }
                    else if constexpr (std::is_same_v<T, int>) { os.put(static_cast<char>(TypeTag::Int)); os.write(reinterpret_cast<const char*>(&v), sizeof(v)); }
                    else if constexpr (std::is_same_v<T, bool>) { os.put(static_cast<char>(TypeTag::Bool)); os.put(v ? 1 : 0); }
                    else if constexpr (std::is_same_v<T, double>) { os.put(static_cast<char>(TypeTag::Double)); os.write(reinterpret_cast<const char*>(&v), sizeof(v)); }
                    else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
                        os.put(static_cast<char>(TypeTag::VecString));
                        uint32_t vc = static_cast<uint32_t>(v.size());
                        os.write(reinterpret_cast<const char*>(&vc), sizeof(vc));
                        for (const auto& s : v) writeString(os, s);
                    } else if constexpr (std::is_same_v<T, std::vector<int>>) {
                        os.put(static_cast<char>(TypeTag::VecInt));
                        uint32_t vc = static_cast<uint32_t>(v.size());
                        os.write(reinterpret_cast<const char*>(&vc), sizeof(vc));
                        os.write(reinterpret_cast<const char*>(v.data()), vc * sizeof(int));
                    } else if constexpr (std::is_same_v<T, std::vector<double>>) {
                        os.put(static_cast<char>(TypeTag::VecDouble));
                        uint32_t vc = static_cast<uint32_t>(v.size());
                        os.write(reinterpret_cast<const char*>(&vc), sizeof(vc));
                        os.write(reinterpret_cast<const char*>(v.data()), vc * sizeof(double));
                    } else if constexpr (std::is_same_v<T, std::vector<bool>>) {
                        os.put(static_cast<char>(TypeTag::VecBool));
                        uint32_t vc = static_cast<uint32_t>(v.size());
                        os.write(reinterpret_cast<const char*>(&vc), sizeof(vc));
                        for (bool b : v) os.put(b ? 1 : 0);
                    }
                }, *val_ptr);
            }
        }
        os.flush();
        int fd = open(temp_filename.c_str(), O_WRONLY);
        if (fd != -1) { fsync(fd); close(fd); }
    }
    std::rename(temp_filename.c_str(), filename.c_str());
    return true;
}

bool KVick::loadFromFile(const std::string& filename) {
    std::ifstream is(filename, std::ios::binary);
    if (!is.is_open()) return false;
    char magic[4]; is.read(magic, 4);
    if (std::string(magic, 4) != "KV01") return false;
    uint64_t count = 0; is.read(reinterpret_cast<char*>(&count), sizeof(count));
    for (uint64_t i = 0; i < count; ++i) {
        std::string key = readString(is);
        uint8_t tag_val = is.get();
        TypeTag tag = static_cast<TypeTag>(tag_val);
        std::shared_ptr<ValueType> val_ptr;
        switch (tag) {
            case TypeTag::String: val_ptr = std::make_shared<ValueType>(readString(is)); break;
            case TypeTag::Int: { int v; is.read(reinterpret_cast<char*>(&v), sizeof(v)); val_ptr = std::make_shared<ValueType>(v); break; }
            case TypeTag::Bool: { val_ptr = std::make_shared<ValueType>(is.get() != 0); break; }
            case TypeTag::Double: { double v; is.read(reinterpret_cast<char*>(&v), sizeof(v)); val_ptr = std::make_shared<ValueType>(v); break; }
            case TypeTag::VecString: { uint32_t vc; is.read(reinterpret_cast<char*>(&vc), sizeof(vc)); std::vector<std::string> v(vc); for(auto& s:v) s=readString(is); val_ptr=std::make_shared<ValueType>(v); break; }
            case TypeTag::VecInt: { uint32_t vc; is.read(reinterpret_cast<char*>(&vc), sizeof(vc)); std::vector<int> v(vc); is.read(reinterpret_cast<char*>(v.data()), vc*sizeof(int)); val_ptr=std::make_shared<ValueType>(v); break; }
            case TypeTag::VecDouble: { uint32_t vc; is.read(reinterpret_cast<char*>(&vc), sizeof(vc)); std::vector<double> v(vc); is.read(reinterpret_cast<char*>(v.data()), vc*sizeof(double)); val_ptr=std::make_shared<ValueType>(v); break; }
            case TypeTag::VecBool: { uint32_t vc; is.read(reinterpret_cast<char*>(&vc), sizeof(vc)); std::vector<bool> v(vc); for(uint32_t j=0; j<vc; ++j) v[j]=(is.get()!=0); val_ptr=std::make_shared<ValueType>(v); break; }
            default: break;
        }
        size_t idx = getShardIndex(key);
        std::unique_lock<std::shared_mutex> lock(shards[idx].mutex);
        auto new_map = std::make_shared<ShardMap>(*shards[idx].map_ptr);
        (*new_map)[key] = std::move(val_ptr);
        shards[idx].map_ptr = std::move(new_map);
    }
    return true;
}

void KVick::enableAutoPersist(const std::string& filename, int intervalSeconds) {
    persist_filename = filename;
    auto_persist_enabled = true;
    persist_thread = std::thread([this, intervalSeconds]() {
        while (auto_persist_enabled) {
            std::this_thread::sleep_for(std::chrono::seconds(intervalSeconds));
            if (auto_persist_enabled) saveToFile(persist_filename);
        }
    });
}

void KVick::disableAutoPersist() {
    auto_persist_enabled = false;
    if (persist_thread.joinable()) persist_thread.join();
}

void KVick::openWAL(const std::string& filename) {
    wal_filename = filename;
    wal_stream.open(filename, std::ios::app);
}

void KVick::logToWAL(const std::string& op, const std::string& key, const std::string& value) {
    if (wal_stream.is_open()) {
        wal_stream << op << " " << key << " " << value << "\n";
        wal_stream.flush();
    }
}

void KVick::replayWAL() {
    std::ifstream file(wal_filename);
    if (!file.is_open()) return;
    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string op, key, val_str;
        iss >> op >> key;
        if (op == "SET") {
            std::getline(iss, val_str);
            if (!val_str.empty() && val_str.front() == ' ') val_str.erase(0, 1);
            set(key, parseLiteral(val_str), false);
        } else if (op == "DEL") del(key, false);
    }
}
