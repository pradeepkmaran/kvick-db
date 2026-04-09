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

// ---------- Read / Exists / Del ----------

std::shared_ptr<KVick::ValueType> KVick::get(const std::string& key) const {
    size_t idx = getShardIndex(key);
    std::shared_lock<std::shared_mutex> lock(shards[idx].mutex);
    auto it = shards[idx].map.find(key);
    if (it != shards[idx].map.end()) {
        return it->second; // shared_ptr — safe after lock release
    }
    throw std::runtime_error("Key not found: " + key);
}

bool KVick::exists(const std::string& key) const {
    size_t idx = getShardIndex(key);
    std::shared_lock<std::shared_mutex> lock(shards[idx].mutex);
    return shards[idx].map.find(key) != shards[idx].map.end();
}

bool KVick::del(const std::string& key) {
    size_t idx = getShardIndex(key);
    std::unique_lock<std::shared_mutex> lock(shards[idx].mutex);
    return shards[idx].map.erase(key) > 0;
}

// ---------- Type / Print ----------

std::string KVick::getType(const std::string& key) const {
    auto val_ptr = get(key);
    return std::visit([](const auto& value) -> std::string {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::string>) return "string";
        else if constexpr (std::is_same_v<T, int64_t>) return "int64";
        else if constexpr (std::is_same_v<T, bool>) return "bool";
        else if constexpr (std::is_same_v<T, double>) return "double";
        else if constexpr (std::is_same_v<T, std::vector<std::string>>) return "vector<string>";
        else if constexpr (std::is_same_v<T, std::vector<int64_t>>) return "vector<int64>";
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
        std::shared_lock<std::shared_mutex> lock(shards[i].mutex);
        for (const auto& [key, val_ptr] : shards[i].map) {
            std::cout << key << ": ";
            printValue(*val_ptr);
            std::cout << std::endl;
        }
    }
}

size_t KVick::size() const {
    size_t total = 0;
    for (size_t i = 0; i < NUM_SHARDS; ++i) {
        std::shared_lock<std::shared_mutex> lock(shards[i].mutex);
        total += shards[i].map.size();
    }
    return total;
}

void KVick::clear() {
    for (size_t i = 0; i < NUM_SHARDS; ++i) {
        std::unique_lock<std::shared_mutex> lock(shards[i].mutex);
        shards[i].map.clear();
    }
}

std::vector<std::string> KVick::keys() const {
    std::vector<std::string> result;
    for (size_t i = 0; i < NUM_SHARDS; ++i) {
        std::shared_lock<std::shared_mutex> lock(shards[i].mutex);
        for (const auto& [key, val_ptr] : shards[i].map) {
            result.push_back(key);
        }
    }
    return result;
}

void KVick::printValue(const ValueType& value) const {
    std::visit([](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, bool>) std::cout << (v ? "true" : "false");
        else if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, int64_t> || std::is_same_v<T, double>) std::cout << v;
        else if constexpr (std::is_same_v<T, std::vector<std::string>> || std::is_same_v<T, std::vector<int64_t>> || std::is_same_v<T, std::vector<bool>> || std::is_same_v<T, std::vector<double>>) {
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

// ---------- Parse literal ----------

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

        bool all_int = true, all_double = true, all_bool = true;
        for (const auto& p : parts) {
            if (p != "true" && p != "false") all_bool = false;
            try { size_t pos; std::stoll(p, &pos); if (pos != p.length()) all_int = false; } catch (...) { all_int = false; }
            try { size_t pos; std::stod(p, &pos); if (pos != p.length()) all_double = false; } catch (...) { all_double = false; }
        }

        if (all_bool) {
            std::vector<bool> v; for (const auto& p : parts) v.push_back(p == "true");
            return v;
        }
        if (all_int) {
            std::vector<int64_t> v; for (const auto& p : parts) v.push_back(std::stoll(p));
            return v;
        }
        if (all_double) {
            std::vector<double> v; for (const auto& p : parts) v.push_back(std::stod(p));
            return v;
        }

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
            int64_t i = std::stoll(val, &pos);
            if (pos == val.length()) return i;
        }
    } catch (...) {}

    if (val.size() >= 2 && val.front() == '"' && val.back() == '"') return val.substr(1, val.length() - 2);
    return val;
}

// ---------- Binary persistence (KV02 format with int64_t) ----------

namespace {
    enum class TypeTag : uint8_t { String = 0, Int64 = 1, Bool = 2, Double = 3, VecString = 4, VecInt64 = 5, VecBool = 6, VecDouble = 7 };
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
    // Snapshot each shard under its shared_lock (cheap — copies shared_ptrs, not values)
    std::vector<ShardMap> snapshots(NUM_SHARDS);
    for (size_t i = 0; i < NUM_SHARDS; ++i) {
        std::shared_lock<std::shared_mutex> lock(shards[i].mutex);
        snapshots[i] = shards[i].map;
    }

    std::string temp_filename = filename + ".tmp";
    {
        std::ofstream os(temp_filename, std::ios::binary);
        if (!os.is_open()) return false;
        os.write("KV02", 4);
        uint64_t total_count = 0;
        for (const auto& snap : snapshots) total_count += snap.size();
        os.write(reinterpret_cast<const char*>(&total_count), sizeof(total_count));

        for (const auto& snap : snapshots) {
            for (const auto& [key, val_ptr] : snap) {
                writeString(os, key);
                std::visit([&os](const auto& v) {
                    using T = std::decay_t<decltype(v)>;
                    if constexpr (std::is_same_v<T, std::string>) { os.put(static_cast<char>(TypeTag::String)); writeString(os, v); }
                    else if constexpr (std::is_same_v<T, int64_t>) { os.put(static_cast<char>(TypeTag::Int64)); os.write(reinterpret_cast<const char*>(&v), sizeof(v)); }
                    else if constexpr (std::is_same_v<T, bool>) { os.put(static_cast<char>(TypeTag::Bool)); os.put(v ? 1 : 0); }
                    else if constexpr (std::is_same_v<T, double>) { os.put(static_cast<char>(TypeTag::Double)); os.write(reinterpret_cast<const char*>(&v), sizeof(v)); }
                    else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
                        os.put(static_cast<char>(TypeTag::VecString));
                        uint32_t vc = static_cast<uint32_t>(v.size());
                        os.write(reinterpret_cast<const char*>(&vc), sizeof(vc));
                        for (const auto& s : v) writeString(os, s);
                    } else if constexpr (std::is_same_v<T, std::vector<int64_t>>) {
                        os.put(static_cast<char>(TypeTag::VecInt64));
                        uint32_t vc = static_cast<uint32_t>(v.size());
                        os.write(reinterpret_cast<const char*>(&vc), sizeof(vc));
                        os.write(reinterpret_cast<const char*>(v.data()), vc * sizeof(int64_t));
                    } else if constexpr (std::is_same_v<T, std::vector<double>>) {
                        os.put(static_cast<char>(TypeTag::VecDouble));
                        uint32_t vc = static_cast<uint32_t>(v.size());
                        os.write(reinterpret_cast<const char*>(&vc), sizeof(vc));
                        os.write(reinterpret_cast<const char*>(v.data()), vc * sizeof(double));
                    } else if constexpr (std::is_same_v<T, std::vector<bool>>) {
                        os.put(static_cast<char>(TypeTag::VecBool));
                        uint32_t vc = static_cast<uint32_t>(v.size());
                        os.write(reinterpret_cast<const char*>(&vc), sizeof(vc));
                        for (size_t i = 0; i < v.size(); ++i) os.put(v[i] ? 1 : 0);
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
    std::string magic_str(magic, 4);
    if (magic_str != "KV02" && magic_str != "KV01") return false;

    bool legacy = (magic_str == "KV01");

    uint64_t count = 0; is.read(reinterpret_cast<char*>(&count), sizeof(count));

    // Bulk-load into temporary per-shard maps to avoid per-key locking
    std::array<ShardMap, NUM_SHARDS> temp_shards;

    for (uint64_t i = 0; i < count; ++i) {
        std::string key = readString(is);
        uint8_t tag_val = is.get();
        TypeTag tag = static_cast<TypeTag>(tag_val);
        std::shared_ptr<ValueType> val_ptr;
        switch (tag) {
            case TypeTag::String: val_ptr = std::make_shared<ValueType>(readString(is)); break;
            case TypeTag::Int64: {
                if (legacy) {
                    int v; is.read(reinterpret_cast<char*>(&v), sizeof(v));
                    val_ptr = std::make_shared<ValueType>(static_cast<int64_t>(v));
                } else {
                    int64_t v; is.read(reinterpret_cast<char*>(&v), sizeof(v));
                    val_ptr = std::make_shared<ValueType>(v);
                }
                break;
            }
            case TypeTag::Bool: { val_ptr = std::make_shared<ValueType>(is.get() != 0); break; }
            case TypeTag::Double: { double v; is.read(reinterpret_cast<char*>(&v), sizeof(v)); val_ptr = std::make_shared<ValueType>(v); break; }
            case TypeTag::VecString: {
                uint32_t vc; is.read(reinterpret_cast<char*>(&vc), sizeof(vc));
                std::vector<std::string> v(vc);
                for(auto& s:v) s=readString(is);
                val_ptr=std::make_shared<ValueType>(v);
                break;
            }
            case TypeTag::VecInt64: {
                uint32_t vc; is.read(reinterpret_cast<char*>(&vc), sizeof(vc));
                if (legacy) {
                    std::vector<int> tmp(vc);
                    is.read(reinterpret_cast<char*>(tmp.data()), vc*sizeof(int));
                    std::vector<int64_t> v(tmp.begin(), tmp.end());
                    val_ptr=std::make_shared<ValueType>(v);
                } else {
                    std::vector<int64_t> v(vc);
                    is.read(reinterpret_cast<char*>(v.data()), vc*sizeof(int64_t));
                    val_ptr=std::make_shared<ValueType>(v);
                }
                break;
            }
            case TypeTag::VecDouble: {
                uint32_t vc; is.read(reinterpret_cast<char*>(&vc), sizeof(vc));
                std::vector<double> v(vc);
                is.read(reinterpret_cast<char*>(v.data()), vc*sizeof(double));
                val_ptr=std::make_shared<ValueType>(v);
                break;
            }
            case TypeTag::VecBool: {
                uint32_t vc; is.read(reinterpret_cast<char*>(&vc), sizeof(vc));
                std::vector<bool> v(vc);
                for(uint32_t j=0; j<vc; ++j) v[j]=(is.get()!=0);
                val_ptr=std::make_shared<ValueType>(v);
                break;
            }
            default: break;
        }
        size_t idx = getShardIndex(key);
        temp_shards[idx][key] = std::move(val_ptr);
    }

    // Swap into real shards — one lock per shard
    for (size_t i = 0; i < NUM_SHARDS; ++i) {
        std::unique_lock<std::shared_mutex> lock(shards[i].mutex);
        shards[i].map.merge(temp_shards[i]);
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
