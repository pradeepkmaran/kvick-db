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
#include <set>
#include <sstream>

// ---------- Clock Logic ----------

KVick::ClockComparison KVick::compareClocks(const VectorClock& a, const VectorClock& b) {
    bool a_greater = false;
    bool b_greater = false;

    // Use a set to get all keys from both maps
    std::set<std::string> nodes;
    for (const auto& [node, _] : a) nodes.insert(node);
    for (const auto& [node, _] : b) nodes.insert(node);

    for (const auto& node : nodes) {
        uint32_t v_a = 0;
        auto it_a = a.find(node);
        if (it_a != a.end()) v_a = it_a->second;

        uint32_t v_b = 0;
        auto it_b = b.find(node);
        if (it_b != b.end()) v_b = it_b->second;

        if (v_a > v_b) a_greater = true;
        else if (v_b > v_a) b_greater = true;
    }

    if (a_greater && b_greater) return ClockComparison::CONCURRENT;
    if (a_greater) return ClockComparison::DOMINATES;
    if (b_greater) return ClockComparison::DOMINATED;
    return ClockComparison::IDENTICAL;
}

static void resolveCausal(KVick::SiblingList& siblings, KVick::VersionedValue incoming) {
    auto it = siblings.begin();
    bool should_add = true;
    while (it != siblings.end()) {
        KVick::ClockComparison cmp = KVick::compareClocks(incoming.clock, it->clock);
        if (cmp == KVick::ClockComparison::DOMINATES || cmp == KVick::ClockComparison::IDENTICAL) {
            it = siblings.erase(it);
        } else if (cmp == KVick::ClockComparison::DOMINATED) {
            should_add = false;
            ++it;
            break; 
        } else {
            ++it;
        }
    }
    if (should_add) {
        siblings.push_back(std::move(incoming));
    }
}

// ---------- Read / Exists / Del ----------

void KVick::set(const std::string& key, const ValueType& value, const VectorClock& clock) {
    size_t idx = getShardIndex(key);
    std::unique_lock<std::shared_mutex> lock(shards[idx].mutex);
    
    auto it = shards[idx].map.find(key);
    if (it == shards[idx].map.end()) {
        auto siblings = std::make_shared<SiblingList>();
        siblings->emplace_back(value, clock, false);
        shards[idx].map[key] = siblings;
    } else {
        // COW (Copy-On-Write) for the SiblingList if shared
        auto new_siblings = std::make_shared<SiblingList>(*it->second);
        resolveCausal(*new_siblings, VersionedValue(value, clock, false));
        it->second = new_siblings;
    }
}

void KVick::del(const std::string& key, const VectorClock& clock) {
    size_t idx = getShardIndex(key);
    std::unique_lock<std::shared_mutex> lock(shards[idx].mutex);

    auto it = shards[idx].map.find(key);
    if (it == shards[idx].map.end()) {
        auto siblings = std::make_shared<SiblingList>();
        siblings->emplace_back(std::string(""), clock, true); // Tombstone
        shards[idx].map[key] = siblings;
    } else {
        auto new_siblings = std::make_shared<SiblingList>(*it->second);
        resolveCausal(*new_siblings, VersionedValue(std::string(""), clock, true));
        it->second = new_siblings;
    }
}

std::shared_ptr<KVick::SiblingList> KVick::get(const std::string& key) const {
    size_t idx = getShardIndex(key);
    std::shared_lock<std::shared_mutex> lock(shards[idx].mutex);
    auto it = shards[idx].map.find(key);
    if (it != shards[idx].map.end()) {
        return it->second;
    }
    throw std::runtime_error("Key not found: " + key);
}

bool KVick::exists(const std::string& key) const {
    size_t idx = getShardIndex(key);
    std::shared_lock<std::shared_mutex> lock(shards[idx].mutex);
    auto it = shards[idx].map.find(key);
    if (it == shards[idx].map.end()) return false;
    
    // Check if at least one sibling is not a tombstone
    for (const auto& s : *it->second) {
        if (!s.is_tombstone) return true;
    }
    return false;
}

// ---------- Type / Print ----------

std::string KVick::getType(const std::string& key) const {
    auto siblings = get(key);
    std::set<std::string> types;
    for (const auto& s : *siblings) {
        if (s.is_tombstone) {
            types.insert("tombstone");
            continue;
        }
        std::string t = std::visit([](const auto& value) -> std::string {
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
        }, s.value);
        types.insert(t);
    }

    std::string result;
    for (auto it = types.begin(); it != types.end(); ++it) {
        if (it != types.begin()) result += ",";
        result += *it;
    }
    return result;
}

void KVick::print(const std::string& key) const {
    try {
        auto siblings = get(key);
        std::cout << key << " (" << siblings->size() << " siblings):" << std::endl;
        for (size_t i = 0; i < siblings->size(); ++i) {
            const auto& s = (*siblings)[i];
            std::cout << "  [" << i << "] ";
            if (s.is_tombstone) std::cout << "(TOMBSTONE) ";
            printValue(s.value);
            std::cout << " | Clock: {";
            for (auto it = s.clock.begin(); it != s.clock.end(); ++it) {
                if (it != s.clock.begin()) std::cout << ", ";
                std::cout << it->first << ":" << it->second;
            }
            std::cout << "}" << std::endl;
        }
    } catch (...) {
        std::cout << "Key '" << key << "' not found" << std::endl;
    }
}

void KVick::printAll() const {
    for (size_t i = 0; i < NUM_SHARDS; ++i) {
        std::shared_lock<std::shared_mutex> lock(shards[i].mutex);
        for (const auto& [key, siblings] : shards[i].map) {
            print(key);
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
        for (const auto& [key, siblings] : shards[i].map) {
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

// ---------- Binary persistence ----------

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

    void writeClock(std::ostream& os, const KVick::VectorClock& clock) {
        uint32_t size = static_cast<uint32_t>(clock.size());
        os.write(reinterpret_cast<const char*>(&size), sizeof(size));
        for (const auto& [node, count] : clock) {
            writeString(os, node);
            os.write(reinterpret_cast<const char*>(&count), sizeof(count));
        }
    }

    KVick::VectorClock readClock(std::istream& is) {
        uint32_t size = 0;
        is.read(reinterpret_cast<char*>(&size), sizeof(size));
        KVick::VectorClock clock;
        for (uint32_t i = 0; i < size; ++i) {
            std::string node = readString(is);
            uint32_t count = 0;
            is.read(reinterpret_cast<char*>(&count), sizeof(count));
            clock[node] = count;
        }
        return clock;
    }

    void writeValue(std::ostream& os, const KVick::ValueType& v) {
        std::visit([&os](const auto& val) {
            using T = std::decay_t<decltype(val)>;
            if constexpr (std::is_same_v<T, std::string>) { os.put(static_cast<char>(TypeTag::String)); writeString(os, val); }
            else if constexpr (std::is_same_v<T, int64_t>) { os.put(static_cast<char>(TypeTag::Int64)); os.write(reinterpret_cast<const char*>(&val), sizeof(val)); }
            else if constexpr (std::is_same_v<T, bool>) { os.put(static_cast<char>(TypeTag::Bool)); os.put(val ? 1 : 0); }
            else if constexpr (std::is_same_v<T, double>) { os.put(static_cast<char>(TypeTag::Double)); os.write(reinterpret_cast<const char*>(&val), sizeof(val)); }
            else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
                os.put(static_cast<char>(TypeTag::VecString));
                uint32_t vc = static_cast<uint32_t>(val.size());
                os.write(reinterpret_cast<const char*>(&vc), sizeof(vc));
                for (const auto& s : val) writeString(os, s);
            } else if constexpr (std::is_same_v<T, std::vector<int64_t>>) {
                os.put(static_cast<char>(TypeTag::VecInt64));
                uint32_t vc = static_cast<uint32_t>(val.size());
                os.write(reinterpret_cast<const char*>(&vc), sizeof(vc));
                os.write(reinterpret_cast<const char*>(val.data()), vc * sizeof(int64_t));
            } else if constexpr (std::is_same_v<T, std::vector<double>>) {
                os.put(static_cast<char>(TypeTag::VecDouble));
                uint32_t vc = static_cast<uint32_t>(val.size());
                os.write(reinterpret_cast<const char*>(&vc), sizeof(vc));
                os.write(reinterpret_cast<const char*>(val.data()), vc * sizeof(double));
            } else if constexpr (std::is_same_v<T, std::vector<bool>>) {
                os.put(static_cast<char>(TypeTag::VecBool));
                uint32_t vc = static_cast<uint32_t>(val.size());
                os.write(reinterpret_cast<const char*>(&vc), sizeof(vc));
                for (size_t i = 0; i < val.size(); ++i) os.put(val[i] ? 1 : 0);
            }
        }, v);
    }

    KVick::ValueType readValue(std::istream& is, bool legacy = false) {
        uint8_t tag_val = is.get();
        TypeTag tag = static_cast<TypeTag>(tag_val);
        switch (tag) {
            case TypeTag::String: return readString(is);
            case TypeTag::Int64: {
                if (legacy) {
                    int v; is.read(reinterpret_cast<char*>(&v), sizeof(v));
                    return static_cast<int64_t>(v);
                } else {
                    int64_t v; is.read(reinterpret_cast<char*>(&v), sizeof(v));
                    return v;
                }
            }
            case TypeTag::Bool: return is.get() != 0;
            case TypeTag::Double: { double v; is.read(reinterpret_cast<char*>(&v), sizeof(v)); return v; }
            case TypeTag::VecString: {
                uint32_t vc; is.read(reinterpret_cast<char*>(&vc), sizeof(vc));
                std::vector<std::string> v(vc);
                for(auto& s:v) s=readString(is);
                return v;
            }
            case TypeTag::VecInt64: {
                uint32_t vc; is.read(reinterpret_cast<char*>(&vc), sizeof(vc));
                if (legacy) {
                    std::vector<int> tmp(vc);
                    is.read(reinterpret_cast<char*>(tmp.data()), vc*sizeof(int));
                    return std::vector<int64_t>(tmp.begin(), tmp.end());
                } else {
                    std::vector<int64_t> v(vc);
                    is.read(reinterpret_cast<char*>(v.data()), vc*sizeof(int64_t));
                    return v;
                }
            }
            case TypeTag::VecDouble: {
                uint32_t vc; is.read(reinterpret_cast<char*>(&vc), sizeof(vc));
                std::vector<double> v(vc);
                is.read(reinterpret_cast<char*>(v.data()), vc*sizeof(double));
                return v;
            }
            case TypeTag::VecBool: {
                uint32_t vc; is.read(reinterpret_cast<char*>(&vc), sizeof(vc));
                std::vector<bool> v(vc);
                for(uint32_t j=0; j<vc; ++j) v[j]=(is.get()!=0);
                return v;
            }
            default: return std::string("");
        }
    }
}

bool KVick::saveToFile(const std::string& filename) const {
    std::vector<ShardMap> snapshots(NUM_SHARDS);
    for (size_t i = 0; i < NUM_SHARDS; ++i) {
        std::shared_lock<std::shared_mutex> lock(shards[i].mutex);
        snapshots[i] = shards[i].map;
    }

    std::string temp_filename = filename + ".tmp";
    {
        std::ofstream os(temp_filename, std::ios::binary);
        if (!os.is_open()) return false;
        os.write("KV03", 4);
        uint64_t total_count = 0;
        for (const auto& snap : snapshots) total_count += snap.size();
        os.write(reinterpret_cast<const char*>(&total_count), sizeof(total_count));

        for (const auto& snap : snapshots) {
            for (const auto& [key, siblings] : snap) {
                writeString(os, key);
                uint32_t sc = static_cast<uint32_t>(siblings->size());
                os.write(reinterpret_cast<const char*>(&sc), sizeof(sc));
                for (const auto& s : *siblings) {
                    os.put(s.is_tombstone ? 1 : 0);
                    writeValue(os, s.value);
                    writeClock(os, s.clock);
                }
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

    if (magic_str == "KV03") {
        uint64_t count = 0; is.read(reinterpret_cast<char*>(&count), sizeof(count));
        std::array<ShardMap, NUM_SHARDS> temp_shards;
        for (uint64_t i = 0; i < count; ++i) {
            std::string key = readString(is);
            uint32_t sc = 0; is.read(reinterpret_cast<char*>(&sc), sizeof(sc));
            auto siblings = std::make_shared<SiblingList>();
            for (uint32_t j = 0; j < sc; ++j) {
                bool is_tombstone = (is.get() != 0);
                ValueType val = readValue(is);
                VectorClock clock = readClock(is);
                siblings->emplace_back(std::move(val), std::move(clock), is_tombstone);
            }
            size_t idx = getShardIndex(key);
            temp_shards[idx][key] = std::move(siblings);
        }
        for (size_t i = 0; i < NUM_SHARDS; ++i) {
            std::unique_lock<std::shared_mutex> lock(shards[i].mutex);
            shards[i].map.merge(temp_shards[i]);
        }
        return true;
    } else if (magic_str == "KV02" || magic_str == "KV01") {
        // Migration path
        bool legacy = (magic_str == "KV01");
        uint64_t count = 0; is.read(reinterpret_cast<char*>(&count), sizeof(count));
        std::array<ShardMap, NUM_SHARDS> temp_shards;
        for (uint64_t i = 0; i < count; ++i) {
            std::string key = readString(is);
            ValueType val = readValue(is, legacy);
            auto siblings = std::make_shared<SiblingList>();
            siblings->emplace_back(std::move(val), VectorClock{}, false);
            size_t idx = getShardIndex(key);
            temp_shards[idx][key] = std::move(siblings);
        }
        for (size_t i = 0; i < NUM_SHARDS; ++i) {
            std::unique_lock<std::shared_mutex> lock(shards[i].mutex);
            shards[i].map.merge(temp_shards[i]);
        }
        return true;
    }
    return false;
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
