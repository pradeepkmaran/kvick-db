#ifndef KVICK_HPP
#define KVICK_HPP

#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <variant>
#include <stdexcept>
#include <cstdint>
#include <array>
#include <shared_mutex>
#include <functional>
#include <thread>
#include <mutex>
#include <atomic>
#include <memory>
#include <map>

#include "../utils/JSONSerializer.hpp"
#include "../utils/Hash.hpp"

class KVick {
public:
    using ValueType = std::variant<
        std::string,
        int64_t,
        bool,
        double,
        std::vector<std::string>,
        std::vector<int64_t>,
        std::vector<bool>,
        std::vector<double>
    >;

    using VectorClock = std::map<std::string, uint32_t>;

    struct VersionedValue {
        ValueType value;
        VectorClock clock;
        bool is_tombstone;

        VersionedValue(ValueType v, VectorClock c, bool tomb = false)
            : value(std::move(v)), clock(std::move(c)), is_tombstone(tomb) {}
    };

    using SiblingList = std::vector<VersionedValue>;

    enum class ClockComparison {
        IDENTICAL,
        DOMINATES,
        DOMINATED,
        CONCURRENT
    };

    static ClockComparison compareClocks(const VectorClock& a, const VectorClock& b);

private:
    using ShardMap = std::unordered_map<std::string, std::shared_ptr<SiblingList>>;

    struct Shard {
        ShardMap map;
        mutable std::shared_mutex mutex;
    };

    static constexpr size_t NUM_SHARDS = 64;
    std::array<Shard, NUM_SHARDS> shards;

    size_t getShardIndex(const std::string& key) const {
        return kvick::hash::hash_string(key) % NUM_SHARDS;
    }

    std::string persist_filename;
    std::thread persist_thread;

public:
    void set(const std::string& key, const ValueType& value, const VectorClock& clock);
    void del(const std::string& key, const VectorClock& clock);

    std::shared_ptr<SiblingList> get(const std::string& key) const;
    bool exists(const std::string& key) const;
    std::string getType(const std::string& key) const;

    // Returns first non-tombstone sibling's value as T
    template<typename T>
    T getAs(const std::string& key) const;

    void print(const std::string& key) const;
    void printAll() const;
    size_t size() const;
    void clear();
    std::vector<std::string> keys() const;

public:
    bool saveToFile(const std::string& filename) const;
    bool loadFromFile(const std::string& filename);
    void enableAutoPersist(const std::string& filename, int intervalSeconds = 30);
    void disableAutoPersist();

    static ValueType parseLiteral(const std::string& val);

private:
    std::atomic<bool> auto_persist_enabled{false};
    void printValue(const ValueType& value) const;
};

template<typename T>
T KVick::getAs(const std::string& key) const {
    auto siblings = get(key);
    for (const auto& s : *siblings) {
        if (!s.is_tombstone) {
            return std::get<T>(s.value);
        }
    }
    throw std::runtime_error("No non-tombstone siblings for key: " + key);
}

#endif
