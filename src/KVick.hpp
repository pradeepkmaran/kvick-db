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

#include "JSONSerializer.hpp"
#include "Hash.hpp"

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

private:
    using ShardMap = std::unordered_map<std::string, std::shared_ptr<ValueType>>;

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
    template<typename T>
    void set(const std::string& key, const T& value);

    std::shared_ptr<ValueType> get(const std::string& key) const;
    bool exists(const std::string& key) const;
    bool del(const std::string& key);
    std::string getType(const std::string& key) const;

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
    auto val_ptr = get(key);
    return std::get<T>(*val_ptr);
}

// In-place write under unique_lock — no COW copy of the entire shard map.
template<typename T>
void KVick::set(const std::string& key, const T& value) {
    size_t idx = getShardIndex(key);
    std::unique_lock<std::shared_mutex> lock(shards[idx].mutex);
    shards[idx].map[key] = std::make_shared<ValueType>(value);
}

#endif
