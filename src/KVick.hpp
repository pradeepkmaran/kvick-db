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

#include "JSONSerializer.hpp"  

#include <memory>

class KVick {
public:
    using Primitive = std::variant<std::string, int, bool, double>;
    using ValueType = std::variant<
        std::string, 
        int, 
        bool, 
        double,
        std::vector<std::string>,
        std::vector<int>,
        std::vector<bool>,
        std::vector<double>
    >;

private:
    using ShardMap = std::unordered_map<std::string, std::shared_ptr<ValueType>>;
    
    struct Shard {
        std::shared_ptr<ShardMap> map_ptr;
        mutable std::shared_mutex mutex;

        Shard() : map_ptr(std::make_shared<ShardMap>()) {}
    };

    static constexpr size_t NUM_SHARDS = 64;
    std::array<Shard, NUM_SHARDS> shards;

    size_t getShardIndex(const std::string& key) const {
        return std::hash<std::string>{}(key) % NUM_SHARDS;
    }
    
    std::shared_ptr<ShardMap> getShardMap(size_t idx) const {
        std::shared_lock<std::shared_mutex> lock(shards[idx].mutex);
        return shards[idx].map_ptr;
    }

    std::string persist_filename;
    std::string wal_filename;
    std::thread persist_thread;
    mutable std::ofstream wal_stream;
    mutable std::mutex wal_mutex;
    
public:
    template<typename T>
    void set(const std::string& key, const T& value, bool use_wal = true);

    std::shared_ptr<ValueType> get(const std::string& key) const;
    bool exists(const std::string& key) const;
    bool del(const std::string& key, bool use_wal = true);
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
    
    // WAL methods
    void openWAL(const std::string& filename);
    void replayWAL();

    static ValueType parseLiteral(const std::string& val);

private:
    void logToWAL(const std::string& op, const std::string& key, const std::string& value = "");
    std::atomic<bool> auto_persist_enabled{false};

private:
    void printValue(const ValueType& value) const;
};

template<typename T>
T KVick::getAs(const std::string& key) const {
    auto val_ptr = get(key);
    return std::get<T>(*val_ptr);
}

template<typename T>
void KVick::set(const std::string& key, const T& value, bool use_wal) {
    if (use_wal) {
        std::lock_guard<std::mutex> wal_lock(wal_mutex);
        logToWAL("SET", key, JSONSerializer::serialize(value));
    }
    
    size_t idx = getShardIndex(key);
    std::unique_lock<std::shared_mutex> lock(shards[idx].mutex);
    
    // Create a new map version (Copy-on-write at the map level for that shard)
    auto new_map = std::make_shared<ShardMap>(*shards[idx].map_ptr);
    (*new_map)[key] = std::make_shared<ValueType>(value);
    shards[idx].map_ptr = std::move(new_map);
}


#endif
