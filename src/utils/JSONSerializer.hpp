#pragma once
#include <string>
#include <sstream>
#include <variant>
#include <vector>
#include <cstdint>

class JSONSerializer {
public:
    template<typename T>
    static std::string serialize(const T& value) {
        using Type = std::decay_t<T>;
        if constexpr (std::is_same_v<Type, std::string>) {
            return "\"" + value + "\"";
        } else if constexpr (std::is_same_v<Type, int64_t>) {
            return std::to_string(value);
        } else if constexpr (std::is_same_v<Type, int>) {
            return std::to_string(value);
        } else if constexpr (std::is_same_v<Type, double>) {
            return std::to_string(value);
        } else if constexpr (std::is_same_v<Type, bool>) {
            return value ? "true" : "false";
        } else if constexpr (std::is_same_v<Type, std::vector<std::string>>) {
            return serializeStringVec(value);
        } else if constexpr (std::is_same_v<Type, std::vector<int64_t>>) {
            return serializeNumVec(value);
        } else if constexpr (std::is_same_v<Type, std::vector<int>>) {
            return serializeNumVec(value);
        } else if constexpr (std::is_same_v<Type, std::vector<double>>) {
            return serializeNumVec(value);
        } else if constexpr (std::is_same_v<Type, std::vector<bool>>) {
            std::string result = "[";
            for (size_t i = 0; i < value.size(); ++i) {
                result += value[i] ? "true" : "false";
                if (i + 1 < value.size()) result += ",";
            }
            return result + "]";
        }
        return "null";
    }

private:
    static std::string serializeStringVec(const std::vector<std::string>& v) {
        std::string result = "[";
        for (size_t i = 0; i < v.size(); ++i) {
            result += "\"" + v[i] + "\"";
            if (i + 1 < v.size()) result += ",";
        }
        return result + "]";
    }

    template<typename T>
    static std::string serializeNumVec(const std::vector<T>& v) {
        std::string result = "[";
        for (size_t i = 0; i < v.size(); ++i) {
            result += std::to_string(v[i]);
            if (i + 1 < v.size()) result += ",";
        }
        return result + "]";
    }
};
