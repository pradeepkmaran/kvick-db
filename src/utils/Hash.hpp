#pragma once
#include <cstdint>
#include <cstddef>
#include <string>

// MurmurHash3 (32-bit) — public-domain, deterministic, cross-platform.
// Replaces std::hash<string> which is non-portable and poorly distributed.
namespace kvick {
    namespace hash {
        inline uint32_t murmur3_32(const void* key, size_t len, uint32_t seed = 0x9747b28c) {
            const uint8_t* data = static_cast<const uint8_t*>(key);
            const int nblocks = static_cast<int>(len / 4);
            uint32_t h1 = seed;

            const uint32_t c1 = 0xcc9e2d51;
            const uint32_t c2 = 0x1b873593;

            const uint32_t* blocks = reinterpret_cast<const uint32_t*>(data);
            for (int i = 0; i < nblocks; i++) {
                uint32_t k1 = blocks[i];
                k1 *= c1;
                k1 = (k1 << 15) | (k1 >> 17);
                k1 *= c2;

                h1 ^= k1;
                h1 = (h1 << 13) | (h1 >> 19);
                h1 = h1 * 5 + 0xe6546b64;
            }

            const uint8_t* tail = data + nblocks * 4;
            uint32_t k1 = 0;
            switch (len & 3) {
                case 3: k1 ^= static_cast<uint32_t>(tail[2]) << 16; [[fallthrough]];
                case 2: k1 ^= static_cast<uint32_t>(tail[1]) << 8;  [[fallthrough]];
                case 1: k1 ^= static_cast<uint32_t>(tail[0]);
                        k1 *= c1; k1 = (k1 << 15) | (k1 >> 17); k1 *= c2; h1 ^= k1;
            }

            h1 ^= static_cast<uint32_t>(len);
            h1 ^= h1 >> 16;
            h1 *= 0x85ebca6b;
            h1 ^= h1 >> 13;
            h1 *= 0xc2b2ae35;
            h1 ^= h1 >> 16;

            return h1;
        }

        inline uint32_t hash_string(const std::string& s, uint32_t seed = 0x9747b28c) {
            return murmur3_32(s.data(), s.size(), seed);
        }

        // Derive a positive int32 server ID from a node_id string (for NuRaft).
        inline int32_t node_id_to_server_id(const std::string& node_id) {
            int32_t id = static_cast<int32_t>(hash_string(node_id) & 0x7FFFFFFF);
            return id == 0 ? 1 : id;
        }
    } // namespace hash
} // namespace kvick
