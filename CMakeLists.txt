cmake_minimum_required(VERSION 3.14)
project(kvick-db LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# --- Dependencies ---

# Threads
find_package(Threads REQUIRED)

# gRPC & Protobuf
find_package(gRPC REQUIRED)
find_package(Protobuf REQUIRED)

# NuRaft
include(FetchContent)
FetchContent_Declare(
  nuraft
  GIT_REPOSITORY https://github.com/eBay/NuRaft.git
  GIT_TAG        master
)
# NuRaft relies on Asio, Zlib, OpenSSL. Make sure they are available.
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)

FetchContent_MakeAvailable(nuraft)

# --- Subdirectories ---
add_subdirectory(src)
