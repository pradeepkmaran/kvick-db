cmake_minimum_required(VERSION 3.14)
project(kvick-db LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
find_package(Protobuf REQUIRED)

# gRPC via pkg-config only (apt version on Ubuntu 24.04 has broken CMake config)
find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPC REQUIRED IMPORTED_TARGET grpc++)

add_subdirectory(src)