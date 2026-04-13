set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_program(PROTOC_EXECUTABLE protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin REQUIRED)

set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/network/kvick_internal.proto")
set(PROTO_SRC "${CMAKE_CURRENT_BINARY_DIR}/kvick_internal.pb.cc")
set(PROTO_HDR "${CMAKE_CURRENT_BINARY_DIR}/kvick_internal.pb.h")
set(GRPC_SRC "${CMAKE_CURRENT_BINARY_DIR}/kvick_internal.grpc.pb.cc")
set(GRPC_HDR "${CMAKE_CURRENT_BINARY_DIR}/kvick_internal.grpc.pb.h")

add_custom_command(
    OUTPUT "${PROTO_SRC}" "${PROTO_HDR}" "${GRPC_SRC}" "${GRPC_HDR}"
    COMMAND ${PROTOC_EXECUTABLE}
    ARGS --grpc_out="${CMAKE_CURRENT_BINARY_DIR}"
         --cpp_out="${CMAKE_CURRENT_BINARY_DIR}"
         -I "${CMAKE_CURRENT_SOURCE_DIR}/network"
         --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN_EXECUTABLE}"
         "${PROTO_FILE}"
    DEPENDS "${PROTO_FILE}"
)

add_library(kvick_proto
    ${PROTO_SRC}
    ${PROTO_HDR}
    ${GRPC_SRC}
    ${GRPC_HDR}
)
target_link_libraries(kvick_proto PUBLIC PkgConfig::GRPC protobuf::libprotobuf)
target_include_directories(kvick_proto PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")

add_executable(kvick
    core/KVick.cpp
    network/KVickServer.cpp
    consensus/KVickStateMachine.cpp
    consensus/KVickStateManager.cpp
    consensus/RaftManager.cpp
    network/ClusterManager.cpp
    main.cpp
    /nuraft/examples/in_memory_log_store.cxx
)

target_include_directories(kvick PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/core"
    "${CMAKE_CURRENT_SOURCE_DIR}/network"
    "${CMAKE_CURRENT_SOURCE_DIR}/consensus"
    "${CMAKE_CURRENT_SOURCE_DIR}/utils"
    "${NURAFT_INCLUDE_DIR}"
    "${NURAFT_INCLUDE_DIR}/libnuraft"
    /nuraft/examples
)

target_link_libraries(kvick
    PRIVATE
    Threads::Threads
    kvick_proto
    "${NURAFT_LIB}"
    OpenSSL::SSL
    OpenSSL::Crypto
    ZLIB::ZLIB
)