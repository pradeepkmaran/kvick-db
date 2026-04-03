set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find gRPC and Protobuf generators
find_program(PROTOC_EXECUTABLE protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin REQUIRED)

# Generate gRPC and Protobuf sources
set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/kvick_internal.proto")
set(PROTO_SRC "${CMAKE_CURRENT_BINARY_DIR}/kvick_internal.pb.cc")
set(PROTO_HDR "${CMAKE_CURRENT_BINARY_DIR}/kvick_internal.pb.h")
set(GRPC_SRC "${CMAKE_CURRENT_BINARY_DIR}/kvick_internal.grpc.pb.cc")
set(GRPC_HDR "${CMAKE_CURRENT_BINARY_DIR}/kvick_internal.grpc.pb.h")

add_custom_command(
    OUTPUT "${PROTO_SRC}" "${PROTO_HDR}" "${GRPC_SRC}" "${GRPC_HDR}"
    COMMAND ${PROTOC_EXECUTABLE}
    ARGS --grpc_out="${CMAKE_CURRENT_BINARY_DIR}"
         --cpp_out="${CMAKE_CURRENT_BINARY_DIR}"
         -I "${CMAKE_CURRENT_SOURCE_DIR}"
         --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN_EXECUTABLE}"
         "${PROTO_FILE}"
    DEPENDS "${PROTO_FILE}"
)

# Create a library for the generated proto files
add_library(kvick_proto
    ${PROTO_SRC}
    ${PROTO_HDR}
    ${GRPC_SRC}
    ${GRPC_HDR}
)
target_link_libraries(kvick_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)
target_include_directories(kvick_proto PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")

# Main Executable
add_executable(kvick
    KVick.cpp
    KVickServer.cpp
    KVickStateMachine.cpp
    ClusterManager.cpp
    main.cpp
)

target_include_directories(kvick PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")

target_link_libraries(kvick
    PRIVATE
    Threads::Threads
    kvick_proto
    nuraft
    OpenSSL::SSL
    OpenSSL::Crypto
    ZLIB::ZLIB
)
