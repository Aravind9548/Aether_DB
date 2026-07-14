cmake_minimum_required(VERSION 3.16)
project(AetherDB LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)

# Add Include Directory
include_directories(include)

# --- gRPC / Protobuf (Raft peer RPC transport) ---
# Debian/Ubuntu's libgrpc++-dev package is built for pkg-config discovery, not
# necessarily upstream-style `find_package(gRPC CONFIG)`. Try the upstream
# config packages first and fall back to pkg-config (via IMPORTED_TARGET,
# which gives us plain `PkgConfig::<NAME>` targets to link against either way).
find_package(Protobuf CONFIG QUIET)
find_package(gRPC CONFIG QUIET)

if(TARGET protobuf::libprotobuf AND TARGET gRPC::grpc++)
    set(AETHER_PROTOBUF_LIB protobuf::libprotobuf)
    set(AETHER_GRPCXX_LIB gRPC::grpc++)
    message(STATUS "AetherDB: using upstream CMake config packages for Protobuf/gRPC")
else()
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(PROTOBUF REQUIRED IMPORTED_TARGET protobuf)
    pkg_check_modules(GRPCXX REQUIRED IMPORTED_TARGET grpc++)
    set(AETHER_PROTOBUF_LIB PkgConfig::PROTOBUF)
    set(AETHER_GRPCXX_LIB PkgConfig::GRPCXX)
    message(STATUS "AetherDB: using pkg-config for Protobuf/gRPC")
endif()

find_program(PROTOC_EXECUTABLE protoc)
if(NOT PROTOC_EXECUTABLE)
    message(FATAL_ERROR "protoc not found - install protobuf-compiler")
endif()
find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
if(NOT GRPC_CPP_PLUGIN_EXECUTABLE)
    message(FATAL_ERROR "grpc_cpp_plugin not found - install protobuf-compiler-grpc")
endif()

set(PROTO_GEN_DIR ${CMAKE_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

# Raft peer RPCs (Phase A)
set(RAFT_PROTO_FILE ${CMAKE_SOURCE_DIR}/proto/raft.proto)
set(RAFT_PROTO_SRCS ${PROTO_GEN_DIR}/raft.pb.cc)
set(RAFT_PROTO_HDRS ${PROTO_GEN_DIR}/raft.pb.h)
set(RAFT_GRPC_SRCS ${PROTO_GEN_DIR}/raft.grpc.pb.cc)
set(RAFT_GRPC_HDRS ${PROTO_GEN_DIR}/raft.grpc.pb.h)

add_custom_command(
    OUTPUT ${RAFT_PROTO_SRCS} ${RAFT_PROTO_HDRS} ${RAFT_GRPC_SRCS} ${RAFT_GRPC_HDRS}
    COMMAND ${PROTOC_EXECUTABLE}
    ARGS --cpp_out=${PROTO_GEN_DIR}
         --grpc_out=${PROTO_GEN_DIR}
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
         -I ${CMAKE_SOURCE_DIR}/proto
         ${RAFT_PROTO_FILE}
    DEPENDS ${RAFT_PROTO_FILE}
    COMMENT "Generating gRPC/protobuf code from raft.proto"
)

# Client data-path RPCs (Phase B)
set(CLIENT_PROTO_FILE ${CMAKE_SOURCE_DIR}/proto/client.proto)
set(CLIENT_PROTO_SRCS ${PROTO_GEN_DIR}/client.pb.cc)
set(CLIENT_PROTO_HDRS ${PROTO_GEN_DIR}/client.pb.h)
set(CLIENT_GRPC_SRCS ${PROTO_GEN_DIR}/client.grpc.pb.cc)
set(CLIENT_GRPC_HDRS ${PROTO_GEN_DIR}/client.grpc.pb.h)

add_custom_command(
    OUTPUT ${CLIENT_PROTO_SRCS} ${CLIENT_PROTO_HDRS} ${CLIENT_GRPC_SRCS} ${CLIENT_GRPC_HDRS}
    COMMAND ${PROTOC_EXECUTABLE}
    ARGS --cpp_out=${PROTO_GEN_DIR}
         --grpc_out=${PROTO_GEN_DIR}
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
         -I ${CMAKE_SOURCE_DIR}/proto
         ${CLIENT_PROTO_FILE}
    DEPENDS ${CLIENT_PROTO_FILE}
    COMMENT "Generating gRPC/protobuf code from client.proto"
)

# Server executable sources
set(SOURCES
    src/Engine.cpp
    src/Persistence.cpp
    src/Server.cpp
    src/Raft.cpp
    src/main.cpp
    ${RAFT_PROTO_SRCS}
    ${RAFT_GRPC_SRCS}
    ${CLIENT_PROTO_SRCS}
    ${CLIENT_GRPC_SRCS}
)

# Headers for IDE visibility
set(HEADERS
    include/LockManager.hpp
    include/Persistence.hpp
    include/Engine.hpp
    include/Server.hpp
    include/Raft.hpp
    ${RAFT_PROTO_HDRS}
    ${RAFT_GRPC_HDRS}
    ${CLIENT_PROTO_HDRS}
    ${CLIENT_GRPC_HDRS}
)

add_executable(AetherDB ${SOURCES} ${HEADERS})
target_include_directories(AetherDB PRIVATE ${PROTO_GEN_DIR})
target_link_libraries(AetherDB PRIVATE ${AETHER_PROTOBUF_LIB} ${AETHER_GRPCXX_LIB})

# Thread support
find_package(Threads REQUIRED)
target_link_libraries(AetherDB PRIVATE Threads::Threads)

# Unit tests
add_executable(wal_v2_test tests/wal_v2_test.cpp src/Persistence.cpp)
target_link_libraries(wal_v2_test PRIVATE Threads::Threads)

# Client gRPC smoke test (Phase B) - pure client, no engine/Raft deps needed
add_executable(client_grpc_test tests/client_grpc_test.cpp ${CLIENT_PROTO_SRCS} ${CLIENT_GRPC_SRCS})
target_include_directories(client_grpc_test PRIVATE ${PROTO_GEN_DIR})
target_link_libraries(client_grpc_test PRIVATE ${AETHER_PROTOBUF_LIB} ${AETHER_GRPCXX_LIB} Threads::Threads)

# TSAN Sanitizer configuration
if(ENABLE_TSAN)
    target_compile_options(AetherDB PRIVATE -fsanitize=thread -g)
    target_link_options(AetherDB PRIVATE -fsanitize=thread)
    message(STATUS "ThreadSanitizer enabled")
endif()
