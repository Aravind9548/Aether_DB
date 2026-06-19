cmake_minimum_required(VERSION 3.16)
project(AetherDB LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)

# Add Include Directory
include_directories(include)

# Server executable sources
set(SOURCES
    src/Engine.cpp
    src/Persistence.cpp
    src/Server.cpp
    src/main.cpp
)

# Headers for IDE visibility
set(HEADERS
    include/LockManager.hpp
    include/Persistence.hpp
    include/Engine.hpp
    include/Server.hpp
)

add_executable(AetherDB ${SOURCES} ${HEADERS})

# Thread support
find_package(Threads REQUIRED)
target_link_libraries(AetherDB PRIVATE Threads::Threads)

# TSAN Sanitizer configuration
if(ENABLE_TSAN)
    target_compile_options(AetherDB PRIVATE -fsanitize=thread -g)
    target_link_options(AetherDB PRIVATE -fsanitize=thread)
    message(STATUS "ThreadSanitizer enabled")
endif()
