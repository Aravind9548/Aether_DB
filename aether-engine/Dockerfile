# Stage 1: Build
FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    pkg-config \
    protobuf-compiler-grpc \
    libgrpc++-dev \
    libprotobuf-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY CMakeLists.txt ./
COPY proto/ ./proto/
COPY include/ ./include/
COPY src/ ./src/
COPY tests/ ./tests/

RUN mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_TSAN=OFF .. && make

# Stage 2: Run
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Runtime shared libs for the gRPC Raft peer transport (not the -dev packages
# needed for the build stage).
RUN apt-get update && apt-get install -y \
    libgrpc++1 \
    libgrpc10 \
    libprotobuf23 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /data

# Copy binary from builder
COPY --from=builder /app/build/AetherDB /app/AetherDB

# Expose port
EXPOSE 8080

# Execute server from /data working directory so persistence files reside on the mounted volume
CMD ["/app/AetherDB", "--shards", "16"]
