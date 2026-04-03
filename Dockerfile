FROM ubuntu:24.04

# Install build dependencies + gRPC/Protobuf
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    g++ \
    git \
    libgrpc++-dev \
    protobuf-compiler-grpc \
    libprotobuf-dev \
    protobuf-compiler \
    libssl-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

# Build the project
RUN mkdir build && cd build && cmake .. && make -j$(n_proc)

# Default Environment Variables
ENV PORT=8080
ENV NODE_ID=node1
ENV GRPC_PORT=50051
ENV SEED_NODES=""

# default port
EXPOSE $PORT
EXPOSE $GRPC_PORT

CMD ["sh", "-c", "./build/src/kvick --port $PORT --id $NODE_ID --grpc 0.0.0.0:$GRPC_PORT $( [ -n \"$SEED_NODES\" ] && echo \"--seed $SEED_NODES\" )"]
