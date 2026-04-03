FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    build-essential cmake g++ git \
    libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc \
    libasio-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/eBay/NuRaft.git /nuraft && \
    cd /nuraft && \
    mkdir build && cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local -DASIO_STANDALONE=ON && \
    make -j$(nproc) && \
    make install && \
    ldconfig

WORKDIR /app
COPY . /app

# Build the project
RUN rm -rf build && mkdir build && cd build && cmake .. && make -j$(nproc)

ENV PORT=8080
ENV NODE_ID=node1
ENV GRPC_PORT=50051
ENV SEED_NODES=""

EXPOSE $PORT
EXPOSE $GRPC_PORT

CMD ["sh", "-c", "./build/src/kvick --port $PORT --id $NODE_ID --grpc 0.0.0.0:$GRPC_PORT $( [ -n \"$SEED_NODES\" ] && echo \"--seed $SEED_NODES\" )"]