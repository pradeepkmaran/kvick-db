FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    build-essential cmake g++ git pkg-config \
    libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc protobuf-compiler \
    libasio-dev libssl-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/eBay/NuRaft.git /nuraft && \
    cd /nuraft && \
    mkdir build && cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local \
             -DASIO_STANDALONE=ON \
             -DCMAKE_BUILD_TYPE=Release && \
    make -j$(nproc) && \
    make install && \
    ldconfig

WORKDIR /app
COPY . /app

RUN rm -rf build && mkdir build && cd build && cmake .. && make -j$(nproc)

ENV PORT=8080
ENV NODE_ID=node1
ENV GRPC_PORT=50051
ENV RAFT_PORT=10051
ENV SEED_NODES=""
ENV ADVERTISE_ADDRESS=""
ENV DATA_DIR=/app/data

EXPOSE $PORT
EXPOSE $GRPC_PORT
EXPOSE $RAFT_PORT

CMD ["sh", "-c", "./build/src/kvick --port $PORT --id $NODE_ID --grpc 0.0.0.0:$GRPC_PORT --advertise ${ADVERTISE_ADDRESS:-$(hostname):$GRPC_PORT} --raft-port $RAFT_PORT --data-dir $DATA_DIR $( [ -n \"$SEED_NODES\" ] && echo \"--seed $SEED_NODES\" )"]