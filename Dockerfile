FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    build-essential cmake g++ git pkg-config \
    libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc protobuf-compiler \
    libasio-dev libssl-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN rm -rf build && mkdir build && cd build && cmake .. && make -j$(nproc)

ENV PORT=5000
ENV GRPC_PORT=50051
ENV SEED_NODES=""
ENV ADVERTISE_ADDRESS=""
ENV DATA_DIR=/app/data

EXPOSE $PORT
EXPOSE $GRPC_PORT

CMD ["sh", "-c", "./build/src/kvick --port $PORT --id ${NODE_ID:-$(hostname)} --grpc 0.0.0.0:$GRPC_PORT --advertise ${ADVERTISE_ADDRESS:-$(hostname):$GRPC_PORT} --data-dir $DATA_DIR $( [ -n \"$SEED_NODES\" ] && echo \"--seed $SEED_NODES\" )"]