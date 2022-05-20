package com.microsoft.durabletask;

import io.grpc.Channel;

public final class DurableTaskGrpcClientBuilder {
    DataConverter dataConverter;
    int port;
    Channel channel;

    public DurableTaskGrpcClientBuilder dataConverter(DataConverter dataConverter) {
        this.dataConverter = dataConverter;
        return this;
    }

    public DurableTaskGrpcClientBuilder grpcChannel(Channel channel) {
        this.channel = channel;
        return this;
    }

    public DurableTaskGrpcClientBuilder forPort(int port) {
        this.port = port;
        return this;
    }

    public DurableTaskClient build() {
        return new DurableTaskGrpcClient(this);
    }
}
