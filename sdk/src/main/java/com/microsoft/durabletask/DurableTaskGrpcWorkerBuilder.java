package com.microsoft.durabletask;

import io.grpc.Channel;

import java.util.HashMap;

public final class DurableTaskGrpcWorkerBuilder {
    final HashMap<String, TaskOrchestrationFactory> orchestrationFactories = new HashMap<>();
    final HashMap<String, TaskActivityFactory> activityFactories = new HashMap<>();
    int port;
    Channel channel;
    DataConverter dataConverter;

    public DurableTaskGrpcWorkerBuilder addOrchestration(TaskOrchestrationFactory factory) {
        String key = factory.getName();
        if (key == null || key.length() == 0) {
            throw new IllegalArgumentException("A non-empty task orchestration name is required.");
        }

        if (this.orchestrationFactories.containsKey(key)) {
            throw new IllegalArgumentException(
                    String.format("A task orchestration factory named %s is already registered.", key));
        }

        this.orchestrationFactories.put(key, factory);
        return this;
    }

    public DurableTaskGrpcWorkerBuilder addActivity(TaskActivityFactory factory) {
        // TODO: Input validation
        String key = factory.getName();
        if (key == null || key.length() == 0) {
            throw new IllegalArgumentException("A non-empty task activity name is required.");
        }

        if (this.activityFactories.containsKey(key)) {
            throw new IllegalArgumentException(
                    String.format("A task activity factory named %s is already registered.", key));
        }

        this.activityFactories.put(key, factory);
        return this;
    }

    public DurableTaskGrpcWorkerBuilder useGrpcChannel(Channel channel) {
        this.channel = channel;
        return this;
    }

    public DurableTaskGrpcWorkerBuilder forPort(int port) {
        this.port = port;
        return this;
    }

    public DurableTaskGrpcWorkerBuilder setDataConverter(DataConverter dataConverter) {
        this.dataConverter = dataConverter;
        return this;
    }

    public DurableTaskGrpcWorker build() {
        return new DurableTaskGrpcWorker(this);
    }
}
