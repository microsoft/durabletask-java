// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import io.grpc.Channel;

import java.util.HashMap;

/**
 * Builder object for constructing customized {@link DurableTaskGrpcWorker} instances.
 */
public final class DurableTaskGrpcWorkerBuilder {
    final HashMap<String, TaskOrchestrationFactory> orchestrationFactories = new HashMap<>();
    final HashMap<String, TaskActivityFactory> activityFactories = new HashMap<>();
    int port;
    Channel channel;
    DataConverter dataConverter;

    /**
     * Adds an orchestration factory to be used by the constructed {@link DurableTaskGrpcWorker}.
     *
     * @param factory an orchestration factory to be used by the constructed {@link DurableTaskGrpcWorker}
     * @return this builder object
     */
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

    /**
     * Adds an activity factory to be used by the constructed {@link DurableTaskGrpcWorker}.
     *
     * @param factory an activity factory to be used by the constructed {@link DurableTaskGrpcWorker}
     * @return this builder object
     */
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

    /**
     * Sets the gRPC channel to use for communicating with the sidecar process.
     * <p>
     * This builder method allows you to provide your own gRPC channel for communicating with the Durable Task sidecar
     * endpoint. Channels provided using this method won't be closed when the worker is closed.
     * Rather, the caller remains responsible for shutting down the channel after disposing the worker.
     * <p>
     * If not specified, a gRPC channel will be created automatically for each constructed
     * {@link DurableTaskGrpcWorker}.
     *
     * @param channel the gRPC channel to use
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder grpcChannel(Channel channel) {
        this.channel = channel;
        return this;
    }

    /**
     * Sets the gRPC endpoint port to connect to. If not specified, the default Durable Task port number will be used.
     *
     * @param port the gRPC endpoint port to connect to
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder port(int port) {
        this.port = port;
        return this;
    }

    /**
     * Sets the {@link DataConverter} to use for converting serializable data payloads.
     *
     * @param dataConverter the {@link DataConverter} to use for converting serializable data payloads
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder dataConverter(DataConverter dataConverter) {
        this.dataConverter = dataConverter;
        return this;
    }

    /**
     * Initializes a new {@link DurableTaskGrpcWorker} object with the settings specified in the current builder object.
     * @return a new {@link DurableTaskGrpcWorker} object
     */
    public DurableTaskGrpcWorker build() {
        return new DurableTaskGrpcWorker(this);
    }
}
