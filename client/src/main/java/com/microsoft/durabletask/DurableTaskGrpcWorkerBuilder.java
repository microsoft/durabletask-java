// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import io.grpc.Channel;

import java.time.Duration;
import java.util.HashMap;

/**
 * Builder object for constructing customized {@link DurableTaskGrpcWorker} instances.
 */
public final class DurableTaskGrpcWorkerBuilder {

    /**
     * Minimum allowed chunk size for orchestrator response messages (1 MiB).
     */
    static final int MIN_CHUNK_SIZE_BYTES = 1_048_576;

    /**
     * Maximum allowed chunk size for orchestrator response messages (~3.9 MiB).
     * This is the largest payload that can fit within a 4 MiB gRPC message after accounting
     * for protobuf overhead.
     */
    static final int MAX_CHUNK_SIZE_BYTES = 4_089_446;

    /**
     * Default chunk size for orchestrator response messages.
     * Matches {@link #MAX_CHUNK_SIZE_BYTES}.
     */
    static final int DEFAULT_CHUNK_SIZE_BYTES = MAX_CHUNK_SIZE_BYTES;

    final HashMap<String, TaskOrchestrationFactory> orchestrationFactories = new HashMap<>();
    final HashMap<String, TaskActivityFactory> activityFactories = new HashMap<>();
    int port;
    Channel channel;
    DataConverter dataConverter;
    Duration maximumTimerInterval;
    DurableTaskGrpcWorkerVersioningOptions versioningOptions;
    PayloadStore payloadStore;
    LargePayloadOptions largePayloadOptions;
    int chunkSizeBytes = DEFAULT_CHUNK_SIZE_BYTES;

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
     * Sets the maximum timer interval. If not specified, the default maximum timer interval duration will be used.
     * The default maximum timer interval duration is 3 days.
     *
     * @param maximumTimerInterval the maximum timer interval
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder maximumTimerInterval(Duration maximumTimerInterval) {
        this.maximumTimerInterval = maximumTimerInterval;
        return this;
    }

    /**
     * Sets the versioning options for this worker.
     * 
     * @param options the versioning options to use
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder useVersioning(DurableTaskGrpcWorkerVersioningOptions options) {
        this.versioningOptions = options;
        return this;
    }

    /**
     * Enables large payload externalization with default options.
     * <p>
     * When enabled, payloads exceeding the default threshold will be uploaded to the
     * provided {@link PayloadStore} and replaced with opaque token references. The worker
     * will also announce {@code WORKER_CAPABILITY_LARGE_PAYLOADS} to the sidecar.
     *
     * @param payloadStore the store to use for externalizing large payloads
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder useExternalizedPayloads(PayloadStore payloadStore) {
        return this.useExternalizedPayloads(payloadStore, new LargePayloadOptions.Builder().build());
    }

    /**
     * Enables large payload externalization with custom options.
     * <p>
     * When enabled, payloads exceeding the configured threshold will be uploaded to the
     * provided {@link PayloadStore} and replaced with opaque token references. The worker
     * will also announce {@code WORKER_CAPABILITY_LARGE_PAYLOADS} to the sidecar.
     *
     * @param payloadStore the store to use for externalizing large payloads
     * @param options the large payload configuration options
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder useExternalizedPayloads(PayloadStore payloadStore, LargePayloadOptions options) {
        if (payloadStore == null) {
            throw new IllegalArgumentException("payloadStore must not be null");
        }
        if (options == null) {
            throw new IllegalArgumentException("options must not be null");
        }
        this.payloadStore = payloadStore;
        this.largePayloadOptions = options;
        return this;
    }

    /**
     * Sets the maximum size in bytes for a single orchestrator response chunk sent over gRPC.
     * Responses larger than this will be automatically split into multiple chunks.
     * <p>
     * The value must be between {@value #MIN_CHUNK_SIZE_BYTES} and {@value #MAX_CHUNK_SIZE_BYTES} bytes.
     * Defaults to {@value #DEFAULT_CHUNK_SIZE_BYTES} bytes.
     *
     * @param chunkSizeBytes the maximum chunk size in bytes
     * @return this builder object
     * @throws IllegalArgumentException if the value is outside the allowed range
     */
    public DurableTaskGrpcWorkerBuilder setCompleteOrchestratorResponseChunkSizeBytes(int chunkSizeBytes) {
        if (chunkSizeBytes < MIN_CHUNK_SIZE_BYTES || chunkSizeBytes > MAX_CHUNK_SIZE_BYTES) {
            throw new IllegalArgumentException(String.format(
                "chunkSizeBytes must be between %d and %d, but was %d",
                MIN_CHUNK_SIZE_BYTES, MAX_CHUNK_SIZE_BYTES, chunkSizeBytes));
        }
        this.chunkSizeBytes = chunkSizeBytes;
        return this;
    }

    /**
     * Gets the current chunk size setting.
     *
     * @return the chunk size in bytes
     */
    int getChunkSizeBytes() {
        return this.chunkSizeBytes;
    }

    /**
     * Initializes a new {@link DurableTaskGrpcWorker} object with the settings specified in the current builder object.
     * @return a new {@link DurableTaskGrpcWorker} object
     */
    public DurableTaskGrpcWorker build() {
        return new DurableTaskGrpcWorker(this);
    }
}
