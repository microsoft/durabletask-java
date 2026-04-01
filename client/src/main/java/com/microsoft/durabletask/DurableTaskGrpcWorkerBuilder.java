// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import io.grpc.Channel;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;

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
    final HashMap<String, TaskEntityFactory> entityFactories = new HashMap<>();
    int port;
    Channel channel;
    DataConverter dataConverter;
    Duration maximumTimerInterval;
    DurableTaskGrpcWorkerVersioningOptions versioningOptions;
    int maxConcurrentEntityWorkItems = 1;
    int maxWorkItemThreads;
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
     * Adds an entity factory to be used by the constructed {@link DurableTaskGrpcWorker}.
     *
     * @param name    the name of the entity type
     * @param factory the factory that creates instances of the entity
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder addEntity(String name, TaskEntityFactory factory) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("A non-empty entity name is required.");
        }
        if (factory == null) {
            throw new IllegalArgumentException("An entity factory is required.");
        }

        String key = name.toLowerCase(Locale.ROOT);
        if (this.entityFactories.containsKey(key)) {
            throw new IllegalArgumentException(
                    String.format("An entity factory named %s is already registered.", name));
        }

        this.entityFactories.put(key, factory);
        return this;
    }

    /**
     * Registers an entity type for the constructed {@link DurableTaskGrpcWorker}.
     * <p>
     * The entity class must implement {@link TaskEntity} and have a public no-argument constructor.
     * A new instance of the entity is created for each operation batch using reflection.
     * <p>
     * The entity name is derived from the simple class name of the provided type.
     *
     * @param entityClass the entity class to register; must implement {@link TaskEntity}
     * @return this builder object
     * @throws IllegalArgumentException if the class does not implement {@link TaskEntity}
     */
    public DurableTaskGrpcWorkerBuilder addEntity(Class<? extends TaskEntity> entityClass) {
        if (entityClass == null) {
            throw new IllegalArgumentException("entityClass must not be null.");
        }
        String name = entityClass.getSimpleName();
        return this.addEntity(name, entityClass);
    }

    /**
     * Registers an entity type with a specific name for the constructed {@link DurableTaskGrpcWorker}.
     * <p>
     * The entity class must implement {@link TaskEntity} and have a public no-argument constructor.
     * A new instance of the entity is created for each operation batch using reflection.
     *
     * @param name        the name of the entity type
     * @param entityClass the entity class to register; must implement {@link TaskEntity}
     * @return this builder object
     * @throws IllegalArgumentException if the class does not implement {@link TaskEntity}
     */
    public DurableTaskGrpcWorkerBuilder addEntity(String name, Class<? extends TaskEntity> entityClass) {
        if (entityClass == null) {
            throw new IllegalArgumentException("entityClass must not be null.");
        }
        if (!TaskEntity.class.isAssignableFrom(entityClass)) {
            throw new IllegalArgumentException(
                    String.format("Type %s does not implement TaskEntity.", entityClass.getName()));
        }
        return this.addEntity(name, () -> {
            try {
                return entityClass.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new RuntimeException(
                        String.format("Failed to create instance of entity type %s. Ensure it has a public no-argument constructor.", entityClass.getName()), e);
            }
        });
    }

    /**
     * Registers an entity singleton for the constructed {@link DurableTaskGrpcWorker}.
     * <p>
     * The same entity instance is reused for every operation batch. This is useful for stateless entities
     * or entities that manage their own lifecycle.
     * <p>
     * The entity name is derived from the simple class name of the provided entity instance.
     * <p>
     * <b>Thread safety warning:</b> Because the same instance handles all operation batches,
     * the entity implementation must be thread-safe if concurrent entity work items are enabled.
     * Implementations that extend {@link AbstractTaskEntity} store mutable state in instance fields and
     * are <b>not</b> safe for singleton registration. Use {@link #addEntity(Class)} or
     * {@link #addEntity(String, Class)} instead to create a new instance per batch.
     *
     * @param entity the entity instance to register
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder addEntity(TaskEntity entity) {
        if (entity == null) {
            throw new IllegalArgumentException("entity must not be null.");
        }
        String name = entity.getClass().getSimpleName();
        return this.addEntity(name, () -> entity);
    }

    /**
     * Registers an entity singleton with a specific name for the constructed {@link DurableTaskGrpcWorker}.
     * <p>
     * The same entity instance is reused for every operation batch.
     * <p>
     * <b>Thread safety warning:</b> Because the same instance handles all operation batches,
     * the entity implementation must be thread-safe if concurrent entity work items are enabled.
     * Implementations that extend {@link AbstractTaskEntity} store mutable state in instance fields and
     * are <b>not</b> safe for singleton registration. Use {@link #addEntity(String, Class)} instead
     * to create a new instance per batch.
     *
     * @param name   the name of the entity type
     * @param entity the entity instance to register
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder addEntity(String name, TaskEntity entity) {
        if (entity == null) {
            throw new IllegalArgumentException("entity must not be null.");
        }
        return this.addEntity(name, () -> entity);
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
     * Sets the maximum number of entity work items that can be processed concurrently by this worker.
     * <p>
     * Each entity instance is always single-threaded (serial execution), but this setting controls
     * how many different entity instances can process work items in parallel. The default value is 1.
     *
     * @param maxConcurrentEntityWorkItems the maximum number of concurrent entity work items (must be at least 1)
     * @return this builder object
     * @throws IllegalArgumentException if the value is less than 1
     */
    public DurableTaskGrpcWorkerBuilder maxConcurrentEntityWorkItems(int maxConcurrentEntityWorkItems) {
        if (maxConcurrentEntityWorkItems < 1) {
            throw new IllegalArgumentException("maxConcurrentEntityWorkItems must be at least 1.");
        }
        this.maxConcurrentEntityWorkItems = maxConcurrentEntityWorkItems;
        return this;
    }

    /**
     * Sets the maximum number of threads used for processing entity work items.
     * <p>
     * The default value is {@value DurableTaskGrpcWorker#DEFAULT_MAX_WORK_ITEM_THREADS}.
     * Threads are created on demand and idle threads are reclaimed after 60 seconds.
     *
     * @param maxWorkItemThreads the maximum number of work item threads (must be at least 1)
     * @return this builder object
     * @throws IllegalArgumentException if the value is less than 1
     */
    public DurableTaskGrpcWorkerBuilder maxWorkItemThreads(int maxWorkItemThreads) {
        if (maxWorkItemThreads < 1) {
            throw new IllegalArgumentException("maxWorkItemThreads must be at least 1.");
        }
        this.maxWorkItemThreads = maxWorkItemThreads;
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
