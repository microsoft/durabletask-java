// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import io.grpc.Channel;
import io.grpc.ClientInterceptor;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

/**
 * Builder object for constructing customized {@link DurableTaskGrpcWorker} instances.
 */
public final class DurableTaskGrpcWorkerBuilder {
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
    private WorkItemFilter workItemFilter;
    private boolean autoGenerateWorkItemFilters;
    final List<ClientInterceptor> interceptors = new ArrayList<>();
    boolean supportsLargePayloads;
    int maxChunkSizeBytes = 4_089_446; // 3.9 MB default, same as .NET

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
     * Sets explicit work item filters for this worker. When set, only work items matching the filters
     * will be dispatched to this worker by the backend.
     * <p>
     * Work item filtering can improve efficiency in multi-worker deployments by ensuring each worker
     * only receives work items it can handle. However, if an orchestration calls a task type
     * (e.g., an activity or sub-orchestrator) that is not registered with any connected worker,
     * the call may hang indefinitely instead of failing with an error.
     *
     * @param workItemFilter the work item filter to use, or {@code null} to disable filtering
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder useWorkItemFilters(WorkItemFilter workItemFilter) {
        this.workItemFilter = workItemFilter;
        this.autoGenerateWorkItemFilters = false;
        return this;
    }

    /**
     * Enables automatic work item filtering by generating filters from the registered
     * orchestrations and activities. When enabled, the backend will only dispatch work items
     * for registered orchestrations and activities to this worker.
     * <p>
     * Work item filtering can improve efficiency in multi-worker deployments by ensuring each worker
     * only receives work items it can handle. However, if an orchestration calls a task type
     * (e.g., an activity or sub-orchestrator) that is not registered with any connected worker,
     * the call may hang indefinitely instead of failing with an error.
     * <p>
     * Only use this method when all task types referenced by orchestrations are guaranteed to be
     * registered with at least one connected worker.
     *
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder useWorkItemFilters() {
        this.autoGenerateWorkItemFilters = true;
        this.workItemFilter = null;
        return this;
    }

    /**
     * Adds a gRPC {@link ClientInterceptor} that will be applied to the channel used by the constructed worker.
     * <p>
     * Interceptors are applied in the order they are added. This is the extension point used by features
     * such as large payload externalization to transparently transform gRPC messages.
     *
     * @param interceptor the interceptor to add
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder addInterceptor(ClientInterceptor interceptor) {
        if (interceptor == null) {
            throw new IllegalArgumentException("interceptor must not be null.");
        }
        this.interceptors.add(interceptor);
        return this;
    }

    /**
     * Indicates that this worker supports large payload externalization.
     * <p>
     * When enabled, the worker announces the {@code WORKER_CAPABILITY_LARGE_PAYLOADS} capability
     * to the sidecar and skips the pre-send action size validation (since the gRPC interceptor
     * will externalize oversized payloads before they hit the wire).
     *
     * @param enabled whether large payload support is enabled
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder setSupportsLargePayloads(boolean enabled) {
        this.supportsLargePayloads = enabled;
        return this;
    }

    /**
     * Sets the maximum size in bytes for each chunk when sending orchestrator responses.
     * <p>
     * If an orchestrator response exceeds this size, it will be automatically split into
     * multiple chunks. The default is 3.9 MB ({@code 4_089_446} bytes), matching the .NET SDK.
     * Must be between 1 MB and 3.9 MB inclusive.
     *
     * @param maxChunkSizeBytes the maximum chunk size in bytes
     * @return this builder object
     * @throws IllegalArgumentException if the value is outside the allowed range
     */
    public DurableTaskGrpcWorkerBuilder setMaxChunkSizeBytes(int maxChunkSizeBytes) {
        if (maxChunkSizeBytes < 1_048_576 || maxChunkSizeBytes > 4_089_446) {
            throw new IllegalArgumentException(
                "maxChunkSizeBytes must be between 1 MB (1048576) and 3.9 MB (4089446), inclusive.");
        }
        this.maxChunkSizeBytes = maxChunkSizeBytes;
        return this;
    }

    /**
     * Initializes a new {@link DurableTaskGrpcWorker} object with the settings specified in the current builder object.
     * @return a new {@link DurableTaskGrpcWorker} object
     */
    public DurableTaskGrpcWorker build() {
        WorkItemFilter resolvedFilter = this.autoGenerateWorkItemFilters
                ? buildAutoWorkItemFilter()
                : this.workItemFilter;
        return new DurableTaskGrpcWorker(this, resolvedFilter);
    }

    private WorkItemFilter buildAutoWorkItemFilter() {
        List<String> versions = Collections.emptyList();
        if (this.versioningOptions != null
                && this.versioningOptions.getMatchStrategy() == DurableTaskGrpcWorkerVersioningOptions.VersionMatchStrategy.STRICT
                && this.versioningOptions.getVersion() != null) {
            versions = Collections.singletonList(this.versioningOptions.getVersion());
        }

        WorkItemFilter.Builder builder = WorkItemFilter.newBuilder();
        List<String> orchestrationNames = new ArrayList<>(this.orchestrationFactories.keySet());
        Collections.sort(orchestrationNames);
        for (String name : orchestrationNames) {
            builder.addOrchestration(name, versions);
        }
        List<String> activityNames = new ArrayList<>(this.activityFactories.keySet());
        Collections.sort(activityNames);
        for (String name : activityNames) {
            builder.addActivity(name, versions);
        }
        return builder.build();
    }
}
