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
    final HashMap<String, TaskOrchestrationFactory> orchestrationFactories = new HashMap<>();
    final HashMap<String, TaskActivityFactory> activityFactories = new HashMap<>();
    final HashMap<String, TaskEntityFactory> entityFactories = new HashMap<>();
    int port;
    Channel channel;
    DataConverter dataConverter;
    Duration maximumTimerInterval;
    DurableTaskGrpcWorkerVersioningOptions versioningOptions;
    int maxConcurrentEntityWorkItems = 1;

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
     * The entity class must implement {@link ITaskEntity} and have a public no-argument constructor.
     * A new instance of the entity is created for each operation batch using reflection.
     * <p>
     * The entity name is derived from the simple class name of the provided type.
     *
     * @param entityClass the entity class to register; must implement {@link ITaskEntity}
     * @return this builder object
     * @throws IllegalArgumentException if the class does not implement {@link ITaskEntity}
     */
    public DurableTaskGrpcWorkerBuilder addEntity(Class<? extends ITaskEntity> entityClass) {
        if (entityClass == null) {
            throw new IllegalArgumentException("entityClass must not be null.");
        }
        String name = entityClass.getSimpleName();
        return this.addEntity(name, entityClass);
    }

    /**
     * Registers an entity type with a specific name for the constructed {@link DurableTaskGrpcWorker}.
     * <p>
     * The entity class must implement {@link ITaskEntity} and have a public no-argument constructor.
     * A new instance of the entity is created for each operation batch using reflection.
     *
     * @param name        the name of the entity type
     * @param entityClass the entity class to register; must implement {@link ITaskEntity}
     * @return this builder object
     * @throws IllegalArgumentException if the class does not implement {@link ITaskEntity}
     */
    public DurableTaskGrpcWorkerBuilder addEntity(String name, Class<? extends ITaskEntity> entityClass) {
        if (entityClass == null) {
            throw new IllegalArgumentException("entityClass must not be null.");
        }
        if (!ITaskEntity.class.isAssignableFrom(entityClass)) {
            throw new IllegalArgumentException(
                    String.format("Type %s does not implement ITaskEntity.", entityClass.getName()));
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
     *
     * @param entity the entity instance to register
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder addEntity(ITaskEntity entity) {
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
     *
     * @param name   the name of the entity type
     * @param entity the entity instance to register
     * @return this builder object
     */
    public DurableTaskGrpcWorkerBuilder addEntity(String name, ITaskEntity entity) {
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
     * Initializes a new {@link DurableTaskGrpcWorker} object with the settings specified in the current builder object.
     * @return a new {@link DurableTaskGrpcWorker} object
     */
    public DurableTaskGrpcWorker build() {
        return new DurableTaskGrpcWorker(this);
    }
}
