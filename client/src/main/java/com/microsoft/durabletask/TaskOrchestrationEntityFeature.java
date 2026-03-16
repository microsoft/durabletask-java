// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Feature for interacting with durable entities from an orchestration.
 * <p>
 * This mirrors the .NET SDK's {@code TaskOrchestrationContext.Entities} shape, adapted to Java.
 */
public abstract class TaskOrchestrationEntityFeature {

    /**
     * Calls an operation on an entity and waits for it to complete.
     *
     * @param entityId the target entity
     * @param operationName the name of the operation
     * @param input the operation input, or {@code null}
     * @param returnType the expected return type
     * @param <TResult> the result type
     * @return a task that completes with the operation result
     */
    public abstract <TResult> Task<TResult> callEntity(
            @Nonnull EntityInstanceId entityId,
            @Nonnull String operationName,
            @Nullable Object input,
            @Nonnull Class<TResult> returnType);

    /**
     * Calls an operation on an entity and waits for it to complete, with options.
     *
     * @param entityId the target entity
     * @param operationName the name of the operation
     * @param input the operation input, or {@code null}
     * @param returnType the expected return type
     * @param options the call options, or {@code null}
     * @param returnType the expected class type of the entity operation output
     * @param <TResult> the result type
     * @return a task that completes with the operation result
     */
    public abstract <TResult> Task<TResult> callEntity(
            @Nonnull EntityInstanceId entityId,
            @Nonnull String operationName,
            @Nullable Object input,
            @Nonnull Class<TResult> returnType,
            @Nullable CallEntityOptions options);

    /**
     * Calls an operation on an entity and waits for it to complete.
     *
     * @param entityId the target entity
     * @param operationName the name of the operation
     * @param <TResult> the result type
     * @return a task that completes with the operation result
     */
    public <TResult> Task<TResult> callEntity(
            @Nonnull EntityInstanceId entityId,
            @Nonnull String operationName,
            @Nonnull Class<TResult> returnType) {
        return this.callEntity(entityId, operationName, null, returnType, null);
    }

    /**
     * Signals an entity operation without waiting for completion.
     *
     * @param entityId the target entity
     * @param operationName the operation name
     * @param input the operation input, or {@code null}
     * @param options signal options, or {@code null}
     */
    public abstract void signalEntity(
            @Nonnull EntityInstanceId entityId,
            @Nonnull String operationName,
            @Nullable Object input,
            @Nullable SignalEntityOptions options);

    /**
     * Signals an entity operation without waiting for completion.
     *
     * @param entityId the target entity
     * @param operationName the operation name
     */
    public void signalEntity(@Nonnull EntityInstanceId entityId, @Nonnull String operationName) {
        this.signalEntity(entityId, operationName, null, null);
    }

    /**
     * Signals an entity operation without waiting for completion.
     *
     * @param entityId the target entity
     * @param operationName the operation name
     * @param input the operation input, or {@code null}
     */
    public void signalEntity(
            @Nonnull EntityInstanceId entityId,
            @Nonnull String operationName,
            @Nullable Object input) {
        this.signalEntity(entityId, operationName, input, null);
    }

    /**
     * Acquires one or more entity locks.
     *
     * @param entityIds the entity IDs to lock
     * @return a task that completes with an {@link AutoCloseable} used to release locks
     */
    public abstract Task<AutoCloseable> lockEntities(@Nonnull List<EntityInstanceId> entityIds);

    /**
     * Acquires one or more entity locks.
     *
     * @param entityIds the entity IDs to lock
     * @return a task that completes with an {@link AutoCloseable} used to release locks
     */
    public abstract Task<AutoCloseable> lockEntities(@Nonnull EntityInstanceId... entityIds);

    /**
     * Gets whether this orchestration is currently inside a critical section.
     *
     * @return {@code true} if inside a critical section, otherwise {@code false}
     */
    public abstract boolean isInCriticalSection();

    /**
     * Gets the currently locked entity IDs.
     *
     * @return the list of currently locked entities, or an empty list
     */
    public abstract List<EntityInstanceId> getLockedEntities();
}

final class ContextBackedTaskOrchestrationEntityFeature extends TaskOrchestrationEntityFeature {
    private final TaskOrchestrationContext context;

    ContextBackedTaskOrchestrationEntityFeature(TaskOrchestrationContext context) {
        this.context = context;
    }

    @Override
    public <TResult> Task<TResult> callEntity(
            @Nonnull EntityInstanceId entityId,
            @Nonnull String operationName,
            @Nullable Object input,
            @Nonnull Class<TResult> returnType) {
        return this.context.callEntity(entityId, operationName, input, returnType);
    }

    @Override
    public <TResult> Task<TResult> callEntity(
            @Nonnull EntityInstanceId entityId,
            @Nonnull String operationName,
            @Nullable Object input,
            @Nonnull Class<TResult> returnType,
            @Nullable CallEntityOptions options) {
        return this.context.callEntity(entityId, operationName, input, returnType, options);
    }

    @Override
    public void signalEntity(
            @Nonnull EntityInstanceId entityId,
            @Nonnull String operationName,
            @Nullable Object input,
            @Nullable SignalEntityOptions options) {
        this.context.signalEntity(entityId, operationName, input, options);
    }

    @Override
    public Task<AutoCloseable> lockEntities(@Nonnull List<EntityInstanceId> entityIds) {
        return this.context.lockEntities(entityIds);
    }

    @Override
    public Task<AutoCloseable> lockEntities(@Nonnull EntityInstanceId... entityIds) {
        return this.context.lockEntities(entityIds);
    }

    @Override
    public boolean isInCriticalSection() {
        return this.context.isInCriticalSection();
    }

    @Override
    public List<EntityInstanceId> getLockedEntities() {
        return this.context.getLockedEntities();
    }
}