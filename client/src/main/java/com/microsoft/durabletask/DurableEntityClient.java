// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;

/**
 * Client for interacting with durable entities.
 * <p>
 * This class provides operations for signaling entities, querying entity metadata,
 * and performing entity storage maintenance. Instances are obtained from
 * {@link DurableTaskClient#getEntities()}.
 * <p>
 * This design mirrors the .NET SDK's {@code DurableEntityClient} which is accessed
 * via the {@code DurableTaskClient.Entities} property.
 */
public abstract class DurableEntityClient {

    private final String name;

    /**
     * Creates a new {@code DurableEntityClient} instance.
     *
     * @param name the name of the client
     */
    protected DurableEntityClient(String name) {
        this.name = name;
    }

    /**
     * Gets the name of this client.
     *
     * @return the client name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Sends a signal to a durable entity instance without waiting for a response.
     * <p>
     * If the target entity does not exist, it will be created automatically when it receives the signal.
     *
     * @param entityId      the target entity's instance ID
     * @param operationName the name of the operation to invoke on the entity
     */
    public void signalEntity(EntityInstanceId entityId, String operationName) {
        this.signalEntity(entityId, operationName, null, null);
    }

    /**
     * Sends a signal with input to a durable entity instance without waiting for a response.
     * <p>
     * If the target entity does not exist, it will be created automatically when it receives the signal.
     *
     * @param entityId      the target entity's instance ID
     * @param operationName the name of the operation to invoke on the entity
     * @param input         the serializable input for the operation, or {@code null}
     */
    public void signalEntity(EntityInstanceId entityId, String operationName, @Nullable Object input) {
        this.signalEntity(entityId, operationName, input, null);
    }

    /**
     * Sends a signal with input and options to a durable entity instance without waiting for a response.
     * <p>
     * If the target entity does not exist, it will be created automatically when it receives the signal.
     * Use {@link SignalEntityOptions#setScheduledTime(java.time.Instant)} to schedule the signal for
     * delivery at a future time.
     *
     * @param entityId      the target entity's instance ID
     * @param operationName the name of the operation to invoke on the entity
     * @param input         the serializable input for the operation, or {@code null}
     * @param options       additional options for the signal, or {@code null}
     */
    public abstract void signalEntity(
            EntityInstanceId entityId,
            String operationName,
            @Nullable Object input,
            @Nullable SignalEntityOptions options);

    /**
     * Fetches the metadata for a durable entity instance, including its state by default.
     * <p>
     * This matches the .NET SDK behavior where {@code includeState} defaults to {@code true}.
     *
     * @param entityId the entity instance ID to query
     * @return the entity metadata, or {@code null} if the entity does not exist
     */
    @Nullable
    public EntityMetadata getEntityMetadata(EntityInstanceId entityId) {
        return this.getEntityMetadata(entityId, true);
    }

    /**
     * Fetches the metadata for a durable entity instance, optionally including its state.
     *
     * @param entityId     the entity instance ID to query
     * @param includeState {@code true} to include the entity's serialized state in the result
     * @return the entity metadata, or {@code null} if the entity does not exist
     */
    @Nullable
    public abstract EntityMetadata getEntityMetadata(EntityInstanceId entityId, boolean includeState);

    /**
     * Fetches the metadata for a durable entity instance with typed state access.
     * <p>
     * This always includes state in the result, matching the .NET SDK's
     * {@code GetEntityAsync<T>()} pattern. The returned {@link TypedEntityMetadata} provides
     * a {@link TypedEntityMetadata#getState()} method for direct typed state access.
     *
     * <pre>{@code
     * TypedEntityMetadata<Integer> metadata = client.getEntities()
     *     .getEntityMetadata(entityId, Integer.class);
     * if (metadata != null) {
     *     Integer state = metadata.getState();
     *     System.out.println("Counter value: " + state);
     * }
     * }</pre>
     *
     * @param entityId  the entity instance ID to query
     * @param stateType the class to deserialize the entity's state into
     * @param <T>       the entity state type
     * @return the typed entity metadata with state, or {@code null} if the entity does not exist
     */
    @Nullable
    public <T> TypedEntityMetadata<T> getEntityMetadata(EntityInstanceId entityId, Class<T> stateType) {
        EntityMetadata metadata = this.getEntityMetadata(entityId, true);
        if (metadata == null) {
            return null;
        }
        return new TypedEntityMetadata<>(metadata, stateType);
    }

    /**
     * Queries the durable store for entity instances matching the specified filter criteria.
     *
     * @param query the query filter criteria
     * @return the query result containing matching entities and an optional continuation token
     */
    public abstract EntityQueryResult queryEntities(EntityQuery query);

    /**
     * Returns an auto-paginating iterable over entity instances matching the specified filter criteria.
     * <p>
     * This method automatically handles pagination when iterating over results. It fetches pages
     * from the store on demand, making it convenient when you want to process all matching entities
     * without manually managing continuation tokens.
     * <p>
     * You can iterate over individual items:
     * <pre>{@code
     * for (EntityMetadata entity : client.getEntities().getAllEntities(query)) {
     *     System.out.println(entity.getEntityInstanceId());
     * }
     * }</pre>
     * <p>
     * Or iterate page by page for more control:
     * <pre>{@code
     * for (EntityQueryResult page : client.getEntities().getAllEntities(query).byPage()) {
     *     for (EntityMetadata entity : page.getEntities()) {
     *         System.out.println(entity.getEntityInstanceId());
     *     }
     * }
     * }</pre>
     *
     * @param query the query filter criteria
     * @return a pageable iterable over all matching entities
     */
    public EntityQueryPageable getAllEntities(EntityQuery query) {
        return new EntityQueryPageable(query, this::queryEntities);
    }

    /**
     * Returns an auto-paginating iterable over all entity instances.
     * <p>
     * This is a convenience overload equivalent to {@code getAllEntities(new EntityQuery())}.
     *
     * @return a pageable iterable over all entities
     */
    public EntityQueryPageable getAllEntities() {
        return getAllEntities(new EntityQuery());
    }

    /**
     * Returns an auto-paginating iterable over entity instances matching the specified filter criteria,
     * with typed state access.
     * <p>
     * This mirrors the .NET SDK's {@code GetAllEntitiesAsync<T>()} pattern. Entity state is always
     * included in the results and eagerly deserialized into the specified type. Each item is a
     * {@link TypedEntityMetadata} with a {@link TypedEntityMetadata#getState()} accessor.
     * <p>
     * Note: A copy of the query is made with {@code includeState} set to {@code true} so the
     * original query is not modified.
     *
     * <pre>{@code
     * EntityQuery query = new EntityQuery().setInstanceIdStartsWith("counter");
     * for (TypedEntityMetadata<Integer> entity : client.getEntities().getAllEntities(query, Integer.class)) {
     *     Integer state = entity.getState();
     *     System.out.println("Counter value: " + state);
     * }
     * }</pre>
     *
     * @param query     the query filter criteria
     * @param stateType the class to deserialize each entity's state into
     * @param <T>       the entity state type
     * @return a pageable iterable over all matching entities with typed state
     */
    public <T> TypedEntityQueryPageable<T> getAllEntities(EntityQuery query, Class<T> stateType) {
        // Create a copy with includeState=true so we don't mutate the caller's query
        EntityQuery typedQuery = new EntityQuery()
                .setInstanceIdStartsWith(query.getInstanceIdStartsWith())
                .setLastModifiedFrom(query.getLastModifiedFrom())
                .setLastModifiedTo(query.getLastModifiedTo())
                .setIncludeState(true)
                .setIncludeTransient(query.isIncludeTransient())
                .setPageSize(query.getPageSize())
                .setContinuationToken(query.getContinuationToken());
        EntityQueryPageable inner = new EntityQueryPageable(typedQuery, this::queryEntities);
        return new TypedEntityQueryPageable<>(inner, stateType);
    }

    /**
     * Cleans up entity storage by removing empty entities and/or releasing orphaned locks.
     * <p>
     * This is an administrative operation that can be used to reclaim storage space and fix
     * entity state inconsistencies.
     *
     * @param request the clean storage request specifying what to clean
     * @return the result of the clean operation, including counts of removed entities and released locks
     */
    public abstract CleanEntityStorageResult cleanEntityStorage(CleanEntityStorageRequest request);
}
