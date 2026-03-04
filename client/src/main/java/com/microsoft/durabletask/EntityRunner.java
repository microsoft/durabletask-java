// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.InvalidProtocolBufferException;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.EntityBatchRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.EntityBatchResult;

import java.util.Base64;
import java.util.HashMap;
import java.util.logging.Logger;

/**
 * Helper class for invoking entity operations directly, without constructing a {@link DurableTaskGrpcWorker} object.
 * <p>
 * This static class can be used to execute entity logic directly. In order to use it for this purpose, the
 * caller must provide entity state as serialized protobuf bytes. This is the entity equivalent of
 * {@link OrchestrationRunner}.
 * <p>
 * Typical usage in an Azure Functions entity trigger:
 * <pre>
 * {@literal @}FunctionName("Counter")
 * public String counterEntity(
 *         {@literal @}DurableEntityTrigger(name = "req") String req) {
 *     return EntityRunner.loadAndRun(req, () -&gt; new CounterEntity());
 * }
 * </pre>
 */
public final class EntityRunner {
    private static final Logger logger = Logger.getLogger(EntityRunner.class.getPackage().getName());

    private EntityRunner() {
    }

    /**
     * Loads an entity batch request from {@code base64EncodedEntityRequest} and uses it to execute
     * entity operations using the entity created by {@code entityFactory}.
     *
     * @param base64EncodedEntityRequest the base64-encoded protobuf payload representing an entity batch request
     * @param entityFactory a factory that creates the entity instance to handle operations
     * @return a base64-encoded protobuf payload of the entity batch result
     * @throws IllegalArgumentException if either parameter is {@code null} or if {@code base64EncodedEntityRequest}
     *                                  is not valid base64-encoded protobuf
     */
    public static String loadAndRun(String base64EncodedEntityRequest, TaskEntityFactory entityFactory) {
        byte[] decodedBytes = Base64.getDecoder().decode(base64EncodedEntityRequest);
        byte[] resultBytes = loadAndRun(decodedBytes, entityFactory);
        return Base64.getEncoder().encodeToString(resultBytes);
    }

    /**
     * Loads an entity batch request from {@code entityRequestBytes} and uses it to execute
     * entity operations using the entity created by {@code entityFactory}.
     *
     * @param entityRequestBytes the protobuf payload representing an entity batch request
     * @param entityFactory a factory that creates the entity instance to handle operations
     * @return a protobuf-encoded payload of the entity batch result
     * @throws IllegalArgumentException if either parameter is {@code null} or if {@code entityRequestBytes}
     *                                  is not valid protobuf
     */
    public static byte[] loadAndRun(byte[] entityRequestBytes, TaskEntityFactory entityFactory) {
        if (entityRequestBytes == null || entityRequestBytes.length == 0) {
            throw new IllegalArgumentException("entityRequestBytes must not be null or empty");
        }

        if (entityFactory == null) {
            throw new IllegalArgumentException("entityFactory must not be null");
        }

        EntityBatchRequest request;
        try {
            request = EntityBatchRequest.parseFrom(entityRequestBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("entityRequestBytes was not valid protobuf", e);
        }

        // Parse entity name from the instance ID so the executor can look it up
        String instanceId = request.getInstanceId();
        String entityName;
        try {
            entityName = EntityInstanceId.fromString(instanceId).getName();
        } catch (Exception e) {
            // Fallback: use the raw instance ID as the entity name
            entityName = instanceId;
        }

        HashMap<String, TaskEntityFactory> factories = new HashMap<>();
        factories.put(entityName, entityFactory);

        TaskEntityExecutor executor = new TaskEntityExecutor(
                factories,
                new JacksonDataConverter(),
                logger);

        EntityBatchResult result = executor.execute(request);
        return result.toByteArray();
    }

    /**
     * Loads an entity batch request from {@code base64EncodedEntityRequest} and uses it to execute
     * entity operations using the provided {@code entity} instance.
     *
     * @param base64EncodedEntityRequest the base64-encoded protobuf payload representing an entity batch request
     * @param entity the entity instance to handle operations
     * @return a base64-encoded protobuf payload of the entity batch result
     * @throws IllegalArgumentException if either parameter is {@code null} or if {@code base64EncodedEntityRequest}
     *                                  is not valid base64-encoded protobuf
     */
    public static String loadAndRun(String base64EncodedEntityRequest, ITaskEntity entity) {
        if (entity == null) {
            throw new IllegalArgumentException("entity must not be null");
        }
        return loadAndRun(base64EncodedEntityRequest, () -> entity);
    }

    /**
     * Loads an entity batch request from {@code entityRequestBytes} and uses it to execute
     * entity operations using the provided {@code entity} instance.
     *
     * @param entityRequestBytes the protobuf payload representing an entity batch request
     * @param entity the entity instance to handle operations
     * @return a protobuf-encoded payload of the entity batch result
     * @throws IllegalArgumentException if either parameter is {@code null} or if {@code entityRequestBytes}
     *                                  is not valid protobuf
     */
    public static byte[] loadAndRun(byte[] entityRequestBytes, ITaskEntity entity) {
        if (entity == null) {
            throw new IllegalArgumentException("entity must not be null");
        }
        return loadAndRun(entityRequestBytes, () -> entity);
    }
}
