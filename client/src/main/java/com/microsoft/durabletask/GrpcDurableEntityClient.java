// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc.TaskHubSidecarServiceBlockingStub;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * gRPC-based implementation of {@link DurableEntityClient}.
 */
final class GrpcDurableEntityClient extends DurableEntityClient {

    private final TaskHubSidecarServiceBlockingStub sidecarClient;
    private final DataConverter dataConverter;

    GrpcDurableEntityClient(
            String name,
            TaskHubSidecarServiceBlockingStub sidecarClient,
            DataConverter dataConverter) {
        super(name);
        this.sidecarClient = sidecarClient;
        this.dataConverter = dataConverter;
    }

    @Override
    public void signalEntity(
            EntityInstanceId entityId,
            String operationName,
            @Nullable Object input,
            @Nullable SignalEntityOptions options) {
        Helpers.throwIfArgumentNull(entityId, "entityId");
        Helpers.throwIfArgumentNull(operationName, "operationName");

        SignalEntityRequest.Builder builder = SignalEntityRequest.newBuilder()
                .setInstanceId(entityId.toString())
                .setName(operationName)
                .setRequestId(UUID.randomUUID().toString());

        if (input != null) {
            String serializedInput = this.dataConverter.serialize(input);
            if (serializedInput != null) {
                builder.setInput(StringValue.of(serializedInput));
            }
        }

        if (options != null && options.getScheduledTime() != null) {
            Timestamp ts = DataConverter.getTimestampFromInstant(options.getScheduledTime());
            builder.setScheduledTime(ts);
        }

        this.sidecarClient.signalEntity(builder.build());
    }

    @Override
    @Nullable
    public EntityMetadata getEntityMetadata(EntityInstanceId entityId, boolean includeState) {
        Helpers.throwIfArgumentNull(entityId, "entityId");

        GetEntityRequest request = GetEntityRequest.newBuilder()
                .setInstanceId(entityId.toString())
                .setIncludeState(includeState)
                .build();

        GetEntityResponse response = this.sidecarClient.getEntity(request);
        if (!response.getExists()) {
            return null;
        }

        return toEntityMetadata(response.getEntity(), includeState);
    }

    @Override
    public EntityQueryResult queryEntities(EntityQuery query) {
        Helpers.throwIfArgumentNull(query, "query");

        com.microsoft.durabletask.implementation.protobuf.OrchestratorService.EntityQuery.Builder queryBuilder =
                com.microsoft.durabletask.implementation.protobuf.OrchestratorService.EntityQuery.newBuilder();

        if (query.getInstanceIdStartsWith() != null) {
            queryBuilder.setInstanceIdStartsWith(StringValue.of(query.getInstanceIdStartsWith()));
        }
        if (query.getLastModifiedFrom() != null) {
            queryBuilder.setLastModifiedFrom(DataConverter.getTimestampFromInstant(query.getLastModifiedFrom()));
        }
        if (query.getLastModifiedTo() != null) {
            queryBuilder.setLastModifiedTo(DataConverter.getTimestampFromInstant(query.getLastModifiedTo()));
        }
        queryBuilder.setIncludeState(query.isIncludeState());
        queryBuilder.setIncludeTransient(query.isIncludeTransient());
        if (query.getPageSize() != null) {
            queryBuilder.setPageSize(com.google.protobuf.Int32Value.of(query.getPageSize()));
        }
        if (query.getContinuationToken() != null) {
            queryBuilder.setContinuationToken(StringValue.of(query.getContinuationToken()));
        }

        QueryEntitiesRequest request = QueryEntitiesRequest.newBuilder()
                .setQuery(queryBuilder)
                .build();

        QueryEntitiesResponse response = this.sidecarClient.queryEntities(request);

        List<EntityMetadata> entities = new ArrayList<>();
        for (com.microsoft.durabletask.implementation.protobuf.OrchestratorService.EntityMetadata protoEntity
                : response.getEntitiesList()) {
            entities.add(toEntityMetadata(protoEntity, query.isIncludeState()));
        }

        String continuationToken = response.hasContinuationToken()
                ? response.getContinuationToken().getValue()
                : null;

        return new EntityQueryResult(entities, continuationToken);
    }

    @Override
    public CleanEntityStorageResult cleanEntityStorage(CleanEntityStorageRequest request) {
        Helpers.throwIfArgumentNull(request, "request");

        int totalEmptyEntitiesRemoved = 0;
        int totalOrphanedLocksReleased = 0;
        String continuationToken = request.getContinuationToken();

        do {
            com.microsoft.durabletask.implementation.protobuf.OrchestratorService.CleanEntityStorageRequest.Builder builder =
                    com.microsoft.durabletask.implementation.protobuf.OrchestratorService.CleanEntityStorageRequest.newBuilder()
                            .setRemoveEmptyEntities(request.isRemoveEmptyEntities())
                            .setReleaseOrphanedLocks(request.isReleaseOrphanedLocks());

            if (continuationToken != null) {
                builder.setContinuationToken(StringValue.of(continuationToken));
            }

            CleanEntityStorageResponse response = this.sidecarClient.cleanEntityStorage(builder.build());

            totalEmptyEntitiesRemoved += response.getEmptyEntitiesRemoved();
            totalOrphanedLocksReleased += response.getOrphanedLocksReleased();

            continuationToken = response.hasContinuationToken()
                    ? response.getContinuationToken().getValue()
                    : null;
        } while (request.isContinueUntilComplete() && continuationToken != null);

        return new CleanEntityStorageResult(
                continuationToken,
                totalEmptyEntitiesRemoved,
                totalOrphanedLocksReleased);
    }

    private EntityMetadata toEntityMetadata(
            com.microsoft.durabletask.implementation.protobuf.OrchestratorService.EntityMetadata protoEntity,
            boolean includeState) {
        Instant lastModifiedTime = DataConverter.getInstantFromTimestamp(protoEntity.getLastModifiedTime());
        String lockedBy = protoEntity.hasLockedBy() ? protoEntity.getLockedBy().getValue() : null;
        String serializedState = protoEntity.hasSerializedState()
                ? protoEntity.getSerializedState().getValue()
                : null;

        return new EntityMetadata(
                protoEntity.getInstanceId(),
                lastModifiedTime,
                protoEntity.getBacklogQueueSize(),
                lockedBy,
                serializedState,
                includeState,
                this.dataConverter);
    }
}
