// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc.*;

import io.grpc.*;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 * Durable Task client implementation that uses gRPC to connect to a remote "sidecar" process.
 */
public final class DurableTaskGrpcClient extends DurableTaskClient {
    private static final int DEFAULT_PORT = 4001;
    private static final Logger logger = Logger.getLogger(DurableTaskGrpcClient.class.getPackage().getName());

    private final DataConverter dataConverter;
    private final ManagedChannel managedSidecarChannel;
    private final TaskHubSidecarServiceBlockingStub sidecarClient;

    DurableTaskGrpcClient(DurableTaskGrpcClientBuilder builder) {
        this.dataConverter = builder.dataConverter != null ? builder.dataConverter : new JacksonDataConverter();

        Channel sidecarGrpcChannel;
        if (builder.channel != null) {
            // The caller is responsible for managing the channel lifetime
            this.managedSidecarChannel = null;
            sidecarGrpcChannel = builder.channel;
        } else {
            // Construct our own channel using localhost + a port number
            int port = DEFAULT_PORT;
            if (builder.port > 0) {
                port = builder.port;
            }

            // Need to keep track of this channel so we can dispose it on close()
            this.managedSidecarChannel = ManagedChannelBuilder
                    .forAddress("localhost", port)
                    .usePlaintext()
                    .build();
            sidecarGrpcChannel = this.managedSidecarChannel;
        }

        this.sidecarClient = TaskHubSidecarServiceGrpc.newBlockingStub(sidecarGrpcChannel);
    }

    /**
     * Closes the internally managed gRPC channel, if one exists.
     * <p>
     * This method is a no-op if this client object was created using a builder with a gRPC channel object explicitly
     * configured.
     */
    @Override
    public void close() {
        if (this.managedSidecarChannel != null) {
            try {
                this.managedSidecarChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // Best effort. Also note that AutoClose documentation recommends NOT having
                // close() methods throw InterruptedException:
                // https://docs.oracle.com/javase/7/docs/api/java/lang/AutoCloseable.html
            }
        }
    }

    @Override
    public String scheduleNewOrchestrationInstance(
            String orchestratorName,
            NewOrchestrationInstanceOptions options) {
        if (orchestratorName == null || orchestratorName.length() == 0) {
            throw new IllegalArgumentException("A non-empty orchestrator name must be specified.");
        }

        Helpers.throwIfArgumentNull(options, "options");

        CreateInstanceRequest.Builder builder = CreateInstanceRequest.newBuilder();
        builder.setName(orchestratorName);

        String instanceId = options.getInstanceId();
        if (instanceId == null) {
            instanceId = UUID.randomUUID().toString();
        }
        builder.setInstanceId(instanceId);

        String version = options.getVersion();
        if (version != null) {
            builder.setVersion(StringValue.of(version));
        }

        Object input = options.getInput();
        if (input != null) {
            String serializedInput = this.dataConverter.serialize(input);
            builder.setInput(StringValue.of(serializedInput));
        }

        Instant startTime = options.getStartTime();
        if (startTime != null) {
            Timestamp ts = DataConverter.getTimestampFromInstant(startTime);
            builder.setScheduledStartTimestamp(ts);
        }

        CreateInstanceRequest request = builder.build();
        CreateInstanceResponse response = this.sidecarClient.startInstance(request);
        return response.getInstanceId();
    }

    @Override
    public void raiseEvent(String instanceId, String eventName, Object eventPayload) {
        Helpers.throwIfArgumentNull(instanceId, "instanceId");
        Helpers.throwIfArgumentNull(eventName, "eventName");

        RaiseEventRequest.Builder builder = RaiseEventRequest.newBuilder()
                .setInstanceId(instanceId)
                .setName(eventName);
        if (eventPayload != null) {
            String serializedPayload = this.dataConverter.serialize(eventPayload);
            builder.setInput(StringValue.of(serializedPayload));
        }

        RaiseEventRequest request = builder.build();
        this.sidecarClient.raiseEvent(request);
    }

    @Override
    public OrchestrationMetadata getInstanceMetadata(String instanceId, boolean getInputsAndOutputs) {
        GetInstanceRequest request = GetInstanceRequest.newBuilder()
                .setInstanceId(instanceId)
                .setGetInputsAndOutputs(getInputsAndOutputs)
                .build();
        GetInstanceResponse response = this.sidecarClient.getInstance(request);
        return new OrchestrationMetadata(response, this.dataConverter, request.getGetInputsAndOutputs());
    }

    @Override
    public OrchestrationMetadata waitForInstanceStart(String instanceId, Duration timeout, boolean getInputsAndOutputs) throws TimeoutException {
        GetInstanceRequest request = GetInstanceRequest.newBuilder()
                .setInstanceId(instanceId)
                .setGetInputsAndOutputs(getInputsAndOutputs)
                .build();

        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            timeout = Duration.ofMinutes(10);
        }

        TaskHubSidecarServiceBlockingStub grpcClient = this.sidecarClient.withDeadlineAfter(
                timeout.toMillis(),
                TimeUnit.MILLISECONDS);

        GetInstanceResponse response;
        try {
            response = grpcClient.waitForInstanceStart(request);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                throw new TimeoutException("Start orchestration timeout reached.");
            }
            throw e;
        }
        return new OrchestrationMetadata(response, this.dataConverter, request.getGetInputsAndOutputs());
    }

    @Override
    public OrchestrationMetadata waitForInstanceCompletion(String instanceId, Duration timeout, boolean getInputsAndOutputs) throws TimeoutException {
        GetInstanceRequest request = GetInstanceRequest.newBuilder()
                .setInstanceId(instanceId)
                .setGetInputsAndOutputs(getInputsAndOutputs)
                .build();

        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            timeout = Duration.ofMinutes(10);
        }

        TaskHubSidecarServiceBlockingStub grpcClient = this.sidecarClient.withDeadlineAfter(
                timeout.toMillis(),
                TimeUnit.MILLISECONDS);

        GetInstanceResponse response;
        try {
            response = grpcClient.waitForInstanceCompletion(request);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                throw new TimeoutException("Orchestration instance completion timeout reached.");
            }
            throw e;
        }
        return new OrchestrationMetadata(response, this.dataConverter, request.getGetInputsAndOutputs());
    }

    @Override
    public void terminate(String instanceId, @Nullable Object output) {
        Helpers.throwIfArgumentNull(instanceId, "instanceId");
        String serializeOutput = this.dataConverter.serialize(output);
        this.logger.fine(() -> String.format(
                "Terminating instance %s and setting output to: %s",
                instanceId,
                serializeOutput != null ? serializeOutput : "(null)"));
        TerminateRequest.Builder builder = TerminateRequest.newBuilder().setInstanceId(instanceId);
        if (serializeOutput != null){
            builder.setOutput(StringValue.of(serializeOutput));
        }
        this.sidecarClient.terminateInstance(builder.build());
    }

    @Override
    public OrchestrationStatusQueryResult queryInstances(OrchestrationStatusQuery query) {
        InstanceQuery.Builder instanceQueryBuilder = InstanceQuery.newBuilder();
        Optional.ofNullable(query.getCreatedTimeFrom()).ifPresent(createdTimeFrom -> instanceQueryBuilder.setCreatedTimeFrom(DataConverter.getTimestampFromInstant(createdTimeFrom)));
        Optional.ofNullable(query.getCreatedTimeTo()).ifPresent(createdTimeTo -> instanceQueryBuilder.setCreatedTimeTo(DataConverter.getTimestampFromInstant(createdTimeTo)));
        Optional.ofNullable(query.getContinuationToken()).ifPresent(token -> instanceQueryBuilder.setContinuationToken(StringValue.of(token)));
        Optional.ofNullable(query.getInstanceIdPrefix()).ifPresent(prefix -> instanceQueryBuilder.setInstanceIdPrefix(StringValue.of(prefix)));
        instanceQueryBuilder.setFetchInputsAndOutputs(query.isFetchInputsAndOutputs());
        instanceQueryBuilder.setMaxInstanceCount(query.getMaxInstanceCount());
        query.getRuntimeStatusList().forEach(runtimeStatus -> Optional.ofNullable(runtimeStatus).ifPresent(status -> instanceQueryBuilder.addRuntimeStatus(OrchestrationRuntimeStatus.toProtobuf(status))));
        query.getTaskHubNames().forEach(taskHubName -> Optional.ofNullable(taskHubName).ifPresent(name -> instanceQueryBuilder.addTaskHubNames(StringValue.of(name))));
        QueryInstancesResponse queryInstancesResponse = this.sidecarClient.queryInstances(QueryInstancesRequest.newBuilder().setQuery(instanceQueryBuilder).build());
        return toQueryResult(queryInstancesResponse, query.isFetchInputsAndOutputs());
    }

    private OrchestrationStatusQueryResult toQueryResult(QueryInstancesResponse queryInstancesResponse, boolean fetchInputsAndOutputs){
        List<OrchestrationMetadata> metadataList = new ArrayList<>();
        queryInstancesResponse.getOrchestrationStateList().forEach(state -> {
            metadataList.add(new OrchestrationMetadata(state, this.dataConverter, fetchInputsAndOutputs));
        });
        return new OrchestrationStatusQueryResult(metadataList, queryInstancesResponse.getContinuationToken().getValue());
    }

    @Override
    public void createTaskHub(boolean recreateIfExists) {
        this.sidecarClient.createTaskHub(CreateTaskHubRequest.newBuilder().setRecreateIfExists(recreateIfExists).build());
    }

    @Override
    public void deleteTaskHub() {
        this.sidecarClient.deleteTaskHub(DeleteTaskHubRequest.newBuilder().build());
    }

    @Override
    public PurgeResult purgeInstance(String instanceId) {
        PurgeInstancesRequest request = PurgeInstancesRequest.newBuilder()
                .setInstanceId(instanceId)
                .build();

        PurgeInstancesResponse response = this.sidecarClient.purgeInstances(request);
        return toPurgeResult(response);
    }

    @Override
    public PurgeResult purgeInstances(PurgeInstanceCriteria purgeInstanceCriteria) throws TimeoutException {
        PurgeInstanceFilter.Builder builder = PurgeInstanceFilter.newBuilder();
        builder.setCreatedTimeFrom(DataConverter.getTimestampFromInstant(purgeInstanceCriteria.getCreatedTimeFrom()));
        Optional.ofNullable(purgeInstanceCriteria.getCreatedTimeTo()).ifPresent(createdTimeTo -> builder.setCreatedTimeTo(DataConverter.getTimestampFromInstant(createdTimeTo)));
        purgeInstanceCriteria.getRuntimeStatusList().forEach(runtimeStatus -> Optional.ofNullable(runtimeStatus).ifPresent(status -> builder.addRuntimeStatus(OrchestrationRuntimeStatus.toProtobuf(status))));

        Duration timeout = purgeInstanceCriteria.getTimeout();
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            timeout = Duration.ofMinutes(4);
        }

        TaskHubSidecarServiceBlockingStub grpcClient = this.sidecarClient.withDeadlineAfter(
                timeout.toMillis(),
                TimeUnit.MILLISECONDS);

        PurgeInstancesResponse response;
        try {
            response = grpcClient.purgeInstances(PurgeInstancesRequest.newBuilder().setPurgeInstanceFilter(builder).build());
            return toPurgeResult(response);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                String timeOutException = String.format("Purge instances timeout duration of %s reached.", timeout);
                throw new TimeoutException(timeOutException);
            }
            throw e;
        }
    }

    @Override
    public void suspendInstance(String instanceId, @Nullable String reason) {
        SuspendRequest.Builder suspendRequestBuilder = SuspendRequest.newBuilder();
        suspendRequestBuilder.setInstanceId(instanceId);
        if (reason != null) {
            suspendRequestBuilder.setReason(StringValue.of(reason));
        }
        this.sidecarClient.suspendInstance(suspendRequestBuilder.build());
    }

    @Override
    public void resumeInstance(String instanceId, @Nullable String reason) {
        ResumeRequest.Builder resumeRequestBuilder = ResumeRequest.newBuilder();
        resumeRequestBuilder.setInstanceId(instanceId);
        if (reason != null) {
            resumeRequestBuilder.setReason(StringValue.of(reason));
        }
        this.sidecarClient.resumeInstance(resumeRequestBuilder.build());
    }

    @Override
    public String restartInstance(String instanceId, boolean restartWithNewInstanceId) {
        OrchestrationMetadata metadata = this.getInstanceMetadata(instanceId, true);
        if (!metadata.isInstanceFound()) {
            throw new IllegalArgumentException(new StringBuilder()
                    .append("An orchestration with instanceId ")
                    .append(instanceId)
                    .append(" was not found.").toString());
        }

        if (restartWithNewInstanceId) {
            return this.scheduleNewOrchestrationInstance(metadata.getName(), this.dataConverter.deserialize(metadata.getSerializedInput(), Object.class));
        }
        else {
            return this.scheduleNewOrchestrationInstance(metadata.getName(), this.dataConverter.deserialize(metadata.getSerializedInput(), Object.class), metadata.getInstanceId());
        }
    }

    private PurgeResult toPurgeResult(PurgeInstancesResponse response){
        return new PurgeResult(response.getDeletedInstanceCount());
    }
}
