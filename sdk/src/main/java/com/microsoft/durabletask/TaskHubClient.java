// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.protobuf.OrchestratorService.*;
import com.microsoft.durabletask.protobuf.TaskHubClientServiceGrpc;
import com.microsoft.durabletask.protobuf.TaskHubClientServiceGrpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TaskHubClient {
    private static final int DEFAULT_PORT = 4001;

    private final DataConverter dataConverter;
    private final TaskHubClientServiceBlockingStub grpcClient;

    private TaskHubClient(Builder builder) {
        this.dataConverter = Objects.requireNonNullElseGet(builder.dataConverter, JacksonDataConverter::new);

        int port = builder.port;
        if (port <= 0) {
            port = DEFAULT_PORT;
        }

        // We assume unencrypted localhost for all communication.
        // Requests that need to cross trust boundaries should go through secure gRPC proxies, like Dapr.
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", port)
                .usePlaintext()
                .build();
        this.grpcClient = TaskHubClientServiceGrpc.newBlockingStub(channel);
    }

    public String scheduleNewOrchestrationInstance(String orchestrationName, Object input) {
        String randomInstanceId = UUID.randomUUID().toString();
        return this.scheduleNewOrchestrationInstance(orchestrationName, input, randomInstanceId);
    }

    public String scheduleNewOrchestrationInstance(String orchestrationName, Object input, String instanceId) {
        NewOrchestrationInstanceOptions options = NewOrchestrationInstanceOptions.newBuilder()
                .setInput(input)
                .setInstanceId(instanceId)
                .build();
        return this.scheduleNewOrchestrationInstance(orchestrationName, options);
    }

    // TODO: Create async flavors of these APIs
    public String scheduleNewOrchestrationInstance(
            String orchestrationName,
            NewOrchestrationInstanceOptions options) {
        if (orchestrationName == null || orchestrationName.length() == 0) {
            throw new IllegalArgumentException("A non-empty orchestrator name must be specified.");
        }

        CreateInstanceRequest.Builder builder = CreateInstanceRequest.newBuilder();
        builder.setName(orchestrationName);

        String instanceId = options.getInstanceId();
        if (instanceId == null) {
            instanceId = UUID.randomUUID().toString();
            builder.setInstanceId(instanceId);
        }

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
        CreateInstanceResponse response = this.grpcClient.startInstance(request);
        return response.getInstanceId();
    }

    // TODO: More parameters
    public TaskOrchestrationInstance getOrchestrationInstance(String instanceId, boolean getInputsAndOutputs) {
        GetInstanceRequest request = GetInstanceRequest.newBuilder()
                .setInstanceId(instanceId)
                .setGetInputsAndOutputs(getInputsAndOutputs)
                .build();
        GetInstanceResponse response = this.grpcClient.getInstance(request);
        return new TaskOrchestrationInstance(request, response, this.dataConverter);
    }

    // TODO: What other parameters do we want to support? Input/output fetching?
    public TaskOrchestrationInstance waitForInstanceStart(String instanceId, Duration timeout) {
        GetInstanceRequest request = GetInstanceRequest.newBuilder().setInstanceId(instanceId).build();

        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            timeout = Duration.ofMinutes(10);
        }

        TaskHubClientServiceBlockingStub grpcClient = this.grpcClient.withDeadlineAfter(
                timeout.toMillis(),
                TimeUnit.MILLISECONDS);
        GetInstanceResponse response = grpcClient.waitForInstanceStart(request);
        return new TaskOrchestrationInstance(request, response, this.dataConverter);
    }

    public TaskOrchestrationInstance waitForInstanceCompletion(String instanceId, Duration timeout, boolean getInputsAndOutputs) {
        GetInstanceRequest request = GetInstanceRequest.newBuilder()
                .setInstanceId(instanceId)
                .setGetInputsAndOutputs(getInputsAndOutputs)
                .build();

        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            timeout = Duration.ofMinutes(10);
        }

        TaskHubClientServiceBlockingStub grpcClient = this.grpcClient.withDeadlineAfter(
                timeout.toMillis(),
                TimeUnit.MILLISECONDS);
        GetInstanceResponse response = grpcClient.waitForInstanceCompletion(request);
        return new TaskOrchestrationInstance(request, response, this.dataConverter);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private DataConverter dataConverter;
        private int port;

        public void setDataConverter(DataConverter dataConverter) {
            this.dataConverter = dataConverter;
        }

        public void setPort(int port) {
            this.port = port;
        }

        // TODO: Have this return an interface?
        public TaskHubClient build() {
            return new TaskHubClient(this);
        }
    }
}
