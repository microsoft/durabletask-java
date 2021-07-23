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

        String host = builder.host;
        if (host == null || host.length() == 0) {
            host = System.getenv("DURABLETASK_WORKER_HOST");
            if (host == null || host.length() == 0) {
                host = "127.0.0.1";
            }
        }

        // We assume unencrypted localhost for all communication.
        // Requests that need to cross trust boundaries should go through secure gRPC proxies, like Dapr.
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
        this.grpcClient = TaskHubClientServiceGrpc.newBlockingStub(channel);
    }

    public String scheduleNewOrchestrationInstance(String orchestratorName) {
        return this.scheduleNewOrchestrationInstance(orchestratorName, null, null);
    }

    public String scheduleNewOrchestrationInstance(String orchestratorName, Object input) {
        return this.scheduleNewOrchestrationInstance(orchestratorName, input, null);
    }

    public String scheduleNewOrchestrationInstance(String orchestratorName, Object input, String instanceId) {
        NewOrchestrationInstanceOptions options = NewOrchestrationInstanceOptions.newBuilder()
                .setInput(input)
                .setInstanceId(instanceId)
                .build();
        return this.scheduleNewOrchestrationInstance(orchestratorName, options);
    }

    // TODO: Create async flavors of these APIs
    public String scheduleNewOrchestrationInstance(
            String orchestratorName,
            NewOrchestrationInstanceOptions options) {
        if (orchestratorName == null || orchestratorName.length() == 0) {
            throw new IllegalArgumentException("A non-empty orchestrator name must be specified.");
        }

        if (options == null) {
            throw new IllegalArgumentException("Options must not be null.");
        }

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
        private String host;
        private int port;

        public Builder setDataConverter(DataConverter dataConverter) {
            this.dataConverter = dataConverter;
            return this;
        }

        public Builder forAddress(String host, int port) {
            this.host = host;
            this.port = port;
            return this;
        }

        // TODO: Have this return an interface?
        public TaskHubClient build() {
            return new TaskHubClient(this);
        }
    }
}
