// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.microsoft.durabletask.protobuf.OrchestratorService;
import com.microsoft.durabletask.protobuf.OrchestratorService.OrchestrationState;

import java.time.Instant;

public class OrchestrationMetadata {
    private final DataConverter dataConverter;
    private final boolean requestedInputsAndOutputs;

    private final String name;
    private final String instanceId;
    private final OrchestrationRuntimeStatus runtimeStatus;
    private final Instant createdAt;
    private final Instant lastUpdatedAt;
    private final String serializedInput;
    private final String serializedOutput;
    private final String serializedCustomStatus;
    private final FailureDetails failureDetails;

    OrchestrationMetadata(
            OrchestratorService.GetInstanceResponse fetchResponse,
            DataConverter dataConverter,
            boolean requestedInputsAndOutputs) {
        this.dataConverter = dataConverter;
        this.requestedInputsAndOutputs = requestedInputsAndOutputs;

        OrchestrationState state = fetchResponse.getOrchestrationState();
        this.name = state.getName();
        this.instanceId = state.getInstanceId();
        this.runtimeStatus = OrchestrationRuntimeStatus.fromProtobuf(state.getOrchestrationStatus());
        this.createdAt = DataConverter.getInstantFromTimestamp(state.getCreatedTimestamp());
        this.lastUpdatedAt = DataConverter.getInstantFromTimestamp(state.getLastUpdatedTimestamp());
        this.serializedInput = state.getInput().getValue();
        this.serializedOutput = state.getOutput().getValue();
        this.serializedCustomStatus = state.getCustomStatus().getValue();
        this.failureDetails = new FailureDetails(state.getFailureDetails());
    }

    public String getName() {
        return this.name;
    }

    public String getInstanceId() {
        return this.instanceId;
    }

    public OrchestrationRuntimeStatus getRuntimeStatus() {
        return this.runtimeStatus;
    }

    public Instant getCreatedAt() {
        return this.createdAt;
    }

    public Instant getLastUpdatedAt() {
        return this.lastUpdatedAt;
    }

    public String getSerializedInput() {
        return this.serializedInput;
    }

    public String getSerializedOutput() {
        return this.serializedOutput;
    }

    public FailureDetails getFailureDetails() {
        return this.failureDetails;
    }

    public boolean isRunning() {
        return this.runtimeStatus == OrchestrationRuntimeStatus.RUNNING;
    }

    public boolean isCompleted() {
        return
            this.runtimeStatus == OrchestrationRuntimeStatus.COMPLETED ||
            this.runtimeStatus == OrchestrationRuntimeStatus.FAILED ||
            this.runtimeStatus == OrchestrationRuntimeStatus.TERMINATED;
    }

    public <T> T readInputAs(Class<T> type) {
        return this.readPayloadAs(type, this.serializedInput);
    }

    public <T> T readOutputAs(Class<T> type) {
        return this.readPayloadAs(type, this.serializedOutput);
    }

    public <T> T readCustomStatusAs(Class<T> type) {
        return this.readPayloadAs(type, this.serializedCustomStatus);
    }

    public <T> boolean hasCustomStatus(Object object, Class<T> type){
        return object.equals(this.readCustomStatusAs(type));
    }

    private <T> T readPayloadAs(Class<T> type, String payload) {
        if (!this.requestedInputsAndOutputs) {
            throw new IllegalStateException("This method can only be used when instance metadata is fetched with the option to include input and output data.");
        }

        // Note that the Java gRPC implementation converts null protobuf strings into empty Java strings
        if (payload == null || payload.isEmpty()) {
            return null;
        }

        return this.dataConverter.deserialize(payload, type);
    }

    @Override
    public String toString() {
        String baseString = String.format(
                "[Name: '%s', ID: '%s', RuntimeStatus: %s, CreatedAt: %s, LastUpdatedAt: %s",
                this.name,
                this.instanceId,
                this.runtimeStatus,
                this.createdAt,
                this.lastUpdatedAt);
        StringBuilder sb = new StringBuilder(baseString);
        if (this.serializedInput != null) {
            sb.append(", Input: '").append(getTrimmedPayload(this.serializedInput)).append('\'');
        }

        if (this.serializedOutput != null) {
            sb.append(", Output: '").append(getTrimmedPayload(this.serializedOutput)).append('\'');
        }

        return sb.append(']').toString();
    }

    private static String getTrimmedPayload(String payload)
    {
        int maxLength = 50;
        if (payload.length() > maxLength)
        {
            return payload.substring(0, maxLength) + "...";
        }

        return payload;
    }
}
