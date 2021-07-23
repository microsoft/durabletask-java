// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.microsoft.durabletask.protobuf.OrchestratorService;

import java.time.Instant;

public class TaskOrchestrationInstance {
    private final OrchestratorService.GetInstanceRequest fetchRequest;
    private final OrchestratorService.GetInstanceResponse fetchResponse;
    private final DataConverter dataConverter;

    TaskOrchestrationInstance(
            OrchestratorService.GetInstanceRequest fetchRequest,
            OrchestratorService.GetInstanceResponse fetchResponse,
            DataConverter dataConverter) {
        this.fetchRequest = fetchRequest;
        this.fetchResponse = fetchResponse;
        this.dataConverter = dataConverter;
    }

    public String getInstanceId() {
        return this.fetchResponse.getInstanceId().getValue();
    }

    public OrchestrationRuntimeStatus getRuntimeStatus() {
        return OrchestrationRuntimeStatus.fromProtobuf(this.fetchResponse.getOrchestrationStatus());
    }

    public Instant getCreatedTime() {
        return DataConverter.getInstantFromTimestamp(this.fetchResponse.getCreatedTimestamp());
    }

    public Instant getLastUpdatedTime() {
        return DataConverter.getInstantFromTimestamp(this.fetchResponse.getLastUpdatedTimestamp());
    }

    public String getSerializedInput() {
        return this.fetchResponse.getInput().getValue();
    }

    public String getSerializedOutput() {
        return this.fetchResponse.getOutput().getValue();
    }

    public <T> T getInputAs(Class<T> type) {
        String serializedInput = this.getSerializedInput();

        // Note that the Java gRPC implementation converts null protobuf strings into empty Java strings
        if (serializedInput == null || serializedInput.isEmpty()) {
            return null;
        }

        return this.dataConverter.deserialize(serializedInput, type);
    }

    public <T> T getOutputAs(Class<T> type) {
        String serializedOutput = this.getSerializedOutput();

        // Note that the Java gRPC implementation converts null protobuf strings into empty Java strings
        if (serializedOutput == null || serializedOutput.isEmpty()) {
            return null;
        }

        return this.dataConverter.deserialize(serializedOutput, type);
    }

    @Override
    public String toString() {
        String baseString = String.format(
                "ID: '%s', runtime status: %s, created: %s, last updated: %s",
                this.getInstanceId(),
                this.getRuntimeStatus(),
                this.getCreatedTime(),
                this.getLastUpdatedTime());

        if (this.fetchRequest.getGetInputsAndOutputs()) {
            baseString += String.format(
                    ", serialized input: %s, serialized output: %s",
                    this.getSerializedInput(),
                    this.getSerializedOutput());
        }

        return "{" + baseString + "}";
    }
}
