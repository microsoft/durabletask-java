// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.microsoft.durabletask.implementation.protobuf.OrchestratorService;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.OrchestrationState;

import java.time.Instant;

import static com.microsoft.durabletask.Helpers.isNullOrEmpty;

/**
 * Represents a snapshot of an orchestration instance's current state, including metadata.
 * <p>
 * Instances of this class are produced by methods in the {@link DurableTaskClient} class, such as
 * {@link DurableTaskClient#getInstanceMetadata}, {@link DurableTaskClient#waitForInstanceStart}  and
 * {@link DurableTaskClient#waitForInstanceCompletion}
 */
public final class OrchestrationMetadata {
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
        this(fetchResponse.getOrchestrationState(), dataConverter, requestedInputsAndOutputs);
    }

    OrchestrationMetadata(
            OrchestrationState state,
            DataConverter dataConverter,
            boolean requestedInputsAndOutputs) {
        this.dataConverter = dataConverter;
        this.requestedInputsAndOutputs = requestedInputsAndOutputs;

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

    /**
     * Gets the name of the orchestration.
     * @return the name of the orchestration
     */
    public String getName() {
        return this.name;
    }

    /**
     * Gets the unique ID of the orchestration instance.
     * @return the unique ID of the orchestration instance
     */
    public String getInstanceId() {
        return this.instanceId;
    }

    /**
     * Gets the current runtime status of the orchestration instance at the time this object was fetched.
     * @return the current runtime status of the orchestration instance at the time this object was fetched
     */
    public OrchestrationRuntimeStatus getRuntimeStatus() {
        return this.runtimeStatus;
    }

    /**
     * Gets the orchestration instance's creation time in UTC.
     * @return the orchestration instance's creation time in UTC
     */
    public Instant getCreatedAt() {
        return this.createdAt;
    }

    /**
     * Gets the orchestration instance's last updated time in UTC.
     * @return the orchestration instance's last updated time in UTC
     */
    public Instant getLastUpdatedAt() {
        return this.lastUpdatedAt;
    }

    /**
     * Gets the orchestration instance's serialized input, if any, as a string value.
     * @return the orchestration instance's serialized input or {@code null}
     */
    public String getSerializedInput() {
        return this.serializedInput;
    }

    /**
     * Gets the orchestration instance's serialized output, if any, as a string value.
     * @return the orchestration instance's serialized output or {@code null}
     */
    public String getSerializedOutput() {
        return this.serializedOutput;
    }

    /**
     * Gets the failure details, if any, for the failed orchestration instance.
     * <p>
     * This method returns data only if the orchestration is in the {@link OrchestrationRuntimeStatus#FAILED} state,
     * and only if this instance metadata was fetched with the option to include output data.
     *
     * @return the failure details of the failed orchestration instance or {@code null}
     */
    public FailureDetails getFailureDetails() {
        return this.failureDetails;
    }

    /**
     * Gets a value indicating whether the orchestration instance was running at the time this object was fetched.
     *
     * @return {@code true} if the orchestration was in a running state; otherwise {@code false}
     */
    public boolean isRunning() {
        return isInstanceFound() && this.runtimeStatus == OrchestrationRuntimeStatus.RUNNING;
    }

    /**
     * Gets a value indicating whether the orchestration instance was completed at the time this object was fetched.
     * <p>
     * An orchestration instance is considered completed when its runtime status value is
     * {@link OrchestrationRuntimeStatus#COMPLETED}, {@link OrchestrationRuntimeStatus#FAILED}, or
     * {@link OrchestrationRuntimeStatus#TERMINATED}.
     *
     * @return {@code true} if the orchestration was in a terminal state; otherwise {@code false}
     */
    public boolean isCompleted() {
        return
            this.runtimeStatus == OrchestrationRuntimeStatus.COMPLETED ||
            this.runtimeStatus == OrchestrationRuntimeStatus.FAILED ||
            this.runtimeStatus == OrchestrationRuntimeStatus.TERMINATED;
    }

    /**
     * Deserializes the orchestration's input into an object of the specified type.
     * <p>
     * Deserialization is performed using the {@link DataConverter} that was configured on the {@link DurableTaskClient}
     * object that created this orchestration metadata object.
     *
     * @param type the class associated with the type to deserialize the input data into
     * @param <T> the type to deserialize the input data into
     * @throws IllegalStateException if the metadata was fetched without the option to read inputs and outputs
     * @return the deserialized input value
     */
    public <T> T readInputAs(Class<T> type) {
        return this.readPayloadAs(type, this.serializedInput);
    }

    /**
     * Deserializes the orchestration's output into an object of the specified type.
     * <p>
     * Deserialization is performed using the {@link DataConverter} that was configured on the {@link DurableTaskClient}
     * object that created this orchestration metadata object.
     *
     * @param type the class associated with the type to deserialize the output data into
     * @param <T> the type to deserialize the output data into
     * @throws IllegalStateException if the metadata was fetched without the option to read inputs and outputs
     * @return the deserialized input value
     */
    public <T> T readOutputAs(Class<T> type) {
        return this.readPayloadAs(type, this.serializedOutput);
    }

    /**
     * Deserializes the orchestration's custom status into an object of the specified type.
     * <p>
     * Deserialization is performed using the {@link DataConverter} that was configured on the {@link DurableTaskClient}
     * object that created this orchestration metadata object.
     *
     * @param type the class associated with the type to deserialize the custom status data into
     * @param <T> the type to deserialize the custom status data into
     * @throws IllegalStateException if the metadata was fetched without the option to read inputs and outputs
     * @return the deserialized input value
     */
    public <T> T readCustomStatusAs(Class<T> type) {
        return this.readPayloadAs(type, this.serializedCustomStatus);
    }

    /**
     * Returns {@code true} if the orchestration has a non-empty custom status value; otherwise {@code false}.
     * <p>
     * This method will always return {@code false} if the metadata was fetched without the option to read inputs and
     * outputs
     *
     * @return {@code true} if the orchestration has a non-empty custom status value; otherwise {@code false}
     */
    public boolean isCustomStatusFetched() {
        return this.serializedCustomStatus != null && !this.serializedCustomStatus.isEmpty();
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

    /**
     * Generates a user-friendly string representation of the current metadata object.
     * @return a user-friendly string representation of the current metadata object
     */
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

    /**
     * Returns {@code true} if an orchestration instance with this ID was found; otherwise {@code false}.
     *
     * @return {@code true} if an orchestration instance with this ID was found; otherwise {@code false}
     */
    public boolean isInstanceFound() {
        return !(isNullOrEmpty(this.name) && isNullOrEmpty(this.instanceId));
    }
}
