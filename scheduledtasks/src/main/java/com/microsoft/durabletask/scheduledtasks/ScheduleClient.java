// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.EntityInstanceId;
import com.microsoft.durabletask.FailureDetails;
import com.microsoft.durabletask.OrchestrationMetadata;
import com.microsoft.durabletask.OrchestrationRuntimeStatus;
import com.microsoft.durabletask.TypedEntityMetadata;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for managing a single schedule. Mutating operations are routed through
 * {@link ExecuteScheduleOperationOrchestrator} and awaited so entity failures reach the caller; reads go directly to
 * the entity state.
 */
public final class ScheduleClient {

    private static final Logger LOGGER = Logger.getLogger(ScheduleClient.class.getName());

    private final DurableTaskClient client;
    private final String scheduleId;
    private final EntityInstanceId entityId;
    private final Duration operationTimeout;

    ScheduleClient(DurableTaskClient client, String scheduleId, Duration operationTimeout) {
        if (scheduleId == null || scheduleId.isEmpty()) {
            throw new IllegalArgumentException("scheduleId must not be null or empty.");
        }
        this.client = client;
        this.scheduleId = scheduleId;
        this.entityId = new EntityInstanceId(Schedule.NAME, scheduleId);
        this.operationTimeout = operationTimeout;
    }

    /** @return the ID of this schedule. */
    public String getScheduleId() {
        return this.scheduleId;
    }

    /**
     * Creates this schedule, or replaces its configuration if it already exists.
     *
     * @param options the creation options
     */
    public void create(ScheduleCreationOptions options) {
        if (options == null) {
            throw new ScheduleClientValidationException(this.scheduleId, "options must not be null.");
        }
        runOperation(ScheduleTransitions.CREATE_SCHEDULE, options);
    }

    /**
     * Updates this schedule's configuration.
     *
     * @param options the update options
     */
    public void update(ScheduleUpdateOptions options) {
        if (options == null) {
            throw new ScheduleClientValidationException(this.scheduleId, "options must not be null.");
        }
        runOperation(ScheduleTransitions.UPDATE_SCHEDULE, options);
    }

    /** Pauses this schedule. */
    public void pause() {
        runOperation(ScheduleTransitions.PAUSE_SCHEDULE, null);
    }

    /** Resumes this schedule. */
    public void resume() {
        runOperation(ScheduleTransitions.RESUME_SCHEDULE, null);
    }

    /** Deletes this schedule. Does not affect target orchestrations that have already started. */
    public void delete() {
        runOperation(ScheduleTransitions.DELETE, null);
    }

    /**
     * Retrieves the current details of this schedule.
     *
     * @return the schedule description
     * @throws ScheduleNotFoundException if the schedule does not exist
     */
    public ScheduleDescription describe() {
        TypedEntityMetadata<ScheduleState> metadata =
                this.client.getEntities().getEntityMetadata(this.entityId, ScheduleState.class);
        ScheduleState state = metadata == null ? null : metadata.getState();
        if (state == null) {
            throw new ScheduleNotFoundException(this.scheduleId);
        }
        return ScheduleDescription.fromState(this.scheduleId, state);
    }

    private void runOperation(String operationName, Object input) {
        ScheduleOperationRequest request = new ScheduleOperationRequest(this.entityId, operationName, input);
        String instanceId = this.client.scheduleNewOrchestrationInstance(
                ExecuteScheduleOperationOrchestrator.NAME, request);
        OrchestrationMetadata result;
        try {
            result = this.client.waitForInstanceCompletion(instanceId, this.operationTimeout, true);
        } catch (TimeoutException e) {
            // A timeout is indeterminate: the backend operation may still have committed. Callers can query the
            // schedule to determine the final state.
            throw new IllegalStateException(
                    "Timed out waiting for operation '" + operationName + "' on schedule '" + this.scheduleId
                            + "' to complete. The operation result is indeterminate; query the schedule to check "
                            + "its state.", e);
        }

        if (result.getRuntimeStatus() != OrchestrationRuntimeStatus.COMPLETED) {
            FailureDetails failure = result.getFailureDetails();
            String detail = failure == null ? "unknown error" : failure.getErrorMessage();
            LOGGER.log(Level.FINE, "Operation {0} on schedule {1} failed: {2}",
                    new Object[] {operationName, this.scheduleId, detail});
            throw new IllegalStateException(
                    "Failed to '" + operationName + "' schedule '" + this.scheduleId + "': " + detail);
        }
    }
}
