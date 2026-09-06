// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import com.microsoft.durabletask.EntityInstanceId;
import com.microsoft.durabletask.TaskOrchestration;
import com.microsoft.durabletask.TaskOrchestrationContext;

import javax.annotation.Nullable;

/**
 * Orchestrator that executes a single operation on a schedule entity and returns its result.
 * <p>
 * Mutating client operations schedule this orchestrator (rather than signaling the entity directly) so they can
 * await completion and surface validation and transition failures to the caller.
 */
final class ExecuteScheduleOperationOrchestrator implements TaskOrchestration {

    /** The registered orchestration name. Matches the .NET operation orchestrator name. */
    public static final String NAME = "ExecuteScheduleOperationOrchestrator";

    @Override
    public void run(TaskOrchestrationContext ctx) {
        ScheduleOperationRequest input = ctx.getInput(ScheduleOperationRequest.class);
        Object result = ctx.getEntities()
                .callEntity(input.getEntityId(), input.getOperationName(), input.getInput(), Object.class)
                .await();
        ctx.complete(result);
    }
}

/**
 * Request to execute a single operation on a schedule entity, scheduled by the client through
 * {@link ExecuteScheduleOperationOrchestrator} so the caller can await completion and surface failures.
 */
final class ScheduleOperationRequest {

    private EntityInstanceId entityId;
    private String operationName;
    private Object input;

    /** Creates an empty {@code ScheduleOperationRequest} (for deserialization). */
    public ScheduleOperationRequest() {
    }

    ScheduleOperationRequest(EntityInstanceId entityId, String operationName, @Nullable Object input) {
        this.entityId = entityId;
        this.operationName = operationName;
        this.input = input;
    }

    public EntityInstanceId getEntityId() {
        return this.entityId;
    }

    public void setEntityId(EntityInstanceId entityId) {
        this.entityId = entityId;
    }

    public String getOperationName() {
        return this.operationName;
    }

    public void setOperationName(String operationName) {
        this.operationName = operationName;
    }

    @Nullable
    public Object getInput() {
        return this.input;
    }

    public void setInput(@Nullable Object input) {
        this.input = input;
    }
}
