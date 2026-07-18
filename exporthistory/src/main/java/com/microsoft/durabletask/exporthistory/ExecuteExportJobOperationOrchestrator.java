// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.EntityInstanceId;
import com.microsoft.durabletask.TaskOrchestration;
import com.microsoft.durabletask.TaskOrchestrationContext;

import javax.annotation.Nullable;

/**
 * Orchestrator that executes a single operation on an export job entity and returns its result.
 * <p>
 * The client schedules this orchestrator (rather than signaling the entity directly) so it can await completion and
 * surface validation errors.
 */
final class ExecuteExportJobOperationOrchestrator implements TaskOrchestration {

    /** The registered orchestration name. */
    public static final String NAME = "ExecuteExportJobOperationOrchestrator";

    @Override
    public void run(TaskOrchestrationContext ctx) {
        ExportJobOperationRequest input = ctx.getInput(ExportJobOperationRequest.class);
        Object result = ctx.getEntities()
                .callEntity(input.getEntityId(), input.getOperationName(), input.getInput(), Object.class)
                .await();
        ctx.complete(result);
    }
}

/**
 * Request to execute a single operation on an export job entity, scheduled by the client through
 * {@link ExecuteExportJobOperationOrchestrator} so the caller can await completion and surface validation errors.
 */
final class ExportJobOperationRequest {

    private EntityInstanceId entityId;
    private String operationName;
    private Object input;

    /** Creates an empty {@code ExportJobOperationRequest} (for deserialization). */
    public ExportJobOperationRequest() {
    }

    /**
     * Creates an {@code ExportJobOperationRequest}.
     *
     * @param entityId      the target entity ID
     * @param operationName the operation name
     * @param input         the operation input, or {@code null}
     */
    public ExportJobOperationRequest(EntityInstanceId entityId, String operationName, @Nullable Object input) {
        this.entityId = entityId;
        this.operationName = operationName;
        this.input = input;
    }

    /** @return the target entity ID. */
    public EntityInstanceId getEntityId() {
        return this.entityId;
    }

    /**
     * Sets the target entity ID.
     *
     * @param entityId the entity ID
     */
    public void setEntityId(EntityInstanceId entityId) {
        this.entityId = entityId;
    }

    /** @return the operation name. */
    public String getOperationName() {
        return this.operationName;
    }

    /**
     * Sets the operation name.
     *
     * @param operationName the operation name
     */
    public void setOperationName(String operationName) {
        this.operationName = operationName;
    }

    /** @return the operation input, or {@code null}. */
    @Nullable
    public Object getInput() {
        return this.input;
    }

    /**
     * Sets the operation input.
     *
     * @param input the input, or {@code null}
     */
    public void setInput(@Nullable Object input) {
        this.input = input;
    }
}
