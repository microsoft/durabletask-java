// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.TaskOrchestration;
import com.microsoft.durabletask.TaskOrchestrationContext;

/**
 * Orchestrator that executes a single operation on an export job entity and returns its result.
 * <p>
 * The client schedules this orchestrator (rather than signaling the entity directly) so it can await completion and
 * surface validation errors.
 */
public final class ExecuteExportJobOperationOrchestrator implements TaskOrchestration {

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
