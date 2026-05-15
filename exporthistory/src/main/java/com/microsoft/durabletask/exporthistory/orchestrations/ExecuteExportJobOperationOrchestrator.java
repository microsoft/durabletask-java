// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.orchestrations;

import com.microsoft.durabletask.TaskOrchestration;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.exporthistory.models.ExportJobOperationRequest;

/**
 * Orchestrator that executes operations on export job entities.
 * Routes the operation call to the target entity and returns the result.
 */
public class ExecuteExportJobOperationOrchestrator implements TaskOrchestration {

    @Override
    public void run(TaskOrchestrationContext ctx) {
        ExportJobOperationRequest input = ctx.getInput(ExportJobOperationRequest.class);
        if (input == null || input.getEntityId() == null || input.getOperationName() == null) {
            throw new IllegalArgumentException("ExportJobOperationRequest with entityId and operationName is required.");
        }

        Object result = ctx.callEntity(
                input.getEntityId(),
                input.getOperationName(),
                input.getInput(),
                Object.class).await();

        ctx.complete(result);
    }
}
