// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.orchestrations;

import com.microsoft.durabletask.TaskOrchestration;
import com.microsoft.durabletask.TaskOrchestrationFactory;

/**
 * Factory for {@link ExecuteExportJobOperationOrchestrator}.
 */
public class ExecuteExportJobOperationOrchestratorFactory implements TaskOrchestrationFactory {

    @Override
    public String getName() {
        return "ExecuteExportJobOperationOrchestrator";
    }

    @Override
    public TaskOrchestration create() {
        return new ExecuteExportJobOperationOrchestrator();
    }
}
