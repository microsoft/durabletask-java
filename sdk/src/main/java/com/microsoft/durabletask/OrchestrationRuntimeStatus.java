// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.microsoft.durabletask.protobuf.OrchestratorService.*;
import static com.microsoft.durabletask.protobuf.OrchestratorService.OrchestrationStatus.*;

public enum OrchestrationRuntimeStatus {
    RUNNING,
    COMPLETED,
    CONTINUED_AS_NEW,
    FAILED,
    CANCELED,
    TERMINATED,
    PENDING;

    public static OrchestrationRuntimeStatus fromProtobuf(OrchestrationStatus status) {
        switch (status) {
            case ORCHESTRATION_STATUS_RUNNING:
                return RUNNING;
            case ORCHESTRATION_STATUS_COMPLETED:
                return COMPLETED;
            case ORCHESTRATION_STATUS_CONTINUED_AS_NEW:
                return CONTINUED_AS_NEW;
            case ORCHESTRATION_STATUS_FAILED:
                return FAILED;
            case ORCHESTRATION_STATUS_CANCELED:
                return CANCELED;
            case ORCHESTRATION_STATUS_TERMINATED:
                return TERMINATED;
            case ORCHESTRATION_STATUS_PENDING:
                return PENDING;
            default:
                throw new IllegalArgumentException(String.format("Unknown status value: %s", status));
        }
    }

    public static OrchestrationStatus toProtobuf(OrchestrationRuntimeStatus status){
        switch (status) {
            case RUNNING:
                return ORCHESTRATION_STATUS_RUNNING;
            case COMPLETED:
                return ORCHESTRATION_STATUS_COMPLETED;
            case CONTINUED_AS_NEW:
                return ORCHESTRATION_STATUS_CONTINUED_AS_NEW;
            case FAILED:
                return ORCHESTRATION_STATUS_FAILED;
            case CANCELED:
                return ORCHESTRATION_STATUS_CANCELED;
            case TERMINATED:
                return ORCHESTRATION_STATUS_TERMINATED;
            case PENDING:
                return ORCHESTRATION_STATUS_PENDING;
            default:
                throw new IllegalArgumentException(String.format("Unknown status value: %s", status));
        }
    }
}
