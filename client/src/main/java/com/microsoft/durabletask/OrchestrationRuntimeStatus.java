// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import static com.microsoft.durabletask.implementation.protobuf.OrchestratorService.OrchestrationStatus.*;

/**
 * Enum describing the runtime status of the orchestration.
 */
public enum OrchestrationRuntimeStatus {
    /**
     * The orchestration started running.
     */
    RUNNING,

    /**
     * The orchestration completed normally.
     */
    COMPLETED,

    /**
     * The orchestration is transitioning into a new instance.
     * <p>
     * This status value is obsolete and exists only for compatibility reasons.
     */
    CONTINUED_AS_NEW,

    /**
     * The orchestration completed with an unhandled exception.
     */
    FAILED,

    /**
     * The orchestration canceled gracefully.
     * <p>
     * The Canceled status is not currently used and exists only for compatibility reasons.
     */
    CANCELED,

    /**
     * The orchestration was abruptly terminated via a management API call.
     */
    TERMINATED,

    /**
     * The orchestration was scheduled but hasn't started running.
     */
    PENDING;

    static OrchestrationRuntimeStatus fromProtobuf(OrchestrationStatus status) {
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

    static OrchestrationStatus toProtobuf(OrchestrationRuntimeStatus status){
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
