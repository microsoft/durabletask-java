// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import com.microsoft.durabletask.exporthistory.constants.ExportJobOperationNames;

/**
 * Manages valid state transitions for export jobs.
 */
public final class ExportJobTransitions {

    private ExportJobTransitions() {
    }

    /**
     * Checks if a transition to the target state is valid for a given operation.
     *
     * @param operationName the operation being performed
     * @param from          the current job status
     * @param targetStatus  the target status
     * @return {@code true} if the transition is valid
     */
    public static boolean isValidTransition(String operationName, ExportJobStatus from, ExportJobStatus targetStatus) {
        if (ExportJobOperationNames.CREATE.equals(operationName)) {
            return (from == ExportJobStatus.PENDING || from == ExportJobStatus.FAILED || from == ExportJobStatus.COMPLETED)
                    && targetStatus == ExportJobStatus.ACTIVE;
        }
        if (ExportJobOperationNames.MARK_AS_COMPLETED.equals(operationName)) {
            return from == ExportJobStatus.ACTIVE && targetStatus == ExportJobStatus.COMPLETED;
        }
        if (ExportJobOperationNames.MARK_AS_FAILED.equals(operationName)) {
            return from == ExportJobStatus.ACTIVE && targetStatus == ExportJobStatus.FAILED;
        }
        return false;
    }
}
