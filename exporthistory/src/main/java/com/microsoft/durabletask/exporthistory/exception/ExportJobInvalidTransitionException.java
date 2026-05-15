// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.exception;

import com.microsoft.durabletask.exporthistory.models.ExportJobStatus;

/**
 * Thrown when an invalid state transition is attempted on an export job.
 */
public class ExportJobInvalidTransitionException extends RuntimeException {

    private final String jobId;
    private final ExportJobStatus currentStatus;
    private final ExportJobStatus targetStatus;
    private final String operationName;

    public ExportJobInvalidTransitionException(
            String jobId,
            ExportJobStatus currentStatus,
            ExportJobStatus targetStatus,
            String operationName) {
        super("Invalid transition for export job '" + jobId + "': cannot transition from "
                + currentStatus + " to " + targetStatus + " via operation '" + operationName + "'.");
        this.jobId = jobId;
        this.currentStatus = currentStatus;
        this.targetStatus = targetStatus;
        this.operationName = operationName;
    }

    public String getJobId() { return this.jobId; }
    public ExportJobStatus getCurrentStatus() { return this.currentStatus; }
    public ExportJobStatus getTargetStatus() { return this.targetStatus; }
    public String getOperationName() { return this.operationName; }
}
