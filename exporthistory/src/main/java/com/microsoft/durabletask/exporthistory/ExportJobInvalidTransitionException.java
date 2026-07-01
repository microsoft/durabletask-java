// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

/**
 * Thrown when an export job operation attempts an invalid status transition.
 */
public final class ExportJobInvalidTransitionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String jobId;
    private final ExportJobStatus fromStatus;
    private final ExportJobStatus toStatus;
    private final String operationName;

    /**
     * Creates a new {@code ExportJobInvalidTransitionException}.
     *
     * @param jobId         the export job ID
     * @param fromStatus    the current status
     * @param toStatus      the attempted target status
     * @param operationName the operation that attempted the transition
     */
    public ExportJobInvalidTransitionException(
            String jobId,
            ExportJobStatus fromStatus,
            ExportJobStatus toStatus,
            String operationName) {
        super("Export job '" + jobId + "' cannot transition from " + fromStatus + " to " + toStatus
                + " via operation '" + operationName + "'.");
        this.jobId = jobId;
        this.fromStatus = fromStatus;
        this.toStatus = toStatus;
        this.operationName = operationName;
    }

    /** @return the export job ID. */
    public String getJobId() {
        return this.jobId;
    }

    /** @return the current status. */
    public ExportJobStatus getFromStatus() {
        return this.fromStatus;
    }

    /** @return the attempted target status. */
    public ExportJobStatus getToStatus() {
        return this.toStatus;
    }

    /** @return the operation that attempted the transition. */
    public String getOperationName() {
        return this.operationName;
    }
}
