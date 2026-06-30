// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

/**
 * Thrown when an export job cannot be found.
 * <p>
 * Mirrors the .NET {@code Microsoft.DurableTask.ExportHistory.ExportJobNotFoundException}.
 */
public final class ExportJobNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String jobId;

    /**
     * Creates a new {@code ExportJobNotFoundException}.
     *
     * @param jobId the export job ID that was not found
     */
    public ExportJobNotFoundException(String jobId) {
        super("Export job '" + jobId + "' was not found.");
        this.jobId = jobId;
    }

    /** @return the export job ID that was not found. */
    public String getJobId() {
        return this.jobId;
    }
}
