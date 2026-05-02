// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.exception;

/**
 * Thrown when an export job with the specified ID is not found.
 */
public class ExportJobNotFoundException extends RuntimeException {

    private final String jobId;

    public ExportJobNotFoundException(String jobId) {
        super("Export job '" + jobId + "' was not found.");
        this.jobId = jobId;
    }

    public String getJobId() {
        return this.jobId;
    }
}
