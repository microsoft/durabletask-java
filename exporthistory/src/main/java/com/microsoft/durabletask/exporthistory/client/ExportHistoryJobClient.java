// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.client;

import com.microsoft.durabletask.exporthistory.models.ExportJobCreationOptions;
import com.microsoft.durabletask.exporthistory.models.ExportJobDescription;

import javax.annotation.Nonnull;

/**
 * Per-job client for managing an individual export job.
 */
public abstract class ExportHistoryJobClient {

    protected final String jobId;

    protected ExportHistoryJobClient(@Nonnull String jobId) {
        if (jobId == null || jobId.isEmpty()) {
            throw new IllegalArgumentException("jobId must not be null or empty.");
        }
        this.jobId = jobId;
    }

    /**
     * Creates the export job with the specified options.
     *
     * @param options the job creation options
     */
    public abstract void create(@Nonnull ExportJobCreationOptions options);

    /**
     * Describes the current state and progress of the export job.
     *
     * @return the job description
     */
    @Nonnull
    public abstract ExportJobDescription describe();

    /**
     * Deletes the export job and terminates its orchestration.
     */
    public abstract void delete();
}
