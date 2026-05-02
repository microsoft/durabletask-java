// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.client;

import com.microsoft.durabletask.exporthistory.models.ExportJobCreationOptions;
import com.microsoft.durabletask.exporthistory.models.ExportJobDescription;
import com.microsoft.durabletask.exporthistory.models.ExportJobQuery;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Client for managing export history jobs.
 */
public abstract class ExportHistoryClient {

    /**
     * Creates a new export job.
     *
     * @param options the options for the export job
     * @return a job client for the created job
     */
    @Nonnull
    public abstract ExportHistoryJobClient createJob(@Nonnull ExportJobCreationOptions options);

    /**
     * Gets the description of an export job.
     *
     * @param jobId the job ID
     * @return the job description
     */
    @Nonnull
    public abstract ExportJobDescription getJob(@Nonnull String jobId);

    /**
     * Lists export jobs matching the specified filter.
     *
     * @param filter the query filter, or {@code null} for all jobs
     * @return an auto-paginating iterable of job descriptions
     */
    @Nonnull
    public abstract ExportJobQueryPageable listJobs(@Nullable ExportJobQuery filter);

    /**
     * Gets a job client for an existing job.
     *
     * @param jobId the job ID
     * @return the job client
     */
    @Nonnull
    public abstract ExportHistoryJobClient getJobClient(@Nonnull String jobId);
}
