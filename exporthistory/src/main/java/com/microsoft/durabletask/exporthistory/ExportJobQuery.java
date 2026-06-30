// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * Query parameters for filtering export history jobs via {@link ExportHistoryClient#listJobs(ExportJobQuery)}.
 * <p>
 * Mirrors the .NET {@code Microsoft.DurableTask.ExportHistory.ExportJobQuery}.
 */
public final class ExportJobQuery {

    /** The default page size when not supplied. */
    public static final int DEFAULT_PAGE_SIZE = 100;

    private ExportJobStatus status;
    private String jobIdPrefix;
    private Instant createdFrom;
    private Instant createdTo;
    private Integer pageSize;
    private String continuationToken;

    /** Creates an empty {@code ExportJobQuery}. */
    public ExportJobQuery() {
    }

    /** @return the status filter, or {@code null}. */
    @Nullable
    public ExportJobStatus getStatus() {
        return this.status;
    }

    /**
     * Sets the status filter.
     *
     * @param status the status filter, or {@code null}
     * @return this query object
     */
    public ExportJobQuery setStatus(@Nullable ExportJobStatus status) {
        this.status = status;
        return this;
    }

    /** @return the job-ID prefix filter, or {@code null}. */
    @Nullable
    public String getJobIdPrefix() {
        return this.jobIdPrefix;
    }

    /**
     * Sets the job-ID prefix filter.
     *
     * @param jobIdPrefix the prefix, or {@code null}
     * @return this query object
     */
    public ExportJobQuery setJobIdPrefix(@Nullable String jobIdPrefix) {
        this.jobIdPrefix = jobIdPrefix;
        return this;
    }

    /** @return the created-after filter, or {@code null}. */
    @Nullable
    public Instant getCreatedFrom() {
        return this.createdFrom;
    }

    /**
     * Sets the created-after filter.
     *
     * @param createdFrom the lower bound, or {@code null}
     * @return this query object
     */
    public ExportJobQuery setCreatedFrom(@Nullable Instant createdFrom) {
        this.createdFrom = createdFrom;
        return this;
    }

    /** @return the created-before filter, or {@code null}. */
    @Nullable
    public Instant getCreatedTo() {
        return this.createdTo;
    }

    /**
     * Sets the created-before filter.
     *
     * @param createdTo the upper bound, or {@code null}
     * @return this query object
     */
    public ExportJobQuery setCreatedTo(@Nullable Instant createdTo) {
        this.createdTo = createdTo;
        return this;
    }

    /** @return the page size, or {@code null} for the default. */
    @Nullable
    public Integer getPageSize() {
        return this.pageSize;
    }

    /**
     * Sets the maximum number of jobs to return per page.
     *
     * @param pageSize the page size, or {@code null} for the default
     * @return this query object
     */
    public ExportJobQuery setPageSize(@Nullable Integer pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    /** @return the continuation token, or {@code null}. */
    @Nullable
    public String getContinuationToken() {
        return this.continuationToken;
    }

    /**
     * Sets the continuation token for retrieving the next page.
     *
     * @param continuationToken the token, or {@code null}
     * @return this query object
     */
    public ExportJobQuery setContinuationToken(@Nullable String continuationToken) {
        this.continuationToken = continuationToken;
        return this;
    }
}
