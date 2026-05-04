// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * Query filter for listing export jobs.
 */
public final class ExportJobQuery {

    public static final int DEFAULT_PAGE_SIZE = 100;

    private ExportJobStatus status;
    private String jobIdPrefix;
    private Instant createdFrom;
    private Instant createdTo;
    private Integer pageSize;
    private String continuationToken;

    public ExportJobQuery() {
    }

    public ExportJobQuery(
            @Nullable ExportJobStatus status,
            @Nullable String jobIdPrefix,
            @Nullable Instant createdFrom,
            @Nullable Instant createdTo,
            @Nullable Integer pageSize,
            @Nullable String continuationToken) {
        this.status = status;
        this.jobIdPrefix = jobIdPrefix;
        this.createdFrom = createdFrom;
        this.createdTo = createdTo;
        this.pageSize = pageSize;
        this.continuationToken = continuationToken;
    }

    @Nullable public ExportJobStatus getStatus() { return this.status; }
    @Nullable public String getJobIdPrefix() { return this.jobIdPrefix; }
    @Nullable public Instant getCreatedFrom() { return this.createdFrom; }
    @Nullable public Instant getCreatedTo() { return this.createdTo; }
    @Nullable public Integer getPageSize() { return this.pageSize; }
    @Nullable public String getContinuationToken() { return this.continuationToken; }

    public void setStatus(ExportJobStatus status) { this.status = status; }
    public void setJobIdPrefix(String jobIdPrefix) { this.jobIdPrefix = jobIdPrefix; }
    public void setCreatedFrom(Instant createdFrom) { this.createdFrom = createdFrom; }
    public void setCreatedTo(Instant createdTo) { this.createdTo = createdTo; }
    public void setPageSize(Integer pageSize) { this.pageSize = pageSize; }
    public void setContinuationToken(String continuationToken) { this.continuationToken = continuationToken; }
}
