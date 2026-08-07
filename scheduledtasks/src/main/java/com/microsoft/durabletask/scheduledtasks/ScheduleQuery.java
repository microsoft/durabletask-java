// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import javax.annotation.Nullable;
import java.time.OffsetDateTime;

/**
 * Query parameters for filtering schedules.
 * <p>
 * The schedule-ID prefix is applied by the backend; the status and creation-time filters are applied client-side to
 * each returned page, matching the .NET SDK. Both creation-time bounds are exclusive, so a returned page may contain
 * fewer than {@code pageSize} matches (or none) while still carrying a continuation token.
 */
public final class ScheduleQuery {

    /** The default page size when not specified. */
    public static final int DEFAULT_PAGE_SIZE = 100;

    private ScheduleStatus status;
    private String scheduleIdPrefix;
    private OffsetDateTime createdFrom;
    private OffsetDateTime createdTo;
    private Integer pageSize;
    private String continuationToken;

    /** Creates an empty {@code ScheduleQuery}. */
    public ScheduleQuery() {
    }

    /** @return the status filter, or {@code null}. */
    @Nullable
    public ScheduleStatus getStatus() {
        return this.status;
    }

    /**
     * Sets the status filter.
     *
     * @param status the status filter, or {@code null}
     * @return this query object for chaining
     */
    public ScheduleQuery setStatus(@Nullable ScheduleStatus status) {
        this.status = status;
        return this;
    }

    /** @return the schedule-ID prefix filter, or {@code null}. */
    @Nullable
    public String getScheduleIdPrefix() {
        return this.scheduleIdPrefix;
    }

    /**
     * Sets the schedule-ID prefix filter.
     *
     * @param scheduleIdPrefix the prefix, or {@code null}
     * @return this query object for chaining
     */
    public ScheduleQuery setScheduleIdPrefix(@Nullable String scheduleIdPrefix) {
        this.scheduleIdPrefix = scheduleIdPrefix;
        return this;
    }

    /** @return the exclusive lower creation-time bound, or {@code null}. */
    @Nullable
    public OffsetDateTime getCreatedFrom() {
        return this.createdFrom;
    }

    /**
     * Sets the exclusive lower creation-time bound.
     *
     * @param createdFrom the lower bound, or {@code null}
     * @return this query object for chaining
     */
    public ScheduleQuery setCreatedFrom(@Nullable OffsetDateTime createdFrom) {
        this.createdFrom = createdFrom;
        return this;
    }

    /** @return the exclusive upper creation-time bound, or {@code null}. */
    @Nullable
    public OffsetDateTime getCreatedTo() {
        return this.createdTo;
    }

    /**
     * Sets the exclusive upper creation-time bound.
     *
     * @param createdTo the upper bound, or {@code null}
     * @return this query object for chaining
     */
    public ScheduleQuery setCreatedTo(@Nullable OffsetDateTime createdTo) {
        this.createdTo = createdTo;
        return this;
    }

    /** @return the page size, or {@code null} to use {@link #DEFAULT_PAGE_SIZE}. */
    @Nullable
    public Integer getPageSize() {
        return this.pageSize;
    }

    /**
     * Sets the maximum number of entities fetched per backend page.
     *
     * @param pageSize the page size, or {@code null}
     * @return this query object for chaining
     */
    public ScheduleQuery setPageSize(@Nullable Integer pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    /** @return the continuation token for resuming a previous query, or {@code null}. */
    @Nullable
    public String getContinuationToken() {
        return this.continuationToken;
    }

    /**
     * Sets the continuation token for resuming a previous query.
     *
     * @param continuationToken the continuation token, or {@code null}
     * @return this query object for chaining
     */
    public ScheduleQuery setContinuationToken(@Nullable String continuationToken) {
        this.continuationToken = continuationToken;
        return this;
    }
}
