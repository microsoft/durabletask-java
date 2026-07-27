// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Query for listing terminal orchestration instance IDs by completion-time window.
 * <p>
 * Used with {@link DurableTaskClient#listInstanceIds(ListInstanceIdsQuery)} to enumerate the IDs of orchestration
 * instances that reached a terminal state within a completion-time window. Unlike {@link OrchestrationStatusQuery}
 * (which filters by creation time and returns full metadata), this query filters by <em>completion</em> time and
 * returns only instance IDs, making it efficient for bulk enumeration such as archival/export.
 */
public final class ListInstanceIdsQuery {
    private List<OrchestrationRuntimeStatus> runtimeStatusList = new ArrayList<>();
    private Instant completedTimeFrom;
    private Instant completedTimeTo;
    private int pageSize = 100;
    private String continuationToken;

    /**
     * Sole constructor.
     */
    public ListInstanceIdsQuery() {
    }

    /**
     * Sets the terminal runtime status values to filter by. Only instances with a matching status are returned.
     * The default empty list disables runtime-status filtering.
     *
     * @param runtimeStatusList the terminal runtime statuses to filter by (e.g. COMPLETED, FAILED, TERMINATED)
     * @return this query object
     */
    public ListInstanceIdsQuery setRuntimeStatusList(@Nullable List<OrchestrationRuntimeStatus> runtimeStatusList) {
        this.runtimeStatusList = runtimeStatusList != null ? new ArrayList<>(runtimeStatusList) : new ArrayList<>();
        return this;
    }

    /**
     * Includes instances that completed at or after the specified instant.
     *
     * @param completedTimeFrom the minimum completion time, or {@code null} to disable this filter
     * @return this query object
     */
    public ListInstanceIdsQuery setCompletedTimeFrom(@Nullable Instant completedTimeFrom) {
        this.completedTimeFrom = completedTimeFrom;
        return this;
    }

    /**
     * Includes instances that completed before the specified instant.
     *
     * @param completedTimeTo the maximum completion time, or {@code null} to disable this filter
     * @return this query object
     */
    public ListInstanceIdsQuery setCompletedTimeTo(@Nullable Instant completedTimeTo) {
        this.completedTimeTo = completedTimeTo;
        return this;
    }

    /**
     * Sets the maximum number of instance IDs to return per page. The default value is 100.
     * <p>
     * A page may contain fewer IDs than the page size even when more results exist; always use
     * {@link ListInstanceIdsResult#getContinuationToken()} to determine whether to continue paging.
     *
     * @param pageSize the maximum number of instance IDs to return per page; must be greater than zero
     * @return this query object
     * @throws IllegalArgumentException if {@code pageSize} is less than 1
     */
    public ListInstanceIdsQuery setPageSize(int pageSize) {
        if (pageSize < 1) {
            throw new IllegalArgumentException("pageSize must be at least 1.");
        }
        this.pageSize = pageSize;
        return this;
    }

    /**
     * Sets the pagination cursor used to continue listing from a previous page.
     * <p>
     * This should be the {@link ListInstanceIdsResult#getContinuationToken()} value from the previous page.
     *
     * @param continuationToken the cursor from the previous page, or {@code null} to start from the beginning
     * @return this query object
     */
    public ListInstanceIdsQuery setContinuationToken(@Nullable String continuationToken) {
        this.continuationToken = continuationToken;
        return this;
    }

    /**
     * Gets the configured terminal runtime status filter, or an empty list if none was configured.
     * @return an unmodifiable view of the configured terminal runtime status filter
     */
    public List<OrchestrationRuntimeStatus> getRuntimeStatusList() {
        return Collections.unmodifiableList(this.runtimeStatusList);
    }

    /**
     * Gets the configured minimum completion time, or {@code null} if none was configured.
     * @return the configured minimum completion time, or {@code null} if none was configured
     */
    @Nullable
    public Instant getCompletedTimeFrom() {
        return this.completedTimeFrom;
    }

    /**
     * Gets the configured maximum completion time, or {@code null} if none was configured.
     * @return the configured maximum completion time, or {@code null} if none was configured
     */
    @Nullable
    public Instant getCompletedTimeTo() {
        return this.completedTimeTo;
    }

    /**
     * Gets the configured maximum number of instance IDs to return per page.
     * @return the configured page size
     */
    public int getPageSize() {
        return this.pageSize;
    }

    /**
     * Gets the configured pagination cursor, or {@code null} if none was configured.
     * @return the configured pagination cursor, or {@code null} if none was configured
     */
    @Nullable
    public String getContinuationToken() {
        return this.continuationToken;
    }
}
