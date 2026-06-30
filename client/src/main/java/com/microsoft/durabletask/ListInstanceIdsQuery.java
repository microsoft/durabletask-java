// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
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
        this.runtimeStatusList = runtimeStatusList;
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
     * @param pageSize the maximum number of instance IDs to return per page
     * @return this query object
     */
    public ListInstanceIdsQuery setPageSize(int pageSize) {
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

    List<OrchestrationRuntimeStatus> getRuntimeStatusList() {
        return this.runtimeStatusList;
    }

    @Nullable
    Instant getCompletedTimeFrom() {
        return this.completedTimeFrom;
    }

    @Nullable
    Instant getCompletedTimeTo() {
        return this.completedTimeTo;
    }

    int getPageSize() {
        return this.pageSize;
    }

    @Nullable
    String getContinuationToken() {
        return this.continuationToken;
    }
}
