// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.OrchestrationRuntimeStatus;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.List;

/**
 * Input to {@link ListTerminalInstancesActivity}: a completion-time window, terminal status filter, pagination
 * cursor, and batch size.
 */
public final class ListTerminalInstancesRequest {

    private Instant completedTimeFrom;
    private Instant completedTimeTo;
    private List<OrchestrationRuntimeStatus> runtimeStatus;
    private String lastInstanceKey;
    private int maxInstancesPerBatch;

    /** Creates an empty {@code ListTerminalInstancesRequest} (for deserialization). */
    public ListTerminalInstancesRequest() {
    }

    /**
     * Creates a {@code ListTerminalInstancesRequest}.
     *
     * @param completedTimeFrom    the inclusive completion-time lower bound
     * @param completedTimeTo      the inclusive completion-time upper bound, or {@code null}
     * @param runtimeStatus        the terminal runtime statuses, or {@code null}
     * @param lastInstanceKey      the pagination cursor from the previous page, or {@code null}
     * @param maxInstancesPerBatch the maximum number of instance IDs per page
     */
    public ListTerminalInstancesRequest(
            Instant completedTimeFrom,
            @Nullable Instant completedTimeTo,
            @Nullable List<OrchestrationRuntimeStatus> runtimeStatus,
            @Nullable String lastInstanceKey,
            int maxInstancesPerBatch) {
        this.completedTimeFrom = completedTimeFrom;
        this.completedTimeTo = completedTimeTo;
        this.runtimeStatus = runtimeStatus;
        this.lastInstanceKey = lastInstanceKey;
        this.maxInstancesPerBatch = maxInstancesPerBatch;
    }

    /** @return the inclusive completion-time lower bound. */
    public Instant getCompletedTimeFrom() {
        return this.completedTimeFrom;
    }

    /**
     * Sets the inclusive completion-time lower bound.
     *
     * @param completedTimeFrom the lower bound
     */
    public void setCompletedTimeFrom(Instant completedTimeFrom) {
        this.completedTimeFrom = completedTimeFrom;
    }

    /** @return the inclusive completion-time upper bound, or {@code null}. */
    @Nullable
    public Instant getCompletedTimeTo() {
        return this.completedTimeTo;
    }

    /**
     * Sets the inclusive completion-time upper bound.
     *
     * @param completedTimeTo the upper bound, or {@code null}
     */
    public void setCompletedTimeTo(@Nullable Instant completedTimeTo) {
        this.completedTimeTo = completedTimeTo;
    }

    /** @return the terminal runtime statuses, or {@code null}. */
    @Nullable
    public List<OrchestrationRuntimeStatus> getRuntimeStatus() {
        return this.runtimeStatus;
    }

    /**
     * Sets the terminal runtime statuses.
     *
     * @param runtimeStatus the runtime statuses, or {@code null}
     */
    public void setRuntimeStatus(@Nullable List<OrchestrationRuntimeStatus> runtimeStatus) {
        this.runtimeStatus = runtimeStatus;
    }

    /** @return the pagination cursor from the previous page, or {@code null}. */
    @Nullable
    public String getLastInstanceKey() {
        return this.lastInstanceKey;
    }

    /**
     * Sets the pagination cursor.
     *
     * @param lastInstanceKey the cursor, or {@code null}
     */
    public void setLastInstanceKey(@Nullable String lastInstanceKey) {
        this.lastInstanceKey = lastInstanceKey;
    }

    /** @return the maximum number of instance IDs per page. */
    public int getMaxInstancesPerBatch() {
        return this.maxInstancesPerBatch;
    }

    /**
     * Sets the maximum number of instance IDs per page.
     *
     * @param maxInstancesPerBatch the batch size
     */
    public void setMaxInstancesPerBatch(int maxInstancesPerBatch) {
        this.maxInstancesPerBatch = maxInstancesPerBatch;
    }
}
