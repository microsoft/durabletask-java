// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Class used for constructing orchestration instance purge selection criteria.
 */
public final class PurgeInstanceCriteria {

    private Instant createdTimeFrom;
    private Instant createdTimeTo;
    private List<OrchestrationRuntimeStatus> runtimeStatusList = new ArrayList<>();

    /**
     * Sole constructor.
     */
    public PurgeInstanceCriteria() {
    }

    /**
     * Purge orchestration instances that were created after the specified instant.
     *
     * @param createdTimeFrom the minimum orchestration creation time to use as a selection criteria or {@code null} to
     *                        disable this selection criteria
     * @return this criteria object
     */
    public PurgeInstanceCriteria setCreatedTimeFrom(Instant createdTimeFrom) {
        this.createdTimeFrom = createdTimeFrom;
        return this;
    }

    /**
     * Purge orchestration instances that were created before the specified instant.
     *
     * @param createdTimeTo the maximum orchestration creation time to use as a selection criteria or {@code null} to
     *                      disable this selection criteria
     * @return this criteria object
     */
    public PurgeInstanceCriteria setCreatedTimeTo(Instant createdTimeTo) {
        this.createdTimeTo = createdTimeTo;
        return this;
    }

    /**
     * Sets the list of runtime status values to use as a selection criteria. Only orchestration instances that have a
     * matching runtime status will be purged. An empty list is the same as selecting for all runtime status values.
     *
     * @param runtimeStatusList the list of runtime status values to use as a selection criteria
     * @return this criteria object
     */
    public PurgeInstanceCriteria setRuntimeStatusList(List<OrchestrationRuntimeStatus> runtimeStatusList) {
        this.runtimeStatusList = runtimeStatusList;
        return this;
    }

    /**
     * Gets the configured minimum orchestration creation time or {@code null} if none was configured.
     * @return the configured minimum orchestration creation time or {@code null} if none was configured
     */
    @Nullable
    public Instant getCreatedTimeFrom() {
        return this.createdTimeFrom;
    }

    /**
     * Gets the configured maximum orchestration creation time or {@code null} if none was configured.
     * @return the configured maximum orchestration creation time or {@code null} if none was configured
     */
    @Nullable
    public Instant getCreatedTimeTo() {
        return this.createdTimeTo;
    }

    /**
     * Gets the configured runtime status selection criteria.
     * @return the configured runtime status filter as a list of values
     */
    public List<OrchestrationRuntimeStatus> getRuntimeStatusList() {
        return this.runtimeStatusList;
    }
}