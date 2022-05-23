// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class PurgeInstanceCriteria {

    private Instant createdTimeFrom;
    private Instant createdTimeTo;
    private List<OrchestrationRuntimeStatus> runtimeStatusList = new ArrayList<>();

    public PurgeInstanceCriteria() {
    }

    public PurgeInstanceCriteria setCreatedTimeFrom(Instant createdTimeFrom) {
        this.createdTimeFrom = createdTimeFrom;
        return this;
    }

    @Nullable
    public PurgeInstanceCriteria setCreatedTimeTo(Instant createdTimeTo) {
        this.createdTimeTo = createdTimeTo;
        return this;
    }

    @Nullable
    public PurgeInstanceCriteria setRuntimeStatusList(List<OrchestrationRuntimeStatus> runtimeStatusList) {
        this.runtimeStatusList = runtimeStatusList;
        return this;
    }

    public Instant getCreatedTimeFrom() {
        return this.createdTimeFrom;
    }

    @Nullable
    public Instant getCreatedTimeTo() {
        return this.createdTimeTo;
    }

    public List<OrchestrationRuntimeStatus> getRuntimeStatusList() {
        return this.runtimeStatusList;
    }
}