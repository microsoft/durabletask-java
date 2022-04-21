// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.List;

public class PurgeInstanceCriteria {

    private final Instant createdTimeFrom;
    private final Instant createdTimeTo;
    private final List<OrchestrationRuntimeStatus> runtimeStatusList;

    public PurgeInstanceCriteria(Builder builder) {
        this.createdTimeFrom = builder.createdTimeFrom;
        this.createdTimeTo = builder.createdTimeTo;
        this.runtimeStatusList = builder.runtimeStatusList;
    }

    public Instant getCreatedTimeFrom() {
        return this.createdTimeFrom;
    }

    @Nullable
    public Instant getCreatedTimeTo() {
        return this.createdTimeTo;
    }

    @Nullable
    public List<OrchestrationRuntimeStatus> getRuntimeStatusList() {
        return this.runtimeStatusList;
    }

    public static PurgeInstanceCriteria.Builder newBuild(){
        return new PurgeInstanceCriteria.Builder();
    }

    public static class Builder {
        private Instant createdTimeFrom;
        private Instant createdTimeTo;
        private List<OrchestrationRuntimeStatus> runtimeStatusList;

        private Builder(){
        }

        public PurgeInstanceCriteria build(){
            return new PurgeInstanceCriteria(this);
        }

        public PurgeInstanceCriteria.Builder setCreatedTimeFrom(Instant createdTimeFrom) {
            this.createdTimeFrom = createdTimeFrom;
            return this;
        }

        @Nullable
        public PurgeInstanceCriteria.Builder setCreatedTimeTo(Instant createdTimeTo) {
            this.createdTimeTo = createdTimeTo;
            return this;
        }

        @Nullable
        public PurgeInstanceCriteria.Builder setRuntimeStatusList(List<OrchestrationRuntimeStatus> runtimeStatusList) {
            this.runtimeStatusList = runtimeStatusList;
            return this;
        }
    }
}