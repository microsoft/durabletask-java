// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.List;

public class OrchestrationStatusQuery {
    private final List<OrchestrationRuntimeStatus> runtimeStatusList;
    private final Instant createdTimeFrom;
    private final Instant createdTimeTo;
    private final List<String> taskHubNames;
    private final int maxInstanceCount;
    private final String continuationToken;
    private final String instanceIdPrefix;
    private final boolean fetchInputsAndOutputs;

    private OrchestrationStatusQuery(Builder builder){
        this.runtimeStatusList = builder.runtimeStatusList;
        this.createdTimeFrom = builder.createdTimeFrom;
        this.createdTimeTo = builder.createdTimeTo;
        this.taskHubNames = builder.taskHubNames;
        this.maxInstanceCount = builder.maxInstanceCount;
        this.continuationToken = builder.continuationToken;
        this.instanceIdPrefix = builder.instanceIdPrefix;
        this.fetchInputsAndOutputs = builder.fetchInputsAndOutputs;
    }

    @Nullable
    public List<OrchestrationRuntimeStatus> getRuntimeStatusList() {
        return runtimeStatusList;
    }

    @Nullable
    public Instant getCreatedTimeFrom() {
        return createdTimeFrom;
    }

    @Nullable
    public Instant getCreatedTimeTo() {
        return createdTimeTo;
    }

    public int getMaxInstanceCount() {
        return maxInstanceCount;
    }

    @Nullable
    public List<String> getTaskHubNames() {
        return taskHubNames;
    }

    @Nullable
    public String getContinuationToken() {
        return continuationToken;
    }

    @Nullable
    public String getInstanceIdPrefix() {
        return instanceIdPrefix;
    }

    public boolean isFetchInputsAndOutputs() {
        return fetchInputsAndOutputs;
    }

    public static Builder newBuild(){
        return new Builder();
    }

    public static class Builder {
        private List<OrchestrationRuntimeStatus> runtimeStatusList;
        private Instant createdTimeFrom;
        private Instant createdTimeTo;
        private List<String> taskHubNames;
        private int maxInstanceCount = 100;
        private String continuationToken;
        private String instanceIdPrefix;
        private boolean fetchInputsAndOutputs;

        private Builder(){}

        public OrchestrationStatusQuery build(){
            return new OrchestrationStatusQuery(this);
        }

        public Builder setRuntimeStatusList(@Nullable List<OrchestrationRuntimeStatus> runtimeStatusList) {
            this.runtimeStatusList = runtimeStatusList;
            return this;
        }

        public Builder setCreatedTimeFrom(@Nullable Instant createdTimeFrom) {
            this.createdTimeFrom = createdTimeFrom;
            return this;
        }

        public Builder setCreatedTimeTo(@Nullable Instant createdTimeTo) {
            this.createdTimeTo = createdTimeTo;
            return this;
        }

        public Builder setMaxInstanceCount(int maxInstanceCount) {
            this.maxInstanceCount = maxInstanceCount;
            return this;
        }

        public Builder setTaskHubNames(@Nullable List<String> taskHubNames) {
            this.taskHubNames = taskHubNames;
            return this;
        }

        public Builder setContinuationToken(@Nullable String continuationToken) {
            this.continuationToken = continuationToken;
            return this;
        }

        public Builder setInstanceIdPrefix(@Nullable String instanceIdPrefix) {
            this.instanceIdPrefix = instanceIdPrefix;
            return this;
        }

        public Builder setFetchInputsAndOutputs(boolean fetchInputsAndOutputs) {
            this.fetchInputsAndOutputs = fetchInputsAndOutputs;
            return this;
        }
    }
}
