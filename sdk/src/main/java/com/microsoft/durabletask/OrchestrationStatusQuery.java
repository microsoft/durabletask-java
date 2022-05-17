// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.List;

public class OrchestrationStatusQuery {
    private List<OrchestrationRuntimeStatus> runtimeStatusList;
    private Instant createdTimeFrom;
    private Instant createdTimeTo;
    private List<String> taskHubNames;
    private int maxInstanceCount;
    private String continuationToken;
    private String instanceIdPrefix;
    private boolean fetchInputsAndOutputs;

    public OrchestrationStatusQuery() {
    }

    public OrchestrationStatusQuery setRuntimeStatusList(@Nullable List<OrchestrationRuntimeStatus> runtimeStatusList) {
        this.runtimeStatusList = runtimeStatusList;
        return this;
    }

    public OrchestrationStatusQuery setCreatedTimeFrom(@Nullable Instant createdTimeFrom) {
        this.createdTimeFrom = createdTimeFrom;
        return this;
    }

    public OrchestrationStatusQuery setCreatedTimeTo(@Nullable Instant createdTimeTo) {
        this.createdTimeTo = createdTimeTo;
        return this;
    }

    public OrchestrationStatusQuery setMaxInstanceCount(int maxInstanceCount) {
        this.maxInstanceCount = maxInstanceCount;
        return this;
    }

    public OrchestrationStatusQuery setTaskHubNames(@Nullable List<String> taskHubNames) {
        this.taskHubNames = taskHubNames;
        return this;
    }

    public OrchestrationStatusQuery setContinuationToken(@Nullable String continuationToken) {
        this.continuationToken = continuationToken;
        return this;
    }

    public OrchestrationStatusQuery setInstanceIdPrefix(@Nullable String instanceIdPrefix) {
        this.instanceIdPrefix = instanceIdPrefix;
        return this;
    }

    public OrchestrationStatusQuery setFetchInputsAndOutputs(boolean fetchInputsAndOutputs) {
        this.fetchInputsAndOutputs = fetchInputsAndOutputs;
        return this;
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
}
