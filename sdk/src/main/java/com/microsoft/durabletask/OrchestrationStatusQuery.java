package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.List;

public class OrchestrationStatusQuery {
    private List<OrchestrationRuntimeStatus> runtimeStatusList;
    private Instant createdTimeFrom;
    private Instant createdTimeTo;
    private List<String> taskHubNames;
    private int maxInstanceCount = 100;
    private String continuationToken;
    private String instanceIdPrefix;
    private boolean fetchInputsAndOutputs;

    @Nullable
    public List<OrchestrationRuntimeStatus> getRuntimeStatusList() {
        return runtimeStatusList;
    }

    public void setRuntimeStatusList(List<OrchestrationRuntimeStatus> runtimeStatusList) {
        this.runtimeStatusList = runtimeStatusList;
    }

    @Nullable
    public Instant getCreatedTimeFrom() {
        return createdTimeFrom;
    }

    public void setCreatedTimeFrom(Instant createdTimeFrom) {
        this.createdTimeFrom = createdTimeFrom;
    }

    @Nullable
    public Instant getCreatedTimeTo() {
        return createdTimeTo;
    }

    public void setCreatedTimeTo(Instant createdTimeTo) {
        this.createdTimeTo = createdTimeTo;
    }

    public int getMaxInstanceCount() {
        return maxInstanceCount;
    }

    public void setMaxInstanceCount(int maxInstanceCount) {
        this.maxInstanceCount = maxInstanceCount;
    }

    @Nullable
    public List<String> getTaskHubNames() {
        return taskHubNames;
    }

    public void setTaskHubNames(List<String> taskHubNames) {
        this.taskHubNames = taskHubNames;
    }

    @Nullable
    public String getContinuationToken() {
        return continuationToken;
    }

    public void setContinuationToken(String continuationToken) {
        this.continuationToken = continuationToken;
    }

    @Nullable
    public String getInstanceIdPrefix() {
        return instanceIdPrefix;
    }

    public void setInstanceIdPrefix(String instanceIdPrefix) {
        this.instanceIdPrefix = instanceIdPrefix;
    }

    public boolean isFetchInputsAndOutputs() {
        return fetchInputsAndOutputs;
    }

    public void setFetchInputsAndOutputs(boolean fetchInputsAndOutputs) {
        this.fetchInputsAndOutputs = fetchInputsAndOutputs;
    }
}
