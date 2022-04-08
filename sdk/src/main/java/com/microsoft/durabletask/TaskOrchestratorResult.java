package com.microsoft.durabletask;

import com.google.common.collect.ImmutableList;
import com.microsoft.durabletask.protobuf.OrchestratorService;

import java.util.Collection;

public final class TaskOrchestratorResult {

    private final ImmutableList<OrchestratorService.OrchestratorAction> actions;

    private final String customStatus;

    public TaskOrchestratorResult(Collection<OrchestratorService.OrchestratorAction> actions, String customStatus) {
        this.actions = ImmutableList.<OrchestratorService.OrchestratorAction>builder().addAll(actions).build();
        this.customStatus = customStatus;
    }

    public Collection<OrchestratorService.OrchestratorAction> getActions() {
        return this.actions;
    }

    public String getCustomStatus() {
        return this.customStatus;
    }
}
