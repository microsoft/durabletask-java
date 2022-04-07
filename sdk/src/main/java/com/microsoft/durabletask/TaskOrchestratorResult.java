package com.microsoft.durabletask;

import com.microsoft.durabletask.protobuf.OrchestratorService;

import java.util.Collection;

public class TaskOrchestratorResult {

    private Collection<OrchestratorService.OrchestratorAction> actions;

    private String customStatus;

    public TaskOrchestratorResult(Collection<OrchestratorService.OrchestratorAction> actions, String customStatus) {
        this.actions = actions;
        this.customStatus = customStatus;
    }

    public Collection<OrchestratorService.OrchestratorAction> getActions() {
        return actions;
    }

    public void setActions(Collection<OrchestratorService.OrchestratorAction> actions) {
        this.actions = actions;
    }

    public String getCustomStatus() {
        return customStatus;
    }

    public void setCustomStatus(String customStatus) {
        this.customStatus = customStatus;
    }
}
