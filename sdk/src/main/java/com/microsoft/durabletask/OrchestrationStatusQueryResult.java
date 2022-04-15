package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.util.List;

public class OrchestrationStatusQueryResult {
    public List<OrchestrationMetadata> OrchestrationState;
    public String continuationToken;

    public List<OrchestrationMetadata> getOrchestrationState() {
        return OrchestrationState;
    }

    public void setOrchestrationState(List<OrchestrationMetadata> orchestrationState) {
        OrchestrationState = orchestrationState;
    }

    public String getContinuationToken() {
        return continuationToken;
    }

    public void setContinuationToken(@Nullable String continuationToken) {
        this.continuationToken = continuationToken;
    }
}
