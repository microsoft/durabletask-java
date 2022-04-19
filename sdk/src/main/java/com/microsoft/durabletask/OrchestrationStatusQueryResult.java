// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.util.List;

public class OrchestrationStatusQueryResult {
    private List<OrchestrationMetadata> OrchestrationState;
    private String continuationToken;

    public OrchestrationStatusQueryResult(List<OrchestrationMetadata> orchestrationState, @Nullable String continuationToken) {
        OrchestrationState = orchestrationState;
        this.continuationToken = continuationToken;
    }

    public List<OrchestrationMetadata> getOrchestrationState() {
        return OrchestrationState;
    }
    public String getContinuationToken() {
        return continuationToken;
    }
}
