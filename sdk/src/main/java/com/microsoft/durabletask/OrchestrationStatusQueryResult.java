// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.util.List;

public class OrchestrationStatusQueryResult {
    private final List<OrchestrationMetadata> orchestrationStates;
    private final String continuationToken;

    public OrchestrationStatusQueryResult(List<OrchestrationMetadata> orchestrationStates, @Nullable String continuationToken) {
        this.orchestrationStates = orchestrationStates;
        this.continuationToken = continuationToken;
    }

    public List<OrchestrationMetadata> getOrchestrationState() {
        return this.orchestrationStates;
    }
    public String getContinuationToken() {
        return this.continuationToken;
    }
}
