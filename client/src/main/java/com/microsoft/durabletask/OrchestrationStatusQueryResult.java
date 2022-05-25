// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Class representing the results of a filtered orchestration metadata query.
 * <p>
 * Orchestration metadata can be queried with filters using the {@link DurableTaskClient#queryInstances} method.
 */
public final class OrchestrationStatusQueryResult {
    private final List<OrchestrationMetadata> orchestrationStates;
    private final String continuationToken;

    OrchestrationStatusQueryResult(List<OrchestrationMetadata> orchestrationStates, @Nullable String continuationToken) {
        this.orchestrationStates = orchestrationStates;
        this.continuationToken = continuationToken;
    }

    /**
     * Gets the list of orchestration metadata records that matched the {@link DurableTaskClient#queryInstances} query.
     * @return the list of orchestration metadata records that matched the {@link DurableTaskClient#queryInstances} query.
     */
    public List<OrchestrationMetadata> getOrchestrationState() {
        return this.orchestrationStates;
    }

    /**
     * Gets the continuation token to use with the next query or {@code null} if no more metadata records are found.
     * <p>
     * Note that a non-null value does not always mean that there are more metadata records that can be returned by a query.
     *
     * @return the continuation token to use with the next query or {@code null} if no more metadata records are found.
     */
    public String getContinuationToken() {
        return this.continuationToken;
    }
}
