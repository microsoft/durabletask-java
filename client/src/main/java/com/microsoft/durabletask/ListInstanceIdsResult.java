// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * Result of a {@link DurableTaskClient#listInstanceIds(ListInstanceIdsQuery)} call.
 * <p>
 * Contains a page of terminal orchestration instance IDs and a pagination cursor for fetching the next page.
 */
public final class ListInstanceIdsResult {
    private final List<String> instanceIds;
    private final String continuationToken;

    ListInstanceIdsResult(List<String> instanceIds, @Nullable String continuationToken) {
        this.instanceIds = Collections.unmodifiableList(instanceIds);
        this.continuationToken = continuationToken;
    }

    /**
     * Gets the page of terminal orchestration instance IDs that matched the query.
     *
     * @return an unmodifiable list of instance IDs that matched the query
     */
    public List<String> getInstanceIds() {
        return this.instanceIds;
    }

    /**
     * Gets the pagination cursor to pass to the next
     * {@link DurableTaskClient#listInstanceIds(ListInstanceIdsQuery)} call via
     * {@link ListInstanceIdsQuery#setContinuationToken(String)}, or {@code null} when there are no more pages.
     *
     * @return the cursor for the next page, or {@code null} if no more pages are available
     */
    @Nullable
    public String getContinuationToken() {
        return this.continuationToken;
    }
}
