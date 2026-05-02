// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * Represents a single page of orchestration instance IDs returned by
 * {@link DurableTaskClient#listInstanceIds}.
 * <p>
 * Includes a continuation token for key-based pagination. If the continuation token is
 * {@code null}, there are no more results.
 */
public final class InstanceIdPage {

    private final List<String> instanceIds;
    private final String continuationToken;

    /**
     * Creates a new {@code InstanceIdPage}.
     *
     * @param instanceIds       the list of instance IDs in this page
     * @param continuationToken the continuation token for the next page, or {@code null} if no more results
     */
    public InstanceIdPage(
            @Nonnull List<String> instanceIds,
            @Nullable String continuationToken) {
        if (instanceIds == null) {
            throw new IllegalArgumentException("instanceIds must not be null.");
        }
        this.instanceIds = Collections.unmodifiableList(instanceIds);
        this.continuationToken = continuationToken;
    }

    /**
     * Gets the list of orchestration instance IDs in this page.
     *
     * @return unmodifiable list of instance IDs
     */
    @Nonnull
    public List<String> getInstanceIds() {
        return this.instanceIds;
    }

    /**
     * Gets the continuation token for fetching the next page of results.
     *
     * @return the continuation token, or {@code null} if there are no more results
     */
    @Nullable
    public String getContinuationToken() {
        return this.continuationToken;
    }
}
