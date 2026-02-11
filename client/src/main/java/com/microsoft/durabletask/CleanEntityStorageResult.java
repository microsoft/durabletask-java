// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;

/**
 * Represents the result of a {@link DurableTaskClient#cleanEntityStorage(CleanEntityStorageRequest)} operation.
 */
public final class CleanEntityStorageResult {
    private final String continuationToken;
    private final int emptyEntitiesRemoved;
    private final int orphanedLocksReleased;

    /**
     * Creates a new {@code CleanEntityStorageResult}.
     *
     * @param continuationToken    the continuation token for resuming the clean operation, or {@code null} if complete
     * @param emptyEntitiesRemoved the number of empty entities removed in this batch
     * @param orphanedLocksReleased the number of orphaned locks released in this batch
     */
    CleanEntityStorageResult(
            @Nullable String continuationToken,
            int emptyEntitiesRemoved,
            int orphanedLocksReleased) {
        this.continuationToken = continuationToken;
        this.emptyEntitiesRemoved = emptyEntitiesRemoved;
        this.orphanedLocksReleased = orphanedLocksReleased;
    }

    /**
     * Gets the continuation token for resuming the clean operation.
     * If {@code null}, the clean operation has processed all entities.
     *
     * @return the continuation token, or {@code null} if the operation is complete
     */
    @Nullable
    public String getContinuationToken() {
        return this.continuationToken;
    }

    /**
     * Gets the number of empty entities that were removed in this batch.
     *
     * @return the count of empty entities removed
     */
    public int getEmptyEntitiesRemoved() {
        return this.emptyEntitiesRemoved;
    }

    /**
     * Gets the number of orphaned locks that were released in this batch.
     *
     * @return the count of orphaned locks released
     */
    public int getOrphanedLocksReleased() {
        return this.orphanedLocksReleased;
    }
}
