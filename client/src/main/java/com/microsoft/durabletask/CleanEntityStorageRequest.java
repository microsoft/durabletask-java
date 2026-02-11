// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;

/**
 * Represents a request to clean up entity storage by removing empty entities and/or releasing orphaned locks.
 * <p>
 * Use the builder-style setters to configure the request, then pass it to
 * {@link DurableTaskClient#cleanEntityStorage(CleanEntityStorageRequest)}.
 */
public final class CleanEntityStorageRequest {
    private String continuationToken;
    private boolean removeEmptyEntities;
    private boolean releaseOrphanedLocks;

    /**
     * Creates a new {@code CleanEntityStorageRequest} with default settings.
     * By default, both {@code removeEmptyEntities} and {@code releaseOrphanedLocks} are {@code false}.
     */
    public CleanEntityStorageRequest() {
    }

    /**
     * Sets the continuation token for resuming a previous clean operation.
     *
     * @param continuationToken the continuation token, or {@code null} to start from the beginning
     * @return this {@code CleanEntityStorageRequest} for chaining
     */
    public CleanEntityStorageRequest setContinuationToken(@Nullable String continuationToken) {
        this.continuationToken = continuationToken;
        return this;
    }

    /**
     * Gets the continuation token for resuming a previous clean operation.
     *
     * @return the continuation token, or {@code null}
     */
    @Nullable
    public String getContinuationToken() {
        return this.continuationToken;
    }

    /**
     * Sets whether to remove entities that have no state and no pending operations.
     *
     * @param removeEmptyEntities {@code true} to remove empty entities
     * @return this {@code CleanEntityStorageRequest} for chaining
     */
    public CleanEntityStorageRequest setRemoveEmptyEntities(boolean removeEmptyEntities) {
        this.removeEmptyEntities = removeEmptyEntities;
        return this;
    }

    /**
     * Gets whether empty entities should be removed.
     *
     * @return {@code true} if empty entities will be removed
     */
    public boolean isRemoveEmptyEntities() {
        return this.removeEmptyEntities;
    }

    /**
     * Sets whether to release locks held by orchestrations that no longer exist.
     *
     * @param releaseOrphanedLocks {@code true} to release orphaned locks
     * @return this {@code CleanEntityStorageRequest} for chaining
     */
    public CleanEntityStorageRequest setReleaseOrphanedLocks(boolean releaseOrphanedLocks) {
        this.releaseOrphanedLocks = releaseOrphanedLocks;
        return this;
    }

    /**
     * Gets whether orphaned locks should be released.
     *
     * @return {@code true} if orphaned locks will be released
     */
    public boolean isReleaseOrphanedLocks() {
        return this.releaseOrphanedLocks;
    }
}
