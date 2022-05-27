// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

/**
 * Class representing the results of an orchestration state purge operation.
 * <p>
 * Orchestration state can be purged using any of the {@link DurableTaskClient#purgeInstance} method overloads.
 */
public final class PurgeResult {

    private final int deletedInstanceCount;

    PurgeResult(int deletedInstanceCount) {
        this.deletedInstanceCount = deletedInstanceCount;
    }

    /**
     * Gets the number of purged orchestration instances.
     * @return the number of purged orchestration instances
     */
    public int getDeletedInstanceCount() {
        return this.deletedInstanceCount;
    }
}
