// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.models;

public final class PurgeResult {

    private final int deletedInstanceCount;

    public PurgeResult(int deletedInstanceCount) {
        this.deletedInstanceCount = deletedInstanceCount;
    }

    public int getDeletedInstanceCount() {
        return this.deletedInstanceCount;
    }
}
