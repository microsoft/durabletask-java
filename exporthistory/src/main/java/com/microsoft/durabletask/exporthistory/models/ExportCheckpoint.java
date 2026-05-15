// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import javax.annotation.Nullable;

/**
 * Checkpoint for resumable exports. Stores the last instance key processed.
 */
public final class ExportCheckpoint {

    private String lastInstanceKey;

    public ExportCheckpoint() {
    }

    public ExportCheckpoint(@Nullable String lastInstanceKey) {
        this.lastInstanceKey = lastInstanceKey;
    }

    @Nullable
    public String getLastInstanceKey() {
        return this.lastInstanceKey;
    }

    public void setLastInstanceKey(String lastInstanceKey) { this.lastInstanceKey = lastInstanceKey; }
}
