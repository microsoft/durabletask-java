// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import javax.annotation.Nullable;

/**
 * Checkpoint information used to resume an export.
 * <p>
 * Mirrors the .NET {@code Microsoft.DurableTask.ExportHistory.ExportCheckpoint}. The
 * {@code lastInstanceKey} is the pagination cursor returned by the client {@code listInstanceIds} wrapper.
 */
public final class ExportCheckpoint {

    private String lastInstanceKey;

    /** Creates an empty {@code ExportCheckpoint} (for deserialization). */
    public ExportCheckpoint() {
    }

    /**
     * Creates an {@code ExportCheckpoint}.
     *
     * @param lastInstanceKey the pagination cursor, or {@code null}
     */
    public ExportCheckpoint(@Nullable String lastInstanceKey) {
        this.lastInstanceKey = lastInstanceKey;
    }

    /** @return the pagination cursor, or {@code null}. */
    @Nullable
    public String getLastInstanceKey() {
        return this.lastInstanceKey;
    }

    /**
     * Sets the pagination cursor.
     *
     * @param lastInstanceKey the cursor, or {@code null}
     */
    public void setLastInstanceKey(@Nullable String lastInstanceKey) {
        this.lastInstanceKey = lastInstanceKey;
    }
}
