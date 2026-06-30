// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import javax.annotation.Nullable;

/**
 * Export destination settings for Azure Blob Storage.
 * <p>
 * Mirrors the .NET {@code Microsoft.DurableTask.ExportHistory.ExportDestination}.
 */
public final class ExportDestination {

    private String container;
    private String prefix;

    /** Creates an empty {@code ExportDestination} (for deserialization). */
    public ExportDestination() {
    }

    /**
     * Creates an {@code ExportDestination} for the given container.
     *
     * @param container the blob container name
     */
    public ExportDestination(String container) {
        this.container = container;
    }

    /** @return the blob container name. */
    public String getContainer() {
        return this.container;
    }

    /**
     * Sets the blob container name.
     *
     * @param container the container name
     */
    public void setContainer(String container) {
        this.container = container;
    }

    /** @return the optional blob path prefix, or {@code null}. */
    @Nullable
    public String getPrefix() {
        return this.prefix;
    }

    /**
     * Sets the optional blob path prefix.
     *
     * @param prefix the prefix, or {@code null}
     */
    public void setPrefix(@Nullable String prefix) {
        this.prefix = prefix;
    }
}
