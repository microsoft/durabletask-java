// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Azure Blob Storage destination for export history output.
 */
public final class ExportDestination {

    private String container;
    private String prefix;

    /**
     * Default constructor for Jackson deserialization.
     */
    public ExportDestination() {
    }

    /**
     * Creates a new {@code ExportDestination}.
     *
     * @param container the blob container name
     */
    public ExportDestination(@Nonnull String container) {
        this(container, null);
    }

    /**
     * Creates a new {@code ExportDestination} with an optional path prefix.
     *
     * @param container the blob container name
     * @param prefix    optional prefix for blob paths, or {@code null}
     */
    public ExportDestination(@Nonnull String container, @Nullable String prefix) {
        if (container == null || container.isEmpty()) {
            throw new IllegalArgumentException("container must not be null or empty.");
        }
        this.container = container;
        this.prefix = prefix;
    }

    @Nonnull
    public String getContainer() {
        return this.container;
    }

    @Nullable
    public String getPrefix() {
        return this.prefix;
    }

    public void setContainer(String container) { this.container = container; }
    public void setPrefix(String prefix) { this.prefix = prefix; }
}
