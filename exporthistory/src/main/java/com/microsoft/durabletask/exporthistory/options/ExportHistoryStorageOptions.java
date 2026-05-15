// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.options;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Azure Blob Storage configuration for export history jobs.
 */
public final class ExportHistoryStorageOptions {

    private final String connectionString;
    private final String containerName;
    private final String prefix;

    private ExportHistoryStorageOptions(String connectionString, String containerName, String prefix) {
        this.connectionString = connectionString;
        this.containerName = containerName;
        this.prefix = prefix;
    }

    @Nonnull
    public String getConnectionString() { return this.connectionString; }

    @Nonnull
    public String getContainerName() { return this.containerName; }

    @Nullable
    public String getPrefix() { return this.prefix; }

    /**
     * Creates a new builder for {@code ExportHistoryStorageOptions}.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@code ExportHistoryStorageOptions}.
     */
    public static final class Builder {
        private String connectionString;
        private String containerName;
        private String prefix;

        private Builder() {
        }

        public Builder connectionString(@Nonnull String connectionString) {
            this.connectionString = connectionString;
            return this;
        }

        public Builder containerName(@Nonnull String containerName) {
            this.containerName = containerName;
            return this;
        }

        public Builder prefix(@Nullable String prefix) {
            this.prefix = prefix;
            return this;
        }

        public ExportHistoryStorageOptions build() {
            if (this.connectionString == null || this.connectionString.isEmpty()) {
                throw new IllegalArgumentException("connectionString must not be null or empty.");
            }
            if (this.containerName == null || this.containerName.isEmpty()) {
                throw new IllegalArgumentException("containerName must not be null or empty.");
            }
            return new ExportHistoryStorageOptions(this.connectionString, this.containerName, this.prefix);
        }
    }
}
