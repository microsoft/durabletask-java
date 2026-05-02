// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Internal configuration for an export job, derived from {@link ExportJobCreationOptions}.
 */
public final class ExportJobConfiguration {

    private static final int DEFAULT_MAX_PARALLEL_EXPORTS = 32;
    private static final int DEFAULT_MAX_INSTANCES_PER_BATCH = 100;

    private final ExportMode mode;
    private final ExportFilter filter;
    private final ExportDestination destination;
    private final ExportFormat format;
    private final int maxParallelExports;
    private final int maxInstancesPerBatch;

    public ExportJobConfiguration(
            @Nonnull ExportMode mode,
            @Nonnull ExportFilter filter,
            @Nonnull ExportDestination destination,
            @Nonnull ExportFormat format,
            int maxInstancesPerBatch) {
        this(mode, filter, destination, format, DEFAULT_MAX_PARALLEL_EXPORTS, maxInstancesPerBatch);
    }

    public ExportJobConfiguration(
            @Nonnull ExportMode mode,
            @Nonnull ExportFilter filter,
            @Nonnull ExportDestination destination,
            @Nonnull ExportFormat format,
            int maxParallelExports,
            int maxInstancesPerBatch) {
        this.mode = mode;
        this.filter = filter;
        this.destination = destination;
        this.format = format;
        this.maxParallelExports = maxParallelExports > 0 ? maxParallelExports : DEFAULT_MAX_PARALLEL_EXPORTS;
        this.maxInstancesPerBatch = maxInstancesPerBatch > 0 ? maxInstancesPerBatch : DEFAULT_MAX_INSTANCES_PER_BATCH;
    }

    // Default constructor for Jackson deserialization
    public ExportJobConfiguration() {
        this.mode = ExportMode.BATCH;
        this.filter = null;
        this.destination = null;
        this.format = ExportFormat.DEFAULT;
        this.maxParallelExports = DEFAULT_MAX_PARALLEL_EXPORTS;
        this.maxInstancesPerBatch = DEFAULT_MAX_INSTANCES_PER_BATCH;
    }

    @Nonnull
    public ExportMode getMode() {
        return this.mode;
    }

    @Nullable
    public ExportFilter getFilter() {
        return this.filter;
    }

    @Nullable
    public ExportDestination getDestination() {
        return this.destination;
    }

    @Nonnull
    public ExportFormat getFormat() {
        return this.format;
    }

    public int getMaxParallelExports() {
        return this.maxParallelExports;
    }

    public int getMaxInstancesPerBatch() {
        return this.maxInstancesPerBatch;
    }
}
