// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

/**
 * Configuration for an export job, persisted in the {@link ExportJobState} entity state.
 */
public final class ExportJobConfiguration {

    /** The default maximum number of parallel export operations. */
    public static final int DEFAULT_MAX_PARALLEL_EXPORTS = 32;

    /** The default maximum number of instances fetched per batch. */
    public static final int DEFAULT_MAX_INSTANCES_PER_BATCH = 100;

    private ExportMode mode;
    private ExportFilter filter;
    private ExportDestination destination;
    private ExportFormat format;
    private int maxParallelExports = DEFAULT_MAX_PARALLEL_EXPORTS;
    private int maxInstancesPerBatch = DEFAULT_MAX_INSTANCES_PER_BATCH;

    /** Creates an empty {@code ExportJobConfiguration} (for deserialization). */
    public ExportJobConfiguration() {
    }

    /**
     * Creates an {@code ExportJobConfiguration}.
     *
     * @param mode                 the export mode
     * @param filter               the filter criteria
     * @param destination          the export destination
     * @param format               the export format
     * @param maxInstancesPerBatch the maximum instances fetched per batch
     */
    public ExportJobConfiguration(
            ExportMode mode,
            ExportFilter filter,
            ExportDestination destination,
            ExportFormat format,
            int maxInstancesPerBatch) {
        this.mode = mode;
        this.filter = filter;
        this.destination = destination;
        this.format = format;
        this.maxInstancesPerBatch = maxInstancesPerBatch;
    }

    /** @return the export mode. */
    public ExportMode getMode() {
        return this.mode;
    }

    /**
     * Sets the export mode.
     *
     * @param mode the export mode
     */
    public void setMode(ExportMode mode) {
        this.mode = mode;
    }

    /** @return the filter criteria. */
    public ExportFilter getFilter() {
        return this.filter;
    }

    /**
     * Sets the filter criteria.
     *
     * @param filter the filter criteria
     */
    public void setFilter(ExportFilter filter) {
        this.filter = filter;
    }

    /** @return the export destination. */
    public ExportDestination getDestination() {
        return this.destination;
    }

    /**
     * Sets the export destination.
     *
     * @param destination the destination
     */
    public void setDestination(ExportDestination destination) {
        this.destination = destination;
    }

    /** @return the export format. */
    public ExportFormat getFormat() {
        return this.format;
    }

    /**
     * Sets the export format.
     *
     * @param format the format
     */
    public void setFormat(ExportFormat format) {
        this.format = format;
    }

    /** @return the maximum number of parallel export operations. */
    public int getMaxParallelExports() {
        return this.maxParallelExports;
    }

    /**
     * Sets the maximum number of parallel export operations.
     *
     * @param maxParallelExports the maximum parallel exports
     */
    public void setMaxParallelExports(int maxParallelExports) {
        this.maxParallelExports = maxParallelExports;
    }

    /** @return the maximum number of instances fetched per batch. */
    public int getMaxInstancesPerBatch() {
        return this.maxInstancesPerBatch;
    }

    /**
     * Sets the maximum number of instances fetched per batch.
     *
     * @param maxInstancesPerBatch the maximum instances per batch
     */
    public void setMaxInstancesPerBatch(int maxInstancesPerBatch) {
        this.maxInstancesPerBatch = maxInstancesPerBatch;
    }
}
