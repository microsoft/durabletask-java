// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

/**
 * Input to {@link ExportInstanceHistoryActivity}: the instance to export plus the destination and format.
 */
public final class ExportRequest {

    private String instanceId;
    private ExportDestination destination;
    private ExportFormat format;

    /** Creates an empty {@code ExportRequest} (for deserialization). */
    public ExportRequest() {
    }

    /**
     * Creates an {@code ExportRequest}.
     *
     * @param instanceId  the instance ID to export
     * @param destination the export destination
     * @param format      the export format
     */
    public ExportRequest(String instanceId, ExportDestination destination, ExportFormat format) {
        this.instanceId = instanceId;
        this.destination = destination;
        this.format = format;
    }

    /** @return the instance ID to export. */
    public String getInstanceId() {
        return this.instanceId;
    }

    /**
     * Sets the instance ID to export.
     *
     * @param instanceId the instance ID
     */
    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
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
}
