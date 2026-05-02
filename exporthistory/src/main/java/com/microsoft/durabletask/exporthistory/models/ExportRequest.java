// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Export request for one orchestration instance.
 */
public final class ExportRequest {

    private String instanceId;
    private ExportDestination destination;
    private ExportFormat format;

    // Default constructor for Jackson deserialization
    public ExportRequest() {
    }

    public ExportRequest(@Nonnull String instanceId, @Nonnull ExportDestination destination, @Nonnull ExportFormat format) {
        this.instanceId = instanceId;
        this.destination = destination;
        this.format = format;
    }

    @Nonnull
    public String getInstanceId() { return this.instanceId; }
    public void setInstanceId(@Nonnull String instanceId) { this.instanceId = instanceId; }

    @Nonnull
    public ExportDestination getDestination() { return this.destination; }
    public void setDestination(@Nonnull ExportDestination destination) { this.destination = destination; }

    @Nonnull
    public ExportFormat getFormat() { return this.format; }
    public void setFormat(@Nonnull ExportFormat format) { this.format = format; }
}
