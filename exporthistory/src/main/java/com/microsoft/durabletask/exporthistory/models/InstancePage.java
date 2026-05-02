// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A page of orchestration instances to export, with a checkpoint for pagination.
 */
public final class InstancePage {

    private final List<String> instanceIds;
    private final ExportCheckpoint nextCheckpoint;

    public InstancePage(@Nonnull List<String> instanceIds, @Nonnull ExportCheckpoint nextCheckpoint) {
        this.instanceIds = instanceIds != null ? Collections.unmodifiableList(instanceIds) : Collections.emptyList();
        this.nextCheckpoint = nextCheckpoint;
    }

    // Default constructor for Jackson deserialization
    public InstancePage() {
        this.instanceIds = Collections.emptyList();
        this.nextCheckpoint = null;
    }

    @Nonnull
    public List<String> getInstanceIds() { return this.instanceIds; }

    @Nullable
    public ExportCheckpoint getNextCheckpoint() { return this.nextCheckpoint; }
}
