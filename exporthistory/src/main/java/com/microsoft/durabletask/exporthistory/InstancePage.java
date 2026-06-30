// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Output of {@link ListTerminalInstancesActivity}: a page of terminal instance IDs plus the checkpoint to advance
 * to once this page has been exported.
 */
public final class InstancePage {

    private List<String> instanceIds = new ArrayList<>();
    private ExportCheckpoint nextCheckpoint;

    /** Creates an empty {@code InstancePage} (for deserialization). */
    public InstancePage() {
    }

    /**
     * Creates an {@code InstancePage}.
     *
     * @param instanceIds    the page of terminal instance IDs
     * @param nextCheckpoint the checkpoint to advance to after exporting this page, or {@code null}
     */
    public InstancePage(List<String> instanceIds, @Nullable ExportCheckpoint nextCheckpoint) {
        this.instanceIds = instanceIds;
        this.nextCheckpoint = nextCheckpoint;
    }

    /** @return the page of terminal instance IDs. */
    public List<String> getInstanceIds() {
        return this.instanceIds;
    }

    /**
     * Sets the page of terminal instance IDs.
     *
     * @param instanceIds the instance IDs
     */
    public void setInstanceIds(List<String> instanceIds) {
        this.instanceIds = instanceIds;
    }

    /** @return the checkpoint to advance to after exporting this page, or {@code null}. */
    @Nullable
    public ExportCheckpoint getNextCheckpoint() {
        return this.nextCheckpoint;
    }

    /**
     * Sets the checkpoint to advance to after exporting this page.
     *
     * @param nextCheckpoint the next checkpoint, or {@code null}
     */
    public void setNextCheckpoint(@Nullable ExportCheckpoint nextCheckpoint) {
        this.nextCheckpoint = nextCheckpoint;
    }
}
