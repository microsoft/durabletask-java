// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.models;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Internal result of a batch export attempt within the orchestrator.
 */
public final class BatchExportResult {

    private boolean allSucceeded;
    private int exportedCount;
    private List<ExportFailure> failures;

    public BatchExportResult() {
    }

    public BatchExportResult(boolean allSucceeded, int exportedCount, @Nullable List<ExportFailure> failures) {
        this.allSucceeded = allSucceeded;
        this.exportedCount = exportedCount;
        this.failures = failures;
    }

    public boolean isAllSucceeded() { return this.allSucceeded; }
    public void setAllSucceeded(boolean allSucceeded) { this.allSucceeded = allSucceeded; }

    public int getExportedCount() { return this.exportedCount; }
    public void setExportedCount(int exportedCount) { this.exportedCount = exportedCount; }

    @Nullable
    public List<ExportFailure> getFailures() { return this.failures; }
    public void setFailures(@Nullable List<ExportFailure> failures) { this.failures = failures; }
}
