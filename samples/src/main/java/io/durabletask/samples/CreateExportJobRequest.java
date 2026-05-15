// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.OrchestrationRuntimeStatus;
import com.microsoft.durabletask.exporthistory.models.*;

import java.time.Instant;
import java.util.List;

/**
 * Request body for POST /export-jobs.
 */
public class CreateExportJobRequest {

    private String jobId;
    private ExportMode mode;
    private Instant completedTimeFrom;
    private Instant completedTimeTo;
    private String container;
    private String prefix;
    private ExportFormat format;
    private List<OrchestrationRuntimeStatus> runtimeStatus;
    private int maxInstancesPerBatch = 100;

    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }

    public ExportMode getMode() { return mode; }
    public void setMode(ExportMode mode) { this.mode = mode; }

    public Instant getCompletedTimeFrom() { return completedTimeFrom; }
    public void setCompletedTimeFrom(Instant completedTimeFrom) { this.completedTimeFrom = completedTimeFrom; }

    public Instant getCompletedTimeTo() { return completedTimeTo; }
    public void setCompletedTimeTo(Instant completedTimeTo) { this.completedTimeTo = completedTimeTo; }

    public String getContainer() { return container; }
    public void setContainer(String container) { this.container = container; }

    public String getPrefix() { return prefix; }
    public void setPrefix(String prefix) { this.prefix = prefix; }

    public ExportFormat getFormat() { return format; }
    public void setFormat(ExportFormat format) { this.format = format; }

    public List<OrchestrationRuntimeStatus> getRuntimeStatus() { return runtimeStatus; }
    public void setRuntimeStatus(List<OrchestrationRuntimeStatus> runtimeStatus) { this.runtimeStatus = runtimeStatus; }

    public int getMaxInstancesPerBatch() { return maxInstancesPerBatch; }
    public void setMaxInstancesPerBatch(int maxInstancesPerBatch) { this.maxInstancesPerBatch = maxInstancesPerBatch; }
}
