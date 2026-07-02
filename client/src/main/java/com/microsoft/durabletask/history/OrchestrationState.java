// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.history;

import com.microsoft.durabletask.FailureDetails;
import com.microsoft.durabletask.OrchestrationRuntimeStatus;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A snapshot of an orchestration instance's runtime state, as carried by a {@link HistoryStateEvent}.
 * <p>
 * This mirrors the {@code OrchestrationState} surfaced by the sibling .NET and Python SDKs (the .NET
 * {@code HistoryStateEvent.State} property and the Python {@code HistoryStateEvent.orchestration_state} value), so that
 * archival/export consumers can capture the full state checkpoint rather than just the instance ID.
 */
public final class OrchestrationState {
    private final String instanceId;
    private final String name;
    private final String version;
    private final OrchestrationRuntimeStatus runtimeStatus;
    private final Instant scheduledStartTime;
    private final Instant createdTime;
    private final Instant lastUpdatedTime;
    private final Instant completedTime;
    private final String input;
    private final String output;
    private final String customStatus;
    private final FailureDetails failureDetails;
    private final String executionId;
    private final String parentInstanceId;
    private final Map<String, String> tags;

    /**
     * Creates a new {@code OrchestrationState}.
     *
     * @param instanceId       the orchestration instance ID
     * @param name             the orchestration name
     * @param version          the orchestration version, or {@code null}
     * @param runtimeStatus    the runtime status of the orchestration
     * @param scheduledStartTime the scheduled start time, or {@code null}
     * @param createdTime      the creation time, or {@code null}
     * @param lastUpdatedTime  the last-updated time, or {@code null}
     * @param completedTime    the completion time, or {@code null}
     * @param input            the serialized input, or {@code null}
     * @param output           the serialized output, or {@code null}
     * @param customStatus     the serialized custom status, or {@code null}
     * @param failureDetails   the failure details when the orchestration failed, or {@code null}
     * @param executionId      the execution ID, or {@code null}
     * @param parentInstanceId the parent instance ID, or {@code null}
     * @param tags             the orchestration tags, or {@code null}
     */
    public OrchestrationState(
            String instanceId,
            String name,
            @Nullable String version,
            OrchestrationRuntimeStatus runtimeStatus,
            @Nullable Instant scheduledStartTime,
            @Nullable Instant createdTime,
            @Nullable Instant lastUpdatedTime,
            @Nullable Instant completedTime,
            @Nullable String input,
            @Nullable String output,
            @Nullable String customStatus,
            @Nullable FailureDetails failureDetails,
            @Nullable String executionId,
            @Nullable String parentInstanceId,
            @Nullable Map<String, String> tags) {
        this.instanceId = instanceId;
        this.name = name;
        this.version = version;
        this.runtimeStatus = runtimeStatus;
        this.scheduledStartTime = scheduledStartTime;
        this.createdTime = createdTime;
        this.lastUpdatedTime = lastUpdatedTime;
        this.completedTime = completedTime;
        this.input = input;
        this.output = output;
        this.customStatus = customStatus;
        this.failureDetails = failureDetails;
        this.executionId = executionId;
        this.parentInstanceId = parentInstanceId;
        this.tags = tags == null ? null : Collections.unmodifiableMap(new HashMap<>(tags));
    }

    /** @return the orchestration instance ID. */
    public String getInstanceId() {
        return this.instanceId;
    }

    /** @return the orchestration name. */
    public String getName() {
        return this.name;
    }

    /** @return the orchestration version, or {@code null} if not set. */
    @Nullable
    public String getVersion() {
        return this.version;
    }

    /** @return the runtime status of the orchestration. */
    public OrchestrationRuntimeStatus getRuntimeStatus() {
        return this.runtimeStatus;
    }

    /** @return the scheduled start time, or {@code null} if not set. */
    @Nullable
    public Instant getScheduledStartTime() {
        return this.scheduledStartTime;
    }

    /** @return the creation time, or {@code null} if not set. */
    @Nullable
    public Instant getCreatedTime() {
        return this.createdTime;
    }

    /** @return the last-updated time, or {@code null} if not set. */
    @Nullable
    public Instant getLastUpdatedTime() {
        return this.lastUpdatedTime;
    }

    /** @return the completion time, or {@code null} if not set. */
    @Nullable
    public Instant getCompletedTime() {
        return this.completedTime;
    }

    /** @return the serialized orchestration input, or {@code null} if not set. */
    @Nullable
    public String getInput() {
        return this.input;
    }

    /** @return the serialized orchestration output, or {@code null} if not set. */
    @Nullable
    public String getOutput() {
        return this.output;
    }

    /** @return the serialized custom status, or {@code null} if not set. */
    @Nullable
    public String getCustomStatus() {
        return this.customStatus;
    }

    /** @return the failure details when the orchestration failed, or {@code null} otherwise. */
    @Nullable
    public FailureDetails getFailureDetails() {
        return this.failureDetails;
    }

    /** @return the execution ID, or {@code null} if not set. */
    @Nullable
    public String getExecutionId() {
        return this.executionId;
    }

    /** @return the parent instance ID, or {@code null} if not set. */
    @Nullable
    public String getParentInstanceId() {
        return this.parentInstanceId;
    }

    /** @return an unmodifiable view of the orchestration tags, or {@code null} if not set. */
    @Nullable
    public Map<String, String> getTags() {
        return this.tags;
    }
}
