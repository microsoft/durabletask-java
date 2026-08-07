// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.OffsetDateTime;

/**
 * A read-only snapshot of a schedule's configuration and runtime state, returned by describe and list operations.
 */
public final class ScheduleDescription {

    private final String scheduleId;
    private final String orchestrationName;
    private final String orchestrationInput;
    private final String orchestrationInstanceId;
    private final OffsetDateTime startAt;
    private final OffsetDateTime endAt;
    private final Duration interval;
    private final Boolean startImmediatelyIfLate;
    private final ScheduleStatus status;
    private final String executionToken;
    private final OffsetDateTime lastRunAt;
    private final OffsetDateTime nextRunAt;

    ScheduleDescription(
            String scheduleId,
            @Nullable String orchestrationName,
            @Nullable String orchestrationInput,
            @Nullable String orchestrationInstanceId,
            @Nullable OffsetDateTime startAt,
            @Nullable OffsetDateTime endAt,
            @Nullable Duration interval,
            @Nullable Boolean startImmediatelyIfLate,
            ScheduleStatus status,
            String executionToken,
            @Nullable OffsetDateTime lastRunAt,
            @Nullable OffsetDateTime nextRunAt) {
        this.scheduleId = scheduleId;
        this.orchestrationName = orchestrationName;
        this.orchestrationInput = orchestrationInput;
        this.orchestrationInstanceId = orchestrationInstanceId;
        this.startAt = startAt;
        this.endAt = endAt;
        this.interval = interval;
        this.startImmediatelyIfLate = startImmediatelyIfLate;
        this.status = status;
        this.executionToken = executionToken;
        this.lastRunAt = lastRunAt;
        this.nextRunAt = nextRunAt;
    }

    static ScheduleDescription fromState(String scheduleId, ScheduleState state) {
        ScheduleConfiguration config = state.getScheduleConfiguration();
        return new ScheduleDescription(
                scheduleId,
                config == null ? null : config.getOrchestrationName(),
                config == null ? null : config.getOrchestrationInput(),
                config == null ? null : config.getOrchestrationInstanceId(),
                config == null ? null : config.getStartAt(),
                config == null ? null : config.getEndAt(),
                config == null ? null : config.getInterval(),
                config == null ? null : config.isStartImmediatelyIfLate(),
                state.getStatus(),
                state.getExecutionToken(),
                state.getLastRunAt(),
                state.getNextRunAt());
    }

    /** @return the schedule ID. */
    public String getScheduleId() {
        return this.scheduleId;
    }

    /** @return the orchestration name, or {@code null}. */
    @Nullable
    public String getOrchestrationName() {
        return this.orchestrationName;
    }

    /** @return the serialized orchestration input, or {@code null}. */
    @Nullable
    public String getOrchestrationInput() {
        return this.orchestrationInput;
    }

    /** @return the fixed orchestration instance ID, or {@code null}. */
    @Nullable
    public String getOrchestrationInstanceId() {
        return this.orchestrationInstanceId;
    }

    /** @return the start time, or {@code null}. */
    @Nullable
    public OffsetDateTime getStartAt() {
        return this.startAt;
    }

    /** @return the end time, or {@code null}. */
    @Nullable
    public OffsetDateTime getEndAt() {
        return this.endAt;
    }

    /** @return the interval between runs, or {@code null}. */
    @Nullable
    public Duration getInterval() {
        return this.interval;
    }

    /** @return whether the first run starts immediately when late, or {@code null} when unconfigured. */
    @Nullable
    public Boolean getStartImmediatelyIfLate() {
        return this.startImmediatelyIfLate;
    }

    /** @return the current schedule status. */
    public ScheduleStatus getStatus() {
        return this.status;
    }

    /** @return the current execution token. */
    public String getExecutionToken() {
        return this.executionToken;
    }

    /** @return the last time the schedule ran, or {@code null}. */
    @Nullable
    public OffsetDateTime getLastRunAt() {
        return this.lastRunAt;
    }

    /** @return the next scheduled run time, or {@code null}. */
    @Nullable
    public OffsetDateTime getNextRunAt() {
        return this.nextRunAt;
    }

    @Override
    public String toString() {
        return "ScheduleDescription{scheduleId='" + this.scheduleId + "', status=" + this.status
                + ", orchestrationName='" + this.orchestrationName + "', interval=" + this.interval
                + ", nextRunAt=" + this.nextRunAt + ", lastRunAt=" + this.lastRunAt + "}";
    }
}
