// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.OffsetDateTime;

/**
 * Options for creating (or fully replacing) a schedule.
 * <p>
 * The orchestration input is a serialized {@code String}, matching the .NET SDK. Callers that need to pass
 * structured data should serialize it to JSON text.
 */
public final class ScheduleCreationOptions {

    private final String scheduleId;
    private final String orchestrationName;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private final Duration interval;

    private String orchestrationInput;
    private String orchestrationInstanceId;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private OffsetDateTime startAt;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private OffsetDateTime endAt;

    private boolean startImmediatelyIfLate;

    /**
     * Creates a new {@code ScheduleCreationOptions}.
     *
     * @param scheduleId        the unique schedule ID; must not be {@code null} or empty
     * @param orchestrationName the name of the orchestration to start on each run; must not be {@code null} or empty
     * @param interval          the interval between runs; must be at least one second
     * @throws ScheduleClientValidationException if any argument is invalid
     */
    @JsonCreator
    public ScheduleCreationOptions(
            @JsonProperty("scheduleId") String scheduleId,
            @JsonProperty("orchestrationName") String orchestrationName,
            @JsonProperty("interval") Duration interval) {
        if (scheduleId == null || scheduleId.isEmpty()) {
            throw new ScheduleClientValidationException(scheduleId == null ? "" : scheduleId,
                    "scheduleId must not be null or empty.");
        }
        if (orchestrationName == null || orchestrationName.isEmpty()) {
            throw new ScheduleClientValidationException(scheduleId,
                    "orchestrationName must not be null or empty.");
        }
        Intervals.validate(scheduleId, interval);
        this.scheduleId = scheduleId;
        this.orchestrationName = orchestrationName;
        this.interval = interval;
    }

    /** @return the unique schedule ID. */
    public String getScheduleId() {
        return this.scheduleId;
    }

    /** @return the orchestration name to start on each run. */
    public String getOrchestrationName() {
        return this.orchestrationName;
    }

    /** @return the interval between runs. */
    public Duration getInterval() {
        return this.interval;
    }

    /** @return the serialized orchestration input, or {@code null}. */
    @Nullable
    public String getOrchestrationInput() {
        return this.orchestrationInput;
    }

    /**
     * Sets the serialized orchestration input.
     *
     * @param orchestrationInput the serialized input, or {@code null}
     * @return this options object for chaining
     */
    public ScheduleCreationOptions setOrchestrationInput(@Nullable String orchestrationInput) {
        this.orchestrationInput = orchestrationInput;
        return this;
    }

    /** @return the fixed orchestration instance ID, or {@code null} to auto-generate one per run. */
    @Nullable
    public String getOrchestrationInstanceId() {
        return this.orchestrationInstanceId;
    }

    /**
     * Sets a fixed orchestration instance ID. When set, every run uses this ID and the backend's duplicate-instance
     * behavior determines whether overlapping runs are prevented.
     *
     * @param orchestrationInstanceId the instance ID, or {@code null}
     * @return this options object for chaining
     */
    public ScheduleCreationOptions setOrchestrationInstanceId(@Nullable String orchestrationInstanceId) {
        this.orchestrationInstanceId = orchestrationInstanceId;
        return this;
    }

    /** @return the start time, or {@code null} to use the creation time. */
    @Nullable
    public OffsetDateTime getStartAt() {
        return this.startAt;
    }

    /**
     * Sets the start time (the cadence anchor). When {@code null}, the schedule creation time is used.
     *
     * @param startAt the start time, or {@code null}
     * @return this options object for chaining
     */
    public ScheduleCreationOptions setStartAt(@Nullable OffsetDateTime startAt) {
        this.startAt = startAt;
        return this;
    }

    /** @return the end time, or {@code null} to run indefinitely. */
    @Nullable
    public OffsetDateTime getEndAt() {
        return this.endAt;
    }

    /**
     * Sets the end time. After this time the schedule stops and deletes itself.
     *
     * @param endAt the end time, or {@code null}
     * @return this options object for chaining
     */
    public ScheduleCreationOptions setEndAt(@Nullable OffsetDateTime endAt) {
        this.endAt = endAt;
        return this;
    }

    /** @return whether the first run starts immediately when the start time is already in the past. */
    public boolean isStartImmediatelyIfLate() {
        return this.startImmediatelyIfLate;
    }

    /**
     * Sets whether the first run should start immediately when the start time is already in the past. When
     * {@code false} (the default), missed intervals are skipped and the first run is aligned to the next interval
     * boundary.
     *
     * @param startImmediatelyIfLate whether to start immediately when late
     * @return this options object for chaining
     */
    public ScheduleCreationOptions setStartImmediatelyIfLate(boolean startImmediatelyIfLate) {
        this.startImmediatelyIfLate = startImmediatelyIfLate;
        return this;
    }
}
