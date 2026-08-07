// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import com.fasterxml.jackson.annotation.JsonFormat;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.OffsetDateTime;

/**
 * Options for partially updating an existing schedule. Only fields that are set are applied; {@code null} fields and
 * empty strings leave the stored value unchanged, matching the .NET SDK.
 * <p>
 * Nullable fields cannot be cleared through an update. Use create/replacement with complete options to clear an
 * optional value.
 */
public final class ScheduleUpdateOptions {

    private String orchestrationName;
    private String orchestrationInput;
    private String orchestrationInstanceId;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private OffsetDateTime startAt;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private OffsetDateTime endAt;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Duration interval;

    private Boolean startImmediatelyIfLate;

    /** Creates an empty {@code ScheduleUpdateOptions}. */
    public ScheduleUpdateOptions() {
    }

    /** @return the new orchestration name, or {@code null} to leave unchanged. */
    @Nullable
    public String getOrchestrationName() {
        return this.orchestrationName;
    }

    /**
     * Sets the new orchestration name.
     *
     * @param orchestrationName the orchestration name, or {@code null}
     * @return this options object for chaining
     */
    public ScheduleUpdateOptions setOrchestrationName(@Nullable String orchestrationName) {
        this.orchestrationName = orchestrationName;
        return this;
    }

    /** @return the new serialized orchestration input, or {@code null} to leave unchanged. */
    @Nullable
    public String getOrchestrationInput() {
        return this.orchestrationInput;
    }

    /**
     * Sets the new serialized orchestration input.
     *
     * @param orchestrationInput the serialized input, or {@code null}
     * @return this options object for chaining
     */
    public ScheduleUpdateOptions setOrchestrationInput(@Nullable String orchestrationInput) {
        this.orchestrationInput = orchestrationInput;
        return this;
    }

    /** @return the new fixed orchestration instance ID, or {@code null} to leave unchanged. */
    @Nullable
    public String getOrchestrationInstanceId() {
        return this.orchestrationInstanceId;
    }

    /**
     * Sets the new fixed orchestration instance ID.
     *
     * @param orchestrationInstanceId the instance ID, or {@code null}
     * @return this options object for chaining
     */
    public ScheduleUpdateOptions setOrchestrationInstanceId(@Nullable String orchestrationInstanceId) {
        this.orchestrationInstanceId = orchestrationInstanceId;
        return this;
    }

    /** @return the new start time, or {@code null} to leave unchanged. */
    @Nullable
    public OffsetDateTime getStartAt() {
        return this.startAt;
    }

    /**
     * Sets the new start time.
     *
     * @param startAt the start time, or {@code null}
     * @return this options object for chaining
     */
    public ScheduleUpdateOptions setStartAt(@Nullable OffsetDateTime startAt) {
        this.startAt = startAt;
        return this;
    }

    /** @return the new end time, or {@code null} to leave unchanged. */
    @Nullable
    public OffsetDateTime getEndAt() {
        return this.endAt;
    }

    /**
     * Sets the new end time.
     *
     * @param endAt the end time, or {@code null}
     * @return this options object for chaining
     */
    public ScheduleUpdateOptions setEndAt(@Nullable OffsetDateTime endAt) {
        this.endAt = endAt;
        return this;
    }

    /** @return the new interval, or {@code null} to leave unchanged. */
    @Nullable
    public Duration getInterval() {
        return this.interval;
    }

    /**
     * Sets the new interval. When non-{@code null}, it must be at least one second.
     *
     * @param interval the interval, or {@code null}
     * @return this options object for chaining
     * @throws ScheduleClientValidationException if the interval is non-{@code null} and invalid
     */
    public ScheduleUpdateOptions setInterval(@Nullable Duration interval) {
        if (interval != null) {
            Intervals.validate("", interval);
        }
        this.interval = interval;
        return this;
    }

    /** @return the new start-immediately-if-late flag, or {@code null} to leave unchanged. */
    @Nullable
    public Boolean getStartImmediatelyIfLate() {
        return this.startImmediatelyIfLate;
    }

    /**
     * Sets the new start-immediately-if-late flag.
     *
     * @param startImmediatelyIfLate the flag, or {@code null}
     * @return this options object for chaining
     */
    public ScheduleUpdateOptions setStartImmediatelyIfLate(@Nullable Boolean startImmediatelyIfLate) {
        this.startImmediatelyIfLate = startImmediatelyIfLate;
        return this;
    }
}
