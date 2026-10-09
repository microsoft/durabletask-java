// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.Set;

/**
 * Internal schedule configuration persisted inside {@link ScheduleState}.
 * <p>
 * Field names and value shapes mirror the .NET {@code ScheduleConfiguration} so the raw entity state is readable by
 * the .NET SDK and the Durable Task Scheduler dashboard: PascalCase property names, the interval as a .NET
 * {@code TimeSpan} string, and timestamps as .NET {@code DateTimeOffset} strings. The change-detection semantics in
 * {@link #update(ScheduleUpdateOptions)} match .NET, including treating empty strings as "unspecified" and comparing
 * timestamps by instant.
 */
final class ScheduleConfiguration {

    @JsonProperty("ScheduleId")
    private String scheduleId;

    @JsonProperty("OrchestrationName")
    private String orchestrationName;

    @JsonProperty("OrchestrationInput")
    private String orchestrationInput;

    @JsonProperty("OrchestrationInstanceId")
    private String orchestrationInstanceId;

    @JsonProperty("StartAt")
    @JsonSerialize(using = ScheduleJson.OffsetDateTimeSerializer.class)
    @JsonDeserialize(using = ScheduleJson.OffsetDateTimeDeserializer.class)
    private OffsetDateTime startAt;

    @JsonProperty("EndAt")
    @JsonSerialize(using = ScheduleJson.OffsetDateTimeSerializer.class)
    @JsonDeserialize(using = ScheduleJson.OffsetDateTimeDeserializer.class)
    private OffsetDateTime endAt;

    @JsonProperty("Interval")
    @JsonSerialize(using = ScheduleJson.IntervalSerializer.class)
    @JsonDeserialize(using = ScheduleJson.IntervalDeserializer.class)
    private Duration interval;

    @JsonProperty("StartImmediatelyIfLate")
    private boolean startImmediatelyIfLate;

    /** Creates an empty {@code ScheduleConfiguration} (for deserialization). */
    ScheduleConfiguration() {
    }

    static ScheduleConfiguration fromCreateOptions(ScheduleCreationOptions options) {
        ScheduleConfiguration config = new ScheduleConfiguration();
        config.scheduleId = options.getScheduleId();
        config.orchestrationName = options.getOrchestrationName();
        config.interval = options.getInterval();
        config.orchestrationInput = options.getOrchestrationInput();
        config.orchestrationInstanceId = options.getOrchestrationInstanceId();
        config.startAt = options.getStartAt();
        config.endAt = options.getEndAt();
        config.startImmediatelyIfLate = options.isStartImmediatelyIfLate();
        config.validate();
        return config;
    }

    /**
     * Applies the update options and returns the set of changed field names (using the .NET PascalCase field names).
     * Empty string values are treated as "unspecified" and do not change the stored value.
     *
     * @param options the update options
     * @return the set of changed field names
     */
    Set<String> update(ScheduleUpdateOptions options) {
        Set<String> changed = new HashSet<>();

        if (isNotBlank(options.getOrchestrationName())
                && !options.getOrchestrationName().equals(this.orchestrationName)) {
            this.orchestrationName = options.getOrchestrationName();
            changed.add("OrchestrationName");
        }
        if (isNotBlank(options.getOrchestrationInput())
                && !options.getOrchestrationInput().equals(this.orchestrationInput)) {
            this.orchestrationInput = options.getOrchestrationInput();
            changed.add("OrchestrationInput");
        }
        if (isNotBlank(options.getOrchestrationInstanceId())
                && !options.getOrchestrationInstanceId().equals(this.orchestrationInstanceId)) {
            this.orchestrationInstanceId = options.getOrchestrationInstanceId();
            changed.add("OrchestrationInstanceId");
        }
        if (options.getStartAt() != null && !sameInstant(options.getStartAt(), this.startAt)) {
            this.startAt = options.getStartAt();
            changed.add("StartAt");
        }
        if (options.getEndAt() != null && !sameInstant(options.getEndAt(), this.endAt)) {
            this.endAt = options.getEndAt();
            changed.add("EndAt");
        }
        if (options.getInterval() != null && !options.getInterval().equals(this.interval)) {
            this.interval = options.getInterval();
            changed.add("Interval");
        }
        if (options.getStartImmediatelyIfLate() != null
                && options.getStartImmediatelyIfLate() != this.startImmediatelyIfLate) {
            this.startImmediatelyIfLate = options.getStartImmediatelyIfLate();
            changed.add("StartImmediatelyIfLate");
        }

        validate();
        return changed;
    }

    private void validate() {
        if (this.startAt != null && this.endAt != null
                && this.startAt.toInstant().isAfter(this.endAt.toInstant())) {
            throw new ScheduleClientValidationException(
                    this.scheduleId == null ? "" : this.scheduleId,
                    "startAt cannot be later than endAt.");
        }
    }

    String getScheduleId() {
        return this.scheduleId;
    }

    void setScheduleId(String scheduleId) {
        this.scheduleId = scheduleId;
    }

    String getOrchestrationName() {
        return this.orchestrationName;
    }

    void setOrchestrationName(String orchestrationName) {
        this.orchestrationName = orchestrationName;
    }

    @Nullable
    String getOrchestrationInput() {
        return this.orchestrationInput;
    }

    void setOrchestrationInput(@Nullable String orchestrationInput) {
        this.orchestrationInput = orchestrationInput;
    }

    @Nullable
    String getOrchestrationInstanceId() {
        return this.orchestrationInstanceId;
    }

    void setOrchestrationInstanceId(@Nullable String orchestrationInstanceId) {
        this.orchestrationInstanceId = orchestrationInstanceId;
    }

    @Nullable
    OffsetDateTime getStartAt() {
        return this.startAt;
    }

    void setStartAt(@Nullable OffsetDateTime startAt) {
        this.startAt = startAt;
    }

    @Nullable
    OffsetDateTime getEndAt() {
        return this.endAt;
    }

    void setEndAt(@Nullable OffsetDateTime endAt) {
        this.endAt = endAt;
    }

    Duration getInterval() {
        return this.interval;
    }

    void setInterval(Duration interval) {
        this.interval = interval;
    }

    boolean isStartImmediatelyIfLate() {
        return this.startImmediatelyIfLate;
    }

    void setStartImmediatelyIfLate(boolean startImmediatelyIfLate) {
        this.startImmediatelyIfLate = startImmediatelyIfLate;
    }

    private static boolean isNotBlank(String value) {
        return value != null && !value.isEmpty();
    }

    private static boolean sameInstant(@Nullable OffsetDateTime a, @Nullable OffsetDateTime b) {
        if (a == null || b == null) {
            return a == b;
        }
        Instant ia = a.toInstant();
        Instant ib = b.toInstant();
        return ia.equals(ib);
    }
}
