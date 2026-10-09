// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Internal runtime state persisted inside the {@link Schedule} entity.
 * <p>
 * Field names and value shapes mirror the .NET {@code ScheduleState} so the raw entity state is readable by the
 * .NET SDK and the Durable Task Scheduler dashboard: PascalCase names, the status as its numeric ordinal, timestamps
 * as .NET {@code DateTimeOffset} strings, and the nested configuration as a plain object.
 */
final class ScheduleState {

    @JsonProperty("Status")
    @JsonSerialize(using = ScheduleJson.StatusSerializer.class)
    @JsonDeserialize(using = ScheduleJson.StatusDeserializer.class)
    private ScheduleStatus status = ScheduleStatus.UNINITIALIZED;

    @JsonProperty("ExecutionToken")
    private String executionToken = newToken();

    @JsonProperty("LastRunAt")
    @JsonSerialize(using = ScheduleJson.OffsetDateTimeSerializer.class)
    @JsonDeserialize(using = ScheduleJson.OffsetDateTimeDeserializer.class)
    private OffsetDateTime lastRunAt;

    @JsonProperty("NextRunAt")
    @JsonSerialize(using = ScheduleJson.OffsetDateTimeSerializer.class)
    @JsonDeserialize(using = ScheduleJson.OffsetDateTimeDeserializer.class)
    private OffsetDateTime nextRunAt;

    @JsonProperty("ScheduleCreatedAt")
    @JsonSerialize(using = ScheduleJson.OffsetDateTimeSerializer.class)
    @JsonDeserialize(using = ScheduleJson.OffsetDateTimeDeserializer.class)
    private OffsetDateTime scheduleCreatedAt;

    @JsonProperty("ScheduleLastModifiedAt")
    @JsonSerialize(using = ScheduleJson.OffsetDateTimeSerializer.class)
    @JsonDeserialize(using = ScheduleJson.OffsetDateTimeDeserializer.class)
    private OffsetDateTime scheduleLastModifiedAt;

    @JsonProperty("ScheduleConfiguration")
    private ScheduleConfiguration scheduleConfiguration;

    /** Creates a fresh, uninitialized {@code ScheduleState} with a new execution token. */
    ScheduleState() {
    }

    ScheduleStatus getStatus() {
        return this.status;
    }

    void setStatus(ScheduleStatus status) {
        this.status = status;
    }

    String getExecutionToken() {
        return this.executionToken;
    }

    void setExecutionToken(String executionToken) {
        this.executionToken = executionToken;
    }

    /** Generates a new execution token, invalidating any pending run signals. */
    void refreshExecutionToken() {
        this.executionToken = newToken();
    }

    @Nullable
    OffsetDateTime getLastRunAt() {
        return this.lastRunAt;
    }

    void setLastRunAt(@Nullable OffsetDateTime lastRunAt) {
        this.lastRunAt = lastRunAt;
    }

    @Nullable
    OffsetDateTime getNextRunAt() {
        return this.nextRunAt;
    }

    void setNextRunAt(@Nullable OffsetDateTime nextRunAt) {
        this.nextRunAt = nextRunAt;
    }

    @Nullable
    OffsetDateTime getScheduleCreatedAt() {
        return this.scheduleCreatedAt;
    }

    void setScheduleCreatedAt(@Nullable OffsetDateTime scheduleCreatedAt) {
        this.scheduleCreatedAt = scheduleCreatedAt;
    }

    @Nullable
    OffsetDateTime getScheduleLastModifiedAt() {
        return this.scheduleLastModifiedAt;
    }

    void setScheduleLastModifiedAt(@Nullable OffsetDateTime scheduleLastModifiedAt) {
        this.scheduleLastModifiedAt = scheduleLastModifiedAt;
    }

    @Nullable
    ScheduleConfiguration getScheduleConfiguration() {
        return this.scheduleConfiguration;
    }

    void setScheduleConfiguration(@Nullable ScheduleConfiguration scheduleConfiguration) {
        this.scheduleConfiguration = scheduleConfiguration;
    }

    private static String newToken() {
        // Match the .NET Guid "N" format: 32 lowercase hex digits, no separators.
        return UUID.randomUUID().toString().replace("-", "");
    }
}
