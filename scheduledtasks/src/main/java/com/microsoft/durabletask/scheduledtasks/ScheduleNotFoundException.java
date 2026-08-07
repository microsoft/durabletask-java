// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

/**
 * Thrown when attempting to access a schedule that does not exist.
 */
public class ScheduleNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String scheduleId;

    /**
     * Creates a new {@code ScheduleNotFoundException}.
     *
     * @param scheduleId the ID of the schedule that was not found
     */
    public ScheduleNotFoundException(String scheduleId) {
        super("Schedule with ID '" + scheduleId + "' was not found.");
        this.scheduleId = scheduleId;
    }

    /** @return the ID of the schedule that was not found. */
    public String getScheduleId() {
        return this.scheduleId;
    }
}
