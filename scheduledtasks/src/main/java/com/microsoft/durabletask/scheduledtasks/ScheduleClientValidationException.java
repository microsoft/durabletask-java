// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

/**
 * Thrown when a schedule operation fails client-side validation.
 */
public class ScheduleClientValidationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String scheduleId;

    /**
     * Creates a new {@code ScheduleClientValidationException}.
     *
     * @param scheduleId the ID of the schedule that failed validation
     * @param message    the validation error message
     */
    public ScheduleClientValidationException(String scheduleId, String message) {
        super("Validation failed for schedule '" + scheduleId + "': " + message);
        this.scheduleId = scheduleId;
    }

    /**
     * Creates a new {@code ScheduleClientValidationException} with a cause.
     *
     * @param scheduleId the ID of the schedule that failed validation
     * @param message    the validation error message
     * @param cause      the underlying cause
     */
    public ScheduleClientValidationException(String scheduleId, String message, Throwable cause) {
        super("Validation failed for schedule '" + scheduleId + "': " + message, cause);
        this.scheduleId = scheduleId;
    }

    /** @return the ID of the schedule that failed validation. */
    public String getScheduleId() {
        return this.scheduleId;
    }
}
