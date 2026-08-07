// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

/**
 * Thrown when an operation is not valid for the schedule's current status.
 */
public class ScheduleInvalidTransitionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String scheduleId;
    private final ScheduleStatus fromStatus;
    private final ScheduleStatus toStatus;
    private final String operationName;

    /**
     * Creates a new {@code ScheduleInvalidTransitionException}.
     *
     * @param scheduleId    the ID of the schedule on which the invalid transition was attempted
     * @param fromStatus    the current status of the schedule
     * @param toStatus      the target status that was invalid
     * @param operationName the name of the operation that was attempted
     */
    public ScheduleInvalidTransitionException(
            String scheduleId,
            ScheduleStatus fromStatus,
            ScheduleStatus toStatus,
            String operationName) {
        super("Invalid state transition for schedule '" + scheduleId + "': operation '" + operationName
                + "' cannot transition from " + fromStatus + " to " + toStatus + ".");
        this.scheduleId = scheduleId;
        this.fromStatus = fromStatus;
        this.toStatus = toStatus;
        this.operationName = operationName;
    }

    /** @return the ID of the schedule that encountered the invalid transition. */
    public String getScheduleId() {
        return this.scheduleId;
    }

    /** @return the status the schedule was transitioning from. */
    public ScheduleStatus getFromStatus() {
        return this.fromStatus;
    }

    /** @return the invalid target status that was attempted. */
    public ScheduleStatus getToStatus() {
        return this.toStatus;
    }

    /** @return the name of the operation that was attempted. */
    public String getOperationName() {
        return this.operationName;
    }
}
