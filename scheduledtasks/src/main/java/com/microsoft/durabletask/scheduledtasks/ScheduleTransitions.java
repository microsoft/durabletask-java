// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

/**
 * Valid state-transition rules for schedules and the operation-name constants used by the {@link Schedule} entity.
 * <p>
 * The operation-name constants match the .NET operation names and are dispatched to the {@link Schedule} entity
 * methods case-insensitively. The transition table is a direct port of the .NET {@code ScheduleTransitions}.
 */
final class ScheduleTransitions {

    static final String CREATE_SCHEDULE = "CreateSchedule";
    static final String UPDATE_SCHEDULE = "UpdateSchedule";
    static final String PAUSE_SCHEDULE = "PauseSchedule";
    static final String RESUME_SCHEDULE = "ResumeSchedule";
    static final String RUN_SCHEDULE = "RunSchedule";
    static final String DELETE = "delete";

    private ScheduleTransitions() {
    }

    /**
     * Checks whether a transition to the target status is valid for the given operation and current status.
     *
     * @param operationName the operation being performed
     * @param from          the current schedule status
     * @param target        the target status
     * @return {@code true} if the transition is valid; otherwise {@code false}
     */
    static boolean isValidTransition(String operationName, ScheduleStatus from, ScheduleStatus target) {
        if (operationName == null) {
            return false;
        }
        switch (operationName) {
            case CREATE_SCHEDULE:
                return target == ScheduleStatus.ACTIVE
                        && (from == ScheduleStatus.UNINITIALIZED
                            || from == ScheduleStatus.ACTIVE
                            || from == ScheduleStatus.PAUSED);
            case UPDATE_SCHEDULE:
                return (from == ScheduleStatus.ACTIVE && target == ScheduleStatus.ACTIVE)
                        || (from == ScheduleStatus.PAUSED && target == ScheduleStatus.PAUSED);
            case PAUSE_SCHEDULE:
                return from == ScheduleStatus.ACTIVE && target == ScheduleStatus.PAUSED;
            case RESUME_SCHEDULE:
                return from == ScheduleStatus.PAUSED && target == ScheduleStatus.ACTIVE;
            default:
                return false;
        }
    }
}
