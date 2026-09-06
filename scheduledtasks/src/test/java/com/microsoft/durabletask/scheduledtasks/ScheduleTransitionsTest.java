// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the schedule state-transition table matches the .NET {@code ScheduleTransitions}.
 */
class ScheduleTransitionsTest {

    @Test
    void createIsValidFromAnyStatusToActive() {
        assertTrue(valid(ScheduleTransitions.CREATE_SCHEDULE, ScheduleStatus.UNINITIALIZED, ScheduleStatus.ACTIVE));
        assertTrue(valid(ScheduleTransitions.CREATE_SCHEDULE, ScheduleStatus.ACTIVE, ScheduleStatus.ACTIVE));
        assertTrue(valid(ScheduleTransitions.CREATE_SCHEDULE, ScheduleStatus.PAUSED, ScheduleStatus.ACTIVE));
    }

    @Test
    void updateIsValidWithinActiveOrPaused() {
        assertTrue(valid(ScheduleTransitions.UPDATE_SCHEDULE, ScheduleStatus.ACTIVE, ScheduleStatus.ACTIVE));
        assertTrue(valid(ScheduleTransitions.UPDATE_SCHEDULE, ScheduleStatus.PAUSED, ScheduleStatus.PAUSED));
        assertFalse(valid(ScheduleTransitions.UPDATE_SCHEDULE, ScheduleStatus.UNINITIALIZED,
                ScheduleStatus.UNINITIALIZED));
    }

    @Test
    void pauseIsValidOnlyFromActive() {
        assertTrue(valid(ScheduleTransitions.PAUSE_SCHEDULE, ScheduleStatus.ACTIVE, ScheduleStatus.PAUSED));
        assertFalse(valid(ScheduleTransitions.PAUSE_SCHEDULE, ScheduleStatus.PAUSED, ScheduleStatus.PAUSED));
        assertFalse(valid(ScheduleTransitions.PAUSE_SCHEDULE, ScheduleStatus.UNINITIALIZED, ScheduleStatus.PAUSED));
    }

    @Test
    void resumeIsValidOnlyFromPaused() {
        assertTrue(valid(ScheduleTransitions.RESUME_SCHEDULE, ScheduleStatus.PAUSED, ScheduleStatus.ACTIVE));
        assertFalse(valid(ScheduleTransitions.RESUME_SCHEDULE, ScheduleStatus.ACTIVE, ScheduleStatus.ACTIVE));
    }

    @Test
    void unknownOperationIsInvalid() {
        assertFalse(valid("NotAnOperation", ScheduleStatus.ACTIVE, ScheduleStatus.ACTIVE));
        assertFalse(valid(null, ScheduleStatus.ACTIVE, ScheduleStatus.ACTIVE));
    }

    private static boolean valid(String op, ScheduleStatus from, ScheduleStatus target) {
        return ScheduleTransitions.isValidTransition(op, from, target);
    }
}
