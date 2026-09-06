// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies {@link ScheduleStatus} ordinals and persisted-value parsing match the .NET enum.
 */
class ScheduleStatusTest {

    @Test
    void ordinalsMatchDotNet() {
        assertEquals(0, ScheduleStatus.UNINITIALIZED.toDotnetOrdinal());
        assertEquals(1, ScheduleStatus.ACTIVE.toDotnetOrdinal());
        assertEquals(2, ScheduleStatus.PAUSED.toDotnetOrdinal());
    }

    @Test
    void fromDotnetOrdinal() {
        assertEquals(ScheduleStatus.UNINITIALIZED, ScheduleStatus.fromDotnetOrdinal(0));
        assertEquals(ScheduleStatus.ACTIVE, ScheduleStatus.fromDotnetOrdinal(1));
        assertEquals(ScheduleStatus.PAUSED, ScheduleStatus.fromDotnetOrdinal(2));
        assertEquals(ScheduleStatus.UNINITIALIZED, ScheduleStatus.fromDotnetOrdinal(99));
    }

    @Test
    void fromPersistedToleratesNumbersAndLegacyNames() {
        assertEquals(ScheduleStatus.ACTIVE, ScheduleStatus.fromPersisted(1));
        assertEquals(ScheduleStatus.PAUSED, ScheduleStatus.fromPersisted("2"));
        assertEquals(ScheduleStatus.ACTIVE, ScheduleStatus.fromPersisted("Active"));
        assertEquals(ScheduleStatus.UNINITIALIZED, ScheduleStatus.fromPersisted("Unknown"));
        assertEquals(ScheduleStatus.UNINITIALIZED, ScheduleStatus.fromPersisted(null));
    }
}
