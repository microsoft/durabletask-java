// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies validation and partial-update semantics of the schedule option models and internal configuration.
 */
class ScheduleOptionsTest {

    @Test
    void creationRequiresScheduleId() {
        assertThrows(ScheduleClientValidationException.class,
                () -> new ScheduleCreationOptions("", "orch", Duration.ofSeconds(30)));
    }

    @Test
    void creationRequiresOrchestrationName() {
        assertThrows(ScheduleClientValidationException.class,
                () -> new ScheduleCreationOptions("s1", "", Duration.ofSeconds(30)));
    }

    @Test
    void creationRejectsSubSecondInterval() {
        assertThrows(ScheduleClientValidationException.class,
                () -> new ScheduleCreationOptions("s1", "orch", Duration.ofMillis(500)));
    }

    @Test
    void creationRejectsNonPositiveInterval() {
        assertThrows(ScheduleClientValidationException.class,
                () -> new ScheduleCreationOptions("s1", "orch", Duration.ofSeconds(-1)));
        assertThrows(ScheduleClientValidationException.class,
                () -> new ScheduleCreationOptions("s1", "orch", Duration.ZERO));
    }

    @Test
    void creationAcceptsOneSecond() {
        ScheduleCreationOptions options = new ScheduleCreationOptions("s1", "orch", Duration.ofSeconds(1));
        assertEquals("s1", options.getScheduleId());
        assertEquals(Duration.ofSeconds(1), options.getInterval());
    }

    @Test
    void updateRejectsInvalidInterval() {
        assertThrows(ScheduleClientValidationException.class,
                () -> new ScheduleUpdateOptions().setInterval(Duration.ofMillis(100)));
    }

    @Test
    void configurationRejectsStartAfterEnd() {
        ScheduleCreationOptions options = new ScheduleCreationOptions("s1", "orch", Duration.ofSeconds(30))
                .setStartAt(OffsetDateTime.of(2026, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC))
                .setEndAt(OffsetDateTime.of(2026, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC));
        assertThrows(ScheduleClientValidationException.class,
                () -> ScheduleConfiguration.fromCreateOptions(options));
    }

    @Test
    void updateReturnsChangedFields() {
        ScheduleConfiguration config = ScheduleConfiguration.fromCreateOptions(
                new ScheduleCreationOptions("s1", "orch", Duration.ofSeconds(5)));
        Set<String> changed = config.update(new ScheduleUpdateOptions()
                .setInterval(Duration.ofSeconds(10))
                .setOrchestrationName("orch2"));
        assertTrue(changed.contains("Interval"));
        assertTrue(changed.contains("OrchestrationName"));
        assertEquals(Duration.ofSeconds(10), config.getInterval());
        assertEquals("orch2", config.getOrchestrationName());
    }

    @Test
    void updateWithSameValuesReportsNoChanges() {
        ScheduleConfiguration config = ScheduleConfiguration.fromCreateOptions(
                new ScheduleCreationOptions("s1", "orch", Duration.ofSeconds(5)));
        Set<String> changed = config.update(new ScheduleUpdateOptions().setOrchestrationName("orch"));
        assertTrue(changed.isEmpty());
    }

    @Test
    void updateTreatsEmptyStringAsUnspecified() {
        ScheduleConfiguration config = ScheduleConfiguration.fromCreateOptions(
                new ScheduleCreationOptions("s1", "orch", Duration.ofSeconds(5))
                        .setOrchestrationInput("original"));
        Set<String> changed = config.update(new ScheduleUpdateOptions()
                .setOrchestrationName("")
                .setOrchestrationInput(""));
        assertTrue(changed.isEmpty());
        assertEquals("orch", config.getOrchestrationName());
        assertEquals("original", config.getOrchestrationInput());
    }

    @Test
    void updateComparesTimestampsByInstant() {
        OffsetDateTime utc = OffsetDateTime.of(2026, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        ScheduleConfiguration config = ScheduleConfiguration.fromCreateOptions(
                new ScheduleCreationOptions("s1", "orch", Duration.ofSeconds(5)).setStartAt(utc));
        // Same instant expressed with a different offset must be treated as no change.
        OffsetDateTime sameInstantOtherOffset = utc.withOffsetSameInstant(ZoneOffset.ofHours(5));
        Set<String> changed = config.update(new ScheduleUpdateOptions().setStartAt(sameInstantOtherOffset));
        assertFalse(changed.contains("StartAt"));
    }
}
