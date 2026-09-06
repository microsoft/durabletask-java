// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies {@link DotNetTimeSpan} round-trips the .NET {@code TimeSpan} constant format, including the vectors
 * asserted by the .NET SDK tests.
 */
class DotNetTimeSpanTest {

    @Test
    void formatsDotNetVectors() {
        assertEquals("00:00:01", DotNetTimeSpan.format(Duration.ofSeconds(1)));
        assertEquals("01:00:00", DotNetTimeSpan.format(Duration.ofHours(1)));
        assertEquals("1.02:03:04", DotNetTimeSpan.format(
                Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4)));
        assertEquals("00:00:01.5000000", DotNetTimeSpan.format(
                Duration.ofSeconds(1).plusMillis(500)));
    }

    @Test
    void roundTripsDotNetVectors() {
        assertRoundTrip(Duration.ofSeconds(1), "00:00:01");
        assertRoundTrip(Duration.ofHours(1), "01:00:00");
        assertRoundTrip(Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4), "1.02:03:04");
        assertRoundTrip(Duration.ofSeconds(1).plusMillis(500), "00:00:01.5000000");
    }

    @Test
    void truncatesSubTickPrecision() {
        // 150 nanoseconds is one and a half ticks; .NET TimeSpan resolution is whole 100-ns ticks.
        Duration value = Duration.ofSeconds(1).plusNanos(150);
        assertEquals("00:00:01.0000001", DotNetTimeSpan.format(value));
    }

    @Test
    void handlesNegative() {
        assertEquals("-00:00:01", DotNetTimeSpan.format(Duration.ofSeconds(-1)));
        assertEquals(Duration.ofSeconds(-1), DotNetTimeSpan.parse("-00:00:01"));
    }

    private static void assertRoundTrip(Duration duration, String expected) {
        String formatted = DotNetTimeSpan.format(duration);
        assertEquals(expected, formatted);
        assertEquals(duration, DotNetTimeSpan.parse(formatted));
    }
}
