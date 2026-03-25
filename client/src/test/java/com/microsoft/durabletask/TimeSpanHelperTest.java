// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link TimeSpanHelper}.
 */
class TimeSpanHelperTest {

    // ---- format tests ----

    @Test
    void formatSimpleMinutes() {
        assertEquals("00:05:00", TimeSpanHelper.format(Duration.ofMinutes(5)));
    }

    @Test
    void formatSimpleSeconds() {
        assertEquals("00:00:30", TimeSpanHelper.format(Duration.ofSeconds(30)));
    }

    @Test
    void formatHoursMinutesSeconds() {
        Duration d = Duration.ofHours(1).plusMinutes(30).plusSeconds(45);
        assertEquals("01:30:45", TimeSpanHelper.format(d));
    }

    @Test
    void formatWithDays() {
        Duration d = Duration.ofDays(1).plusHours(2);
        assertEquals("1.02:00:00", TimeSpanHelper.format(d));
    }

    @Test
    void formatMultipleDays() {
        Duration d = Duration.ofDays(6);
        assertEquals("6.00:00:00", TimeSpanHelper.format(d));
    }

    @Test
    void formatWithFractionalSeconds() {
        Duration d = Duration.ofSeconds(1, 500_000_000); // 1.5 seconds
        assertEquals("00:00:01.5000000", TimeSpanHelper.format(d));
    }

    @Test
    void formatWithMilliseconds() {
        Duration d = Duration.ofMillis(100);
        assertEquals("00:00:00.1000000", TimeSpanHelper.format(d));
    }

    @Test
    void formatZeroDuration() {
        assertEquals("00:00:00", TimeSpanHelper.format(Duration.ZERO));
    }

    @Test
    void formatOneDayExact() {
        assertEquals("1.00:00:00", TimeSpanHelper.format(Duration.ofDays(1)));
    }

    @Test
    void formatNullDurationThrows() {
        assertThrows(IllegalArgumentException.class, () -> TimeSpanHelper.format(null));
    }

    @Test
    void formatNegativeDurationThrows() {
        assertThrows(IllegalArgumentException.class, () -> TimeSpanHelper.format(Duration.ofSeconds(-1)));
    }

    // ---- parse tests ----

    @Test
    void parseSimpleMinutes() {
        assertEquals(Duration.ofMinutes(5), TimeSpanHelper.parse("00:05:00"));
    }

    @Test
    void parseSimpleSeconds() {
        assertEquals(Duration.ofSeconds(30), TimeSpanHelper.parse("00:00:30"));
    }

    @Test
    void parseHoursMinutesSeconds() {
        assertEquals(Duration.ofHours(1).plusMinutes(30).plusSeconds(45),
                TimeSpanHelper.parse("01:30:45"));
    }

    @Test
    void parseWithDays() {
        assertEquals(Duration.ofDays(1).plusHours(2), TimeSpanHelper.parse("1.02:00:00"));
    }

    @Test
    void parseMultipleDays() {
        assertEquals(Duration.ofDays(6), TimeSpanHelper.parse("6.00:00:00"));
    }

    @Test
    void parseWithFractionalSeconds() {
        assertEquals(Duration.ofSeconds(1, 500_000_000), TimeSpanHelper.parse("00:00:01.5000000"));
    }

    @Test
    void parseWithMilliseconds() {
        assertEquals(Duration.ofMillis(100), TimeSpanHelper.parse("00:00:00.1000000"));
    }

    @Test
    void parseZero() {
        assertEquals(Duration.ZERO, TimeSpanHelper.parse("00:00:00"));
    }

    @Test
    void parseNegative() {
        assertEquals(Duration.ofMinutes(5).negated(), TimeSpanHelper.parse("-00:05:00"));
    }

    @Test
    void parseNullThrows() {
        assertThrows(IllegalArgumentException.class, () -> TimeSpanHelper.parse(null));
    }

    @Test
    void parseEmptyStringThrows() {
        assertThrows(IllegalArgumentException.class, () -> TimeSpanHelper.parse(""));
    }

    @Test
    void parseInvalidFormatThrows() {
        assertThrows(IllegalArgumentException.class, () -> TimeSpanHelper.parse("not-a-timespan"));
    }

    // ---- round-trip tests ----

    @Test
    void roundTripSimpleDuration() {
        Duration original = Duration.ofMinutes(15);
        assertEquals(original, TimeSpanHelper.parse(TimeSpanHelper.format(original)));
    }

    @Test
    void roundTripComplexDuration() {
        Duration original = Duration.ofDays(2).plusHours(5).plusMinutes(30).plusSeconds(15);
        assertEquals(original, TimeSpanHelper.parse(TimeSpanHelper.format(original)));
    }

    @Test
    void roundTripWithNanos() {
        // Note: round-trip precision is 100-nanosecond (7 digits) due to .NET TimeSpan format
        Duration original = Duration.ofSeconds(0, 123_456_700);
        assertEquals(original, TimeSpanHelper.parse(TimeSpanHelper.format(original)));
    }
}
