// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies {@link DotNetDateTimeOffset} produces the .NET {@code DateTimeOffset} round-trip ("o") format used for
 * persisted timestamps and default target-orchestration instance IDs.
 */
class DotNetDateTimeOffsetTest {

    @Test
    void formatsWithSevenFractionalDigitsAndOffset() {
        OffsetDateTime value = OffsetDateTime.of(2026, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        assertEquals("2026-01-01T00:00:00.0000000+00:00", DotNetDateTimeOffset.format(value));
    }

    @Test
    void formatsNonZeroFraction() {
        OffsetDateTime value = OffsetDateTime.of(2026, 3, 4, 5, 6, 7, 8_900_000, ZoneOffset.UTC);
        // 8_900_000 ns = 0.0089 s = 89000 ticks, rendered as the seven-digit fraction "0089000".
        assertEquals("2026-03-04T05:06:07.0089000+00:00", DotNetDateTimeOffset.format(value));
    }

    @Test
    void roundTrips() {
        OffsetDateTime value = OffsetDateTime.of(2026, 1, 2, 3, 4, 5, 6_700_000, ZoneOffset.UTC);
        String formatted = DotNetDateTimeOffset.format(value);
        assertEquals(value.toInstant(), DotNetDateTimeOffset.parse(formatted).toInstant());
    }

    @Test
    void parsesTrailingZDesignator() {
        OffsetDateTime parsed = DotNetDateTimeOffset.parse("2026-01-01T00:00:00Z");
        assertEquals(OffsetDateTime.of(2026, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant(), parsed.toInstant());
    }

    @Test
    void producesDotNetCompatibleInstanceIdSuffix() {
        OffsetDateTime value = OffsetDateTime.of(2026, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        String instanceId = "daily-report-" + DotNetDateTimeOffset.format(value);
        assertEquals("daily-report-2026-01-01T00:00:00.0000000+00:00", instanceId);
        assertTrue(instanceId.matches("daily-report-\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{7}[+-]\\d{2}:\\d{2}"));
    }
}
