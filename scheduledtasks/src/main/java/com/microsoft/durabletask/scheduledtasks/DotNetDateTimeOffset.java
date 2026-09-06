// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;

/**
 * Converts between {@link OffsetDateTime} and the .NET {@code DateTimeOffset} round-trip ("o") string format
 * {@code yyyy-MM-ddTHH:mm:ss.fffffff+00:00} (seven fractional digits, explicit numeric offset).
 * <p>
 * This exact format is required in two places for .NET parity: the persisted schedule timestamps, and the default
 * target-orchestration instance ID {@code {scheduleId}-{scheduledRunTime:o}}. {@code OffsetDateTime#toString()} is
 * not equivalent because it uses variable fractional precision and a {@code Z} designator, which would produce a
 * different identifier for an equivalent instant.
 */
final class DotNetDateTimeOffset {

    // Seven fractional digits (100-ns ticks), always present, with an explicit "+HH:MM" offset.
    private static final DateTimeFormatter ROUND_TRIP = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 7, 7, true)
            .appendOffset("+HH:MM", "+00:00")
            .toFormatter(Locale.ROOT);

    private DotNetDateTimeOffset() {
    }

    /**
     * Formats an {@link OffsetDateTime} as a .NET {@code DateTimeOffset} round-trip string.
     *
     * @param value the value to format
     * @return the formatted string
     */
    static String format(OffsetDateTime value) {
        return value.format(ROUND_TRIP);
    }

    /**
     * Parses a .NET {@code DateTimeOffset} round-trip string into an {@link OffsetDateTime}. Also accepts standard
     * ISO-8601 offset date-times (including a trailing {@code Z}).
     *
     * @param value the string to parse
     * @return the parsed value
     */
    static OffsetDateTime parse(String value) {
        return OffsetDateTime.parse(value.trim());
    }
}
