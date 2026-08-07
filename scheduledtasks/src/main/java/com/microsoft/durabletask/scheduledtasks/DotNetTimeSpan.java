// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import java.time.Duration;

/**
 * Converts between {@link Duration} and the .NET {@code TimeSpan} constant ("c") string format
 * {@code [-][d.]hh:mm:ss[.fffffff]} used when persisting the schedule interval.
 * <p>
 * System.Text.Json (the .NET default entity-state converter) serializes {@code TimeSpan} in this exact format, so
 * matching it keeps persisted state readable by the .NET SDK and the Durable Task Scheduler dashboard. The
 * fractional component uses seven digits (100-nanosecond ticks); any finer precision is truncated to whole ticks,
 * matching the .NET {@code TimeSpan} resolution.
 */
final class DotNetTimeSpan {

    private static final long NANOS_PER_TICK = 100L;
    private static final long TICKS_PER_SECOND = 10_000_000L;

    private DotNetTimeSpan() {
    }

    /**
     * Formats a {@link Duration} as a .NET {@code TimeSpan} constant string.
     *
     * @param value the duration
     * @return the formatted string
     */
    static String format(Duration value) {
        boolean negative = value.isNegative();
        Duration abs = value.abs();
        long totalSeconds = abs.getSeconds();
        int nanos = abs.getNano();

        long days = totalSeconds / 86_400L;
        long remainder = totalSeconds % 86_400L;
        long hours = remainder / 3_600L;
        long minutes = (remainder % 3_600L) / 60L;
        long seconds = remainder % 60L;

        StringBuilder sb = new StringBuilder();
        if (days != 0) {
            sb.append(days).append('.');
        }
        sb.append(String.format("%02d:%02d:%02d", hours, minutes, seconds));
        if (nanos != 0) {
            long ticks = nanos / NANOS_PER_TICK;
            sb.append('.').append(String.format("%07d", ticks));
        }
        return negative ? "-" + sb : sb.toString();
    }

    /**
     * Parses a .NET {@code TimeSpan} constant string into a {@link Duration}.
     *
     * @param value the .NET {@code TimeSpan} string
     * @return the parsed duration
     */
    static Duration parse(String value) {
        String text = value.trim();
        boolean negative = text.startsWith("-");
        if (negative) {
            text = text.substring(1);
        }

        long days;
        long hours;
        long minutes;
        long seconds;
        long fractionTicks = 0;

        int lastDot = text.lastIndexOf('.');
        if (lastDot >= 0) {
            String tail = text.substring(lastDot + 1);
            if (tail.indexOf(':') >= 0) {
                // The dot separates days from the clock, e.g. "1.02:03:04".
                int firstDot = text.indexOf('.');
                days = Long.parseLong(text.substring(0, firstDot));
                long[] hms = splitHms(text.substring(firstDot + 1));
                hours = hms[0];
                minutes = hms[1];
                seconds = hms[2];
            } else {
                // The tail is the fractional-ticks component.
                String padded = (tail + "0000000").substring(0, 7);
                fractionTicks = Long.parseLong(padded);
                long[] dhms = splitClock(text.substring(0, lastDot));
                days = dhms[0];
                hours = dhms[1];
                minutes = dhms[2];
                seconds = dhms[3];
            }
        } else {
            long[] dhms = splitClock(text);
            days = dhms[0];
            hours = dhms[1];
            minutes = dhms[2];
            seconds = dhms[3];
        }

        Duration result = Duration.ofDays(days)
                .plusHours(hours)
                .plusMinutes(minutes)
                .plusSeconds(seconds)
                .plusNanos(fractionTicks * NANOS_PER_TICK);
        return negative ? result.negated() : result;
    }

    /**
     * Returns the whole number of 100-nanosecond ticks represented by the duration, used for .NET-compatible
     * interval arithmetic.
     *
     * @param value the duration
     * @return the number of ticks (may be negative)
     */
    static long toTicks(Duration value) {
        return value.getSeconds() * TICKS_PER_SECOND + value.getNano() / NANOS_PER_TICK;
    }

    private static long[] splitClock(String text) {
        long days = 0;
        int dot = text.indexOf('.');
        if (dot >= 0) {
            days = Long.parseLong(text.substring(0, dot));
            text = text.substring(dot + 1);
        }
        long[] hms = splitHms(text);
        return new long[] {days, hms[0], hms[1], hms[2]};
    }

    private static long[] splitHms(String text) {
        String[] parts = text.split(":");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid TimeSpan clock component: " + text);
        }
        return new long[] {
                Long.parseLong(parts[0]),
                Long.parseLong(parts[1]),
                Long.parseLong(parts[2])
        };
    }
}
