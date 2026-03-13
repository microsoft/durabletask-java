// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azurefunctions;

import java.time.Duration;

/**
 * Package-private utility class for converting between Java {@link Duration} and .NET TimeSpan string format.
 * <p>
 * The .NET TimeSpan invariant format ("c") uses: {@code [-][d.]hh:mm:ss[.fffffff]}
 * <ul>
 *     <li>{@code "00:05:00"} = 5 minutes</li>
 *     <li>{@code "1.02:00:00"} = 1 day, 2 hours</li>
 *     <li>{@code "00:00:01.5000000"} = 1.5 seconds</li>
 * </ul>
 * This format is used for wire compatibility with the .NET Durable Functions host, which uses
 * Newtonsoft.Json to serialize/deserialize {@code TimeSpan} values in this format.
 */
final class TimeSpanHelper {

    private TimeSpanHelper() {
        // Static utility class — not instantiable
    }

    /**
     * Formats a {@link Duration} as a .NET TimeSpan invariant format string.
     *
     * @param duration the duration to format (must not be null or negative)
     * @return the TimeSpan format string
     * @throws IllegalArgumentException if duration is null or negative
     */
    static String format(Duration duration) {
        if (duration == null) {
            throw new IllegalArgumentException("duration must not be null");
        }
        long totalSeconds = duration.getSeconds();
        if (totalSeconds < 0) {
            throw new IllegalArgumentException("Negative durations are not supported for timeout values");
        }
        int nanos = duration.getNano();

        long days = totalSeconds / 86400;
        long hours = (totalSeconds % 86400) / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;

        StringBuilder sb = new StringBuilder();
        if (days > 0) {
            sb.append(days).append('.');
        }
        sb.append(String.format("%02d:%02d:%02d", hours, minutes, seconds));
        if (nanos > 0) {
            // .NET uses 7 fractional digits (100-nanosecond precision)
            long ticks = nanos / 100;
            sb.append(String.format(".%07d", ticks));
        }
        return sb.toString();
    }

    /**
     * Parses a .NET TimeSpan invariant format string into a {@link Duration}.
     *
     * @param timeSpan the TimeSpan format string to parse
     * @return the parsed Duration
     * @throws IllegalArgumentException if the string cannot be parsed
     */
    static Duration parse(String timeSpan) {
        if (timeSpan == null || timeSpan.trim().isEmpty()) {
            throw new IllegalArgumentException("timeSpan must not be null or empty");
        }
        String s = timeSpan.trim();

        // Handle optional negative sign
        boolean negative = s.startsWith("-");
        if (negative) {
            s = s.substring(1);
        }

        long days = 0;
        // Check for days component: "d.hh:mm:ss..."
        int firstColon = s.indexOf(':');
        int firstDot = s.indexOf('.');
        if (firstDot >= 0 && (firstColon < 0 || firstDot < firstColon)) {
            days = Long.parseLong(s.substring(0, firstDot));
            s = s.substring(firstDot + 1);
        }

        // Parse "hh:mm:ss[.fffffff]"
        String[] timeParts = s.split(":");
        if (timeParts.length != 3) {
            throw new IllegalArgumentException("Invalid TimeSpan format: " + timeSpan);
        }

        long hours = Long.parseLong(timeParts[0]);
        long minutes = Long.parseLong(timeParts[1]);

        String secPart = timeParts[2];
        long seconds;
        int nanos = 0;

        int secDot = secPart.indexOf('.');
        if (secDot >= 0) {
            seconds = Long.parseLong(secPart.substring(0, secDot));
            String frac = secPart.substring(secDot + 1);
            // Pad or truncate to 9 digits (nanoseconds)
            if (frac.length() > 9) {
                frac = frac.substring(0, 9);
            }
            while (frac.length() < 9) {
                frac = frac + "0";
            }
            nanos = Integer.parseInt(frac);
        } else {
            seconds = Long.parseLong(secPart);
        }

        long totalSeconds = days * 86400 + hours * 3600 + minutes * 60 + seconds;
        Duration d = Duration.ofSeconds(totalSeconds, nanos);
        return negative ? d.negated() : d;
    }
}
