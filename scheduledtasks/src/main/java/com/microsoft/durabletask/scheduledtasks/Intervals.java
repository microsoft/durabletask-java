// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import java.time.Duration;

/**
 * Interval validation shared by creation and update options and the internal configuration.
 * <p>
 * The interval must be positive and at least one second, matching the .NET SDK.
 */
final class Intervals {

    static final Duration MINIMUM = Duration.ofSeconds(1);

    private Intervals() {
    }

    static void validate(String scheduleId, Duration interval) {
        if (interval == null) {
            throw new ScheduleClientValidationException(nullToEmpty(scheduleId), "interval must not be null.");
        }
        if (interval.isZero() || interval.isNegative()) {
            throw new ScheduleClientValidationException(nullToEmpty(scheduleId), "interval must be positive.");
        }
        if (interval.compareTo(MINIMUM) < 0) {
            throw new ScheduleClientValidationException(nullToEmpty(scheduleId),
                    "interval must be at least one second.");
        }
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
