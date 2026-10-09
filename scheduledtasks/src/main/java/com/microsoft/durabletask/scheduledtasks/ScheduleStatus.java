// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

/**
 * Represents the current status of a schedule.
 * <p>
 * The numeric values returned by {@link #toDotnetOrdinal()} match the .NET {@code ScheduleStatus} enum ordinals so
 * that persisted entity state is interchangeable with the .NET SDK and readable by the Durable Task Scheduler
 * dashboard.
 */
public enum ScheduleStatus {
    /** The schedule has not been created. */
    UNINITIALIZED(0),

    /** The schedule is active and eligible to run. */
    ACTIVE(1),

    /** The schedule is paused and will not run until resumed. */
    PAUSED(2);

    private final int dotnetOrdinal;

    ScheduleStatus(int dotnetOrdinal) {
        this.dotnetOrdinal = dotnetOrdinal;
    }

    /**
     * Gets the numeric value used by the .NET {@code ScheduleStatus} enum
     * ({@code Uninitialized = 0}, {@code Active = 1}, {@code Paused = 2}).
     *
     * @return the .NET-compatible ordinal
     */
    public int toDotnetOrdinal() {
        return this.dotnetOrdinal;
    }

    /**
     * Reconstructs a status from a persisted numeric ordinal, tolerating unknown values by returning
     * {@link #UNINITIALIZED}.
     *
     * @param value the .NET ordinal
     * @return the corresponding status
     */
    public static ScheduleStatus fromDotnetOrdinal(int value) {
        switch (value) {
            case 1:
                return ACTIVE;
            case 2:
                return PAUSED;
            default:
                return UNINITIALIZED;
        }
    }

    /**
     * Reconstructs a status from a persisted value that may be a numeric ordinal or a legacy string name.
     *
     * @param value the persisted value (number or string), may be {@code null}
     * @return the corresponding status, or {@link #UNINITIALIZED} when unrecognized
     */
    public static ScheduleStatus fromPersisted(Object value) {
        if (value instanceof Number) {
            return fromDotnetOrdinal(((Number) value).intValue());
        }
        if (value instanceof String) {
            String text = ((String) value).trim();
            if (text.isEmpty()) {
                return UNINITIALIZED;
            }
            try {
                return fromDotnetOrdinal(Integer.parseInt(text));
            } catch (NumberFormatException ignored) {
                for (ScheduleStatus status : values()) {
                    if (status.name().equalsIgnoreCase(text)) {
                        return status;
                    }
                }
                // The .NET Scheduler client names the zero value "Unknown"; treat it as uninitialized.
                if ("unknown".equalsIgnoreCase(text)) {
                    return UNINITIALIZED;
                }
            }
        }
        return UNINITIALIZED;
    }
}
