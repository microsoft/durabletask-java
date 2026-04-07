// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * Options for signaling a durable entity.
 */
public final class SignalEntityOptions {
    private Instant scheduledTime;

    /**
     * Creates a new {@code SignalEntityOptions} with default settings.
     */
    public SignalEntityOptions() {
    }

    /**
     * Sets the scheduled time for the signal. If set, the signal will be delivered at the specified time
     * rather than immediately.
     *
     * @param scheduledTime the time at which the signal should be delivered
     * @return this {@code SignalEntityOptions} object for chaining
     */
    public SignalEntityOptions setScheduledTime(@Nullable Instant scheduledTime) {
        this.scheduledTime = scheduledTime;
        return this;
    }

    /**
     * Gets the scheduled time for the signal, or {@code null} if the signal should be delivered immediately.
     *
     * @return the scheduled time, or {@code null}
     */
    @Nullable
    public Instant getScheduledTime() {
        return this.scheduledTime;
    }
}
