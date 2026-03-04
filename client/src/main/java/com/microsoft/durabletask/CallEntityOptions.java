// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import javax.annotation.Nullable;
import java.time.Duration;

/**
 * Options for calling a durable entity and waiting for a response.
 */
public final class CallEntityOptions {
    private Duration timeout;

    /**
     * Creates a new {@code CallEntityOptions} with default settings.
     */
    public CallEntityOptions() {
    }

    /**
     * Sets the timeout for the entity call. If the entity does not respond within this duration,
     * the call will fail with a timeout exception.
     *
     * @param timeout the maximum duration to wait for a response
     * @return this {@code CallEntityOptions} object for chaining
     */
    public CallEntityOptions setTimeout(@Nullable Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Gets the timeout for the entity call, or {@code null} if no timeout is configured.
     *
     * @return the timeout duration, or {@code null}
     */
    @Nullable
    public Duration getTimeout() {
        return this.timeout;
    }
}
