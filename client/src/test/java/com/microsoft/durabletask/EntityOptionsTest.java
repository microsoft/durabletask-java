// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SignalEntityOptions} and {@link CallEntityOptions}.
 */
public class EntityOptionsTest {

    // region SignalEntityOptions tests

    @Test
    void signalEntityOptions_default_scheduledTimeIsNull() {
        SignalEntityOptions options = new SignalEntityOptions();
        assertNull(options.getScheduledTime());
    }

    @Test
    void signalEntityOptions_setScheduledTime_roundTrip() {
        Instant time = Instant.parse("2025-06-15T10:00:00Z");
        SignalEntityOptions options = new SignalEntityOptions().setScheduledTime(time);
        assertEquals(time, options.getScheduledTime());
    }

    @Test
    void signalEntityOptions_setScheduledTime_null_resetsToNull() {
        Instant time = Instant.parse("2025-06-15T10:00:00Z");
        SignalEntityOptions options = new SignalEntityOptions()
                .setScheduledTime(time)
                .setScheduledTime(null);
        assertNull(options.getScheduledTime());
    }

    @Test
    void signalEntityOptions_fluentChaining_returnsSameInstance() {
        SignalEntityOptions options = new SignalEntityOptions();
        assertSame(options, options.setScheduledTime(Instant.now()));
    }

    // endregion

    // region CallEntityOptions tests

    @Test
    void callEntityOptions_default_timeoutIsNull() {
        CallEntityOptions options = new CallEntityOptions();
        assertNull(options.getTimeout());
    }

    @Test
    void callEntityOptions_setTimeout_roundTrip() {
        Duration timeout = Duration.ofSeconds(30);
        CallEntityOptions options = new CallEntityOptions().setTimeout(timeout);
        assertEquals(timeout, options.getTimeout());
    }

    @Test
    void callEntityOptions_setTimeout_null_resetsToNull() {
        CallEntityOptions options = new CallEntityOptions()
                .setTimeout(Duration.ofSeconds(30))
                .setTimeout(null);
        assertNull(options.getTimeout());
    }

    @Test
    void callEntityOptions_setTimeout_zero_allowed() {
        CallEntityOptions options = new CallEntityOptions().setTimeout(Duration.ZERO);
        assertEquals(Duration.ZERO, options.getTimeout());
    }

    @Test
    void callEntityOptions_fluentChaining_returnsSameInstance() {
        CallEntityOptions options = new CallEntityOptions();
        assertSame(options, options.setTimeout(Duration.ofMinutes(5)));
    }

    // endregion
}
