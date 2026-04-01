// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for LargePayloadOptions.
 */
public class LargePayloadOptionsTest {

    @Test
    void defaults_matchExpectedValues() {
        LargePayloadOptions options = new LargePayloadOptions.Builder().build();
        assertEquals(900_000, options.getThresholdBytes());
        assertEquals(10 * 1024 * 1024, options.getMaxExternalizedPayloadBytes());
    }

    @Test
    void customValues_areRespected() {
        LargePayloadOptions options = new LargePayloadOptions.Builder()
            .setThresholdBytes(500)
            .setMaxExternalizedPayloadBytes(5000)
            .build();
        assertEquals(500, options.getThresholdBytes());
        assertEquals(5000, options.getMaxExternalizedPayloadBytes());
    }

    @Test
    void negativeThreshold_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> new LargePayloadOptions.Builder().setThresholdBytes(-1));
    }

    @Test
    void thresholdExceeds1MiB_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> new LargePayloadOptions.Builder().setThresholdBytes(1_048_577));
    }

    @Test
    void nonPositiveMax_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> new LargePayloadOptions.Builder().setMaxExternalizedPayloadBytes(0));
    }

    @Test
    void thresholdEqualToMax_throws() {
        assertThrows(IllegalStateException.class,
            () -> new LargePayloadOptions.Builder()
                .setThresholdBytes(100)
                .setMaxExternalizedPayloadBytes(100)
                .build());
    }

    @Test
    void thresholdGreaterThanMax_throws() {
        assertThrows(IllegalStateException.class,
            () -> new LargePayloadOptions.Builder()
                .setThresholdBytes(200)
                .setMaxExternalizedPayloadBytes(100)
                .build());
    }
}
