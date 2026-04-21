// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link LargePayloadStorageOptions}.
 */
class LargePayloadStorageOptionsTest {

    @Test
    void defaults_areCorrect() {
        LargePayloadStorageOptions options = new LargePayloadStorageOptions();
        assertEquals(900_000, options.getThresholdBytes());
        assertEquals(10 * 1024 * 1024, options.getMaxPayloadBytes());
        assertNull(options.getConnectionString());
        assertNull(options.getAccountUri());
        assertNull(options.getCredential());
        assertEquals("durabletask-payloads", options.getContainerName());
        assertTrue(options.isCompressionEnabled());
    }

    @Test
    void setThresholdBytes_atOneMiB_succeeds() {
        LargePayloadStorageOptions options = new LargePayloadStorageOptions();
        options.setThresholdBytes(1024 * 1024);
        assertEquals(1024 * 1024, options.getThresholdBytes());
    }

    @Test
    void setThresholdBytes_exceedingOneMiB_throws() {
        LargePayloadStorageOptions options = new LargePayloadStorageOptions();
        assertThrows(IllegalArgumentException.class, () -> options.setThresholdBytes(1024 * 1024 + 1));
    }

    @Test
    void setThresholdBytes_zero_succeeds() {
        LargePayloadStorageOptions options = new LargePayloadStorageOptions();
        options.setThresholdBytes(0);
        assertEquals(0, options.getThresholdBytes());
    }

    @Test
    void fluentSetters_returnSameInstance() {
        LargePayloadStorageOptions options = new LargePayloadStorageOptions();
        assertSame(options, options.setThresholdBytes(500_000));
        assertSame(options, options.setMaxPayloadBytes(5 * 1024 * 1024));
        assertSame(options, options.setConnectionString("test"));
        assertSame(options, options.setContainerName("my-container"));
        assertSame(options, options.setCompressionEnabled(false));
    }

    @Test
    void setConnectionString_null_clearsValue() {
        // Null handling is consistent across all three auth setters: each accepts null
        // to clear a previously-set value. This lets callers switch from connection-string
        // auth to identity-based auth (or vice versa) without ambiguity.
        LargePayloadStorageOptions options = new LargePayloadStorageOptions();
        options.setConnectionString("UseDevelopmentStorage=true");
        assertEquals("UseDevelopmentStorage=true", options.getConnectionString());
        options.setConnectionString(null);
        assertNull(options.getConnectionString());
    }

    @Test
    void setCompressionEnabled_false_works() {
        LargePayloadStorageOptions options = new LargePayloadStorageOptions();
        options.setCompressionEnabled(false);
        assertFalse(options.isCompressionEnabled());
    }
}
