// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PayloadHelper.
 */
public class PayloadHelperTest {

    /**
     * A simple in-memory PayloadStore for testing.
     */
    private static class InMemoryPayloadStore implements PayloadStore {
        private static final String TOKEN_PREFIX = "inmemory://";
        private final java.util.Map<String, String> blobs = new java.util.HashMap<>();
        private int uploadCount = 0;
        private int downloadCount = 0;

        @Override
        public String upload(String payload) {
            uploadCount++;
            String token = TOKEN_PREFIX + java.util.UUID.randomUUID().toString();
            blobs.put(token, payload);
            return token;
        }

        @Override
        public String download(String token) {
            downloadCount++;
            String value = blobs.get(token);
            if (value == null) {
                throw new IllegalArgumentException("Unknown token: " + token);
            }
            return value;
        }

        @Override
        public boolean isKnownPayloadToken(String value) {
            return value != null && value.startsWith(TOKEN_PREFIX);
        }

        int getUploadCount() { return uploadCount; }
        int getDownloadCount() { return downloadCount; }
    }

    @Test
    void maybeExternalize_nullValue_returnsNull() {
        InMemoryPayloadStore store = new InMemoryPayloadStore();
        PayloadHelper helper = new PayloadHelper(store, defaultOptions());
        assertNull(helper.maybeExternalize(null));
        assertEquals(0, store.getUploadCount());
    }

    @Test
    void maybeExternalize_emptyValue_returnsEmpty() {
        InMemoryPayloadStore store = new InMemoryPayloadStore();
        PayloadHelper helper = new PayloadHelper(store, defaultOptions());
        assertEquals("", helper.maybeExternalize(""));
        assertEquals(0, store.getUploadCount());
    }

    @Test
    void maybeExternalize_belowThreshold_returnsOriginal() {
        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder()
            .setThresholdBytes(100)
            .setMaxExternalizedPayloadBytes(1000)
            .build();
        PayloadHelper helper = new PayloadHelper(store, options);

        String smallPayload = "hello";
        assertEquals(smallPayload, helper.maybeExternalize(smallPayload));
        assertEquals(0, store.getUploadCount());
    }

    @Test
    void maybeExternalize_aboveThreshold_uploadsAndReturnsToken() {
        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder()
            .setThresholdBytes(10)
            .setMaxExternalizedPayloadBytes(10_000)
            .build();
        PayloadHelper helper = new PayloadHelper(store, options);

        String largePayload = "a]".repeat(100); // 200 chars
        String result = helper.maybeExternalize(largePayload);

        assertNotEquals(largePayload, result);
        assertTrue(store.isKnownPayloadToken(result));
        assertEquals(1, store.getUploadCount());
    }

    @Test
    void maybeExternalize_exceedsMaxCap_throwsException() {
        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder()
            .setThresholdBytes(10)
            .setMaxExternalizedPayloadBytes(50)
            .build();
        PayloadHelper helper = new PayloadHelper(store, options);

        // Create a payload larger than 50 bytes
        String hugePayload = "x".repeat(100);
        assertThrows(IllegalArgumentException.class, () -> helper.maybeExternalize(hugePayload));
        assertEquals(0, store.getUploadCount());
    }

    @Test
    void maybeResolve_nullValue_returnsNull() {
        InMemoryPayloadStore store = new InMemoryPayloadStore();
        PayloadHelper helper = new PayloadHelper(store, defaultOptions());
        assertNull(helper.maybeResolve(null));
        assertEquals(0, store.getDownloadCount());
    }

    @Test
    void maybeResolve_regularValue_returnsOriginal() {
        InMemoryPayloadStore store = new InMemoryPayloadStore();
        PayloadHelper helper = new PayloadHelper(store, defaultOptions());
        String regularValue = "just some data";
        assertEquals(regularValue, helper.maybeResolve(regularValue));
        assertEquals(0, store.getDownloadCount());
    }

    @Test
    void maybeResolve_knownToken_downloadsAndReturnsPayload() {
        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder()
            .setThresholdBytes(10)
            .setMaxExternalizedPayloadBytes(10_000)
            .build();
        PayloadHelper helper = new PayloadHelper(store, options);

        // First upload, then resolve
        String originalPayload = "x".repeat(100);
        String token = helper.maybeExternalize(originalPayload);
        String resolved = helper.maybeResolve(token);

        assertEquals(originalPayload, resolved);
        assertEquals(1, store.getDownloadCount());
    }

    @Test
    void roundTrip_externalizeAndResolve() {
        InMemoryPayloadStore store = new InMemoryPayloadStore();
        LargePayloadOptions options = new LargePayloadOptions.Builder()
            .setThresholdBytes(5)
            .setMaxExternalizedPayloadBytes(10_000)
            .build();
        PayloadHelper helper = new PayloadHelper(store, options);

        String payload = "This is a test payload that is long enough";
        String token = helper.maybeExternalize(payload);
        assertNotEquals(payload, token);

        String resolved = helper.maybeResolve(token);
        assertEquals(payload, resolved);
    }

    @Test
    void constructor_nullStore_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> new PayloadHelper(null, defaultOptions()));
    }

    @Test
    void constructor_nullOptions_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> new PayloadHelper(new InMemoryPayloadStore(), null));
    }

    private static LargePayloadOptions defaultOptions() {
        return new LargePayloadOptions.Builder().build();
    }
}
