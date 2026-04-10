// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for blob payload token encode/decode logic in {@link BlobPayloadStore}.
 */
class PayloadTokenTest {

    @Test
    void encodeToken_producesCorrectFormat() {
        String token = BlobPayloadStore.encodeToken("my-container", "abc123");
        assertEquals("blob:v1:my-container:abc123", token);
    }

    @Test
    void decodeToken_roundTrip() {
        String token = BlobPayloadStore.encodeToken("durabletask-payloads", "deadbeef");
        String[] parts = BlobPayloadStore.decodeToken(token);
        assertEquals("durabletask-payloads", parts[0]);
        assertEquals("deadbeef", parts[1]);
    }

    @Test
    void decodeToken_invalidPrefix_throws() {
        assertThrows(IllegalArgumentException.class, () ->
            BlobPayloadStore.decodeToken("invalid:token"));
    }

    @Test
    void decodeToken_missingName_throws() {
        assertThrows(IllegalArgumentException.class, () ->
            BlobPayloadStore.decodeToken("blob:v1:container:"));
    }

    @Test
    void decodeToken_missingContainer_throws() {
        assertThrows(IllegalArgumentException.class, () ->
            BlobPayloadStore.decodeToken("blob:v1::name"));
    }

    @Test
    void decodeToken_noSeparator_throws() {
        assertThrows(IllegalArgumentException.class, () ->
            BlobPayloadStore.decodeToken("blob:v1:containeronly"));
    }

    @Test
    void isKnownPayloadToken_validToken_returnsTrue() {
        BlobPayloadStore store = createStoreWithDefaults();
        assertTrue(store.isKnownPayloadToken("blob:v1:container:name"));
    }

    @Test
    void isKnownPayloadToken_emptyString_returnsFalse() {
        BlobPayloadStore store = createStoreWithDefaults();
        assertFalse(store.isKnownPayloadToken(""));
    }

    @Test
    void isKnownPayloadToken_null_returnsFalse() {
        BlobPayloadStore store = createStoreWithDefaults();
        assertFalse(store.isKnownPayloadToken(null));
    }

    @Test
    void isKnownPayloadToken_randomString_returnsFalse() {
        BlobPayloadStore store = createStoreWithDefaults();
        assertFalse(store.isKnownPayloadToken("just some data"));
    }

    @Test
    void isKnownPayloadToken_partialPrefix_returnsFalse() {
        BlobPayloadStore store = createStoreWithDefaults();
        assertFalse(store.isKnownPayloadToken("blob:v2:container:name"));
    }

    private BlobPayloadStore createStoreWithDefaults() {
        // Use the package-private constructor with null container client
        // since isKnownPayloadToken doesn't need it
        return new BlobPayloadStore(null, new LargePayloadStorageOptions());
    }
}
