// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for blob payload token encode/decode logic in {@link BlobPayloadStore}.
 */
class PayloadTokenTest {

    @Test
    void encodeToken_v2_preservesLocationWithoutAuthentication() {
        String url = "https://account.blob.core.windows.net/payloads/0123456789abcdef0123456789abcdef";
        assertEquals("blob:v2:" + url, BlobPayloadStore.encodeToken(url + "?sv=example&sig=example"));
        assertEquals("blob:v2:" + url, BlobPayloadStore.encodeToken(url));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "https://account.blob.core.windows.net/payloads/0123456789abcdef0123456789abcdef",
        "https://account.blob.core.windows.net/$root/0123456789abcdef0123456789abcdef",
        "http://127.0.0.1:10000/devstoreaccount1/payloads/0123456789abcdef0123456789abcdef"
    })
    void v2Tokens_preserveAbsoluteLocationAndDecodeNames(String url) {
        String token = BlobPayloadStore.encodeToken(url);
        assertTrue(createStoreWithDefaults().isKnownPayloadToken(token));
        String[] decoded = BlobPayloadStore.decodeToken(token);
        assertEquals("0123456789abcdef0123456789abcdef", decoded[1]);
        assertTrue(decoded[0].equals("payloads") || decoded[0].equals("$root"));
        assertEquals("blob:v2:" + url, token);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "blob:v2:payloads:0123456789abcdef0123456789abcdef",
        "blob:v2:/payloads/0123456789abcdef0123456789abcdef",
        "blob:v2:file:///payloads/0123456789abcdef0123456789abcdef",
        "blob:v2:ftp://account/payloads/0123456789abcdef0123456789abcdef",
        "blob:v2:https://account/payloads/",
        "blob:v2:https://account/payloads/not-a-guid",
        "blob:v2:https://account/payloads/0123456789abcdef0123456789abcdef\n",
        "blob:v2:https://account:65536/payloads/0123456789abcdef0123456789abcdef"
    })
    void malformedV2Tokens_areNotRecognized(String token) {
        assertFalse(createStoreWithDefaults().isKnownPayloadToken(token));
        assertThrows(IllegalArgumentException.class, () -> BlobPayloadStore.decodeToken(token));
    }

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
        assertTrue(store.isKnownPayloadToken(
            "blob:v1:container:0123456789abcdef0123456789abcdef"));
    }

    @Test
    void isKnownPayloadToken_rootContainer_returnsTrue() {
        BlobPayloadStore store = createStoreWithDefaults();
        assertTrue(store.isKnownPayloadToken(
            "blob:v1:$root:0123456789abcdef0123456789abcdef"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "blob:v1:$ROOT:0123456789abcdef0123456789abcdef",
        "blob:v1:$root-extra:0123456789abcdef0123456789abcdef",
        "blob:v1:$logs:0123456789abcdef0123456789abcdef",
        "blob:v1:$root:0123456789abcdef0123456789abcde",
        "blob:v1:$root:0123456789abcdef0123456789abcdeg",
        "blob:v1:$root:0123456789abcdef0123456789abcdef:extra",
        "blob:v1:$root:0123456789abcdef0123456789abcdef\n",
        "\"blob:v1:$root:0123456789abcdef0123456789abcdef\""
    })
    void isKnownPayloadToken_rootLookingLiteral_returnsFalse(String literal) {
        assertFalse(createStoreWithDefaults().isKnownPayloadToken(literal));
    }

    @Test
    void isKnownPayloadToken_prefixOnly_returnsFalse() {
        BlobPayloadStore store = createStoreWithDefaults();
        assertFalse(store.isKnownPayloadToken("blob:v1:arbitrary-user-content"));
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
