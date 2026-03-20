// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azureblobpayloads;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for BlobPayloadStoreOptions.
 */
public class BlobPayloadStoreOptionsTest {

    @Test
    void connectionString_setsCorrectly() {
        BlobPayloadStoreOptions options = new BlobPayloadStoreOptions.Builder()
            .setConnectionString("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")
            .build();
        assertNotNull(options.getConnectionString());
        assertEquals("durabletask-payloads", options.getContainerName());
        assertEquals("payloads/", options.getBlobPrefix());
        assertTrue(options.isCompressPayloads());
    }

    @Test
    void customContainerAndPrefix() {
        BlobPayloadStoreOptions options = new BlobPayloadStoreOptions.Builder()
            .setConnectionString("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")
            .setContainerName("my-container")
            .setBlobPrefix("my-prefix/")
            .build();
        assertEquals("my-container", options.getContainerName());
        assertEquals("my-prefix/", options.getBlobPrefix());
    }

    @Test
    void noAuthMethod_throws() {
        assertThrows(IllegalStateException.class,
            () -> new BlobPayloadStoreOptions.Builder().build());
    }

    @Test
    void multipleAuthMethods_throws() {
        assertThrows(IllegalStateException.class,
            () -> new BlobPayloadStoreOptions.Builder()
                .setConnectionString("conn-string")
                .setBlobServiceEndpoint("https://test.blob.core.windows.net")
                .build());
    }

    @Test
    void endpointWithoutCredential_throws() {
        assertThrows(IllegalStateException.class,
            () -> new BlobPayloadStoreOptions.Builder()
                .setBlobServiceEndpoint("https://test.blob.core.windows.net")
                .build());
    }

    @Test
    void nullConnectionString_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> new BlobPayloadStoreOptions.Builder().setConnectionString(null));
    }

    @Test
    void emptyContainerName_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> new BlobPayloadStoreOptions.Builder().setContainerName(""));
    }

    @Test
    void nullBlobPrefix_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> new BlobPayloadStoreOptions.Builder().setBlobPrefix(null));
    }

    @Test
    void compressPayloads_defaultsToTrue() {
        BlobPayloadStoreOptions options = new BlobPayloadStoreOptions.Builder()
            .setConnectionString("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")
            .build();
        assertTrue(options.isCompressPayloads());
    }

    @Test
    void compressPayloads_canBeDisabled() {
        BlobPayloadStoreOptions options = new BlobPayloadStoreOptions.Builder()
            .setConnectionString("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")
            .setCompressPayloads(false)
            .build();
        assertFalse(options.isCompressPayloads());
    }
}
