// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.storage.blob.models.BlobStorageException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class BlobPayloadStoreTransportTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void committedPutWithLostReply_isRetriedVerifiedAndReadable(boolean compressed) {
        OfflineBlobService service = new OfflineBlobService();
        service.loseFirstPutResponse = true;
        BlobPayloadStore store = service.createStore("payloads", compressed);
        String payload = "original payload \u00e9\n".repeat(200);

        String token = store.upload(payload);

        assertEquals(2, service.putUrls.size(), "The Azure SDK must retry the ambiguous PUT");
        assertEquals(service.putUrls.get(0), service.putUrls.get(1), "Retries must use the same blob URI");
        assertArrayEquals(service.putBodies.get(0), service.putBodies.get(1), "The SDK must replay the same body");
        assertEquals(1, service.downloads, "The 412 must trigger content verification");
        assertTrue(store.isKnownPayloadToken(token));
        assertEquals(payload, store.download(token), "The returned token must actually be usable");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void committedPutWithLostReply_rejectsDifferentBytes(boolean compressed) {
        OfflineBlobService service = new OfflineBlobService();
        service.loseFirstPutResponse = true;
        service.changeStoredBytes = true;
        assertCollisionRejected(service, compressed);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void committedPutWithLostReply_rejectsDifferentEncoding(boolean compressed) {
        OfflineBlobService service = new OfflineBlobService();
        service.loseFirstPutResponse = true;
        service.changeStoredEncoding = true;
        assertCollisionRejected(service, compressed);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void verificationDownloadFailure_preservesTransientStatus(boolean compressed) {
        OfflineBlobService service = new OfflineBlobService();
        service.loseFirstPutResponse = true;
        service.downloadStatus = 503;
        BlobPayloadStore store = service.createStore("payloads", compressed);

        PayloadStorageException failure = assertThrows(PayloadStorageException.class,
                () -> store.upload("committed payload"));

        assertInstanceOf(BlobStorageException.class, failure.getCause());
        assertEquals(503, ((BlobStorageException) failure.getCause()).getStatusCode());
        assertFalse(LargePayloadInterceptor.isPermanentStorageFailure(failure));
        assertEquals(2, service.putUrls.size());
        assertEquals(2, service.downloads, "Verification must exhaust the configured GET retries");
    }

    private static void assertCollisionRejected(OfflineBlobService service, boolean compressed) {
        BlobPayloadStore store = service.createStore("payloads", compressed);
        PayloadStorageException failure = assertThrows(PayloadStorageException.class,
                () -> store.upload("attempted payload"));
        assertEquals(412, ((BlobStorageException) failure.getCause()).getStatusCode());
        assertTrue(LargePayloadInterceptor.isPermanentStorageFailure(failure));
        assertEquals(2, service.putUrls.size());
        assertEquals(1, service.downloads);
    }
}
