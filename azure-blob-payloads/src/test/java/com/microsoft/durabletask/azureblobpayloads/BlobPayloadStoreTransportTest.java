// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.storage.blob.models.BlobStorageException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

class BlobPayloadStoreTransportTest {
    @ParameterizedTest
    @CsvSource({"true,409", "false,409", "true,412", "false,412"})
    void committedPutWithLostReply_isRetriedVerifiedAndReadable(boolean compressed, int status) {
        OfflineBlobService service = lostReplyService(status);
        BlobPayloadStore store = service.createStore("payloads", compressed);
        String payload = "original payload \u00e9\n".repeat(200);

        String token = store.upload(payload);

        assertEquals(2, service.putUrls.size(), "The Azure SDK must retry the ambiguous PUT");
        assertEquals(service.putUrls.get(0), service.putUrls.get(1), "Retries must use the same blob URI");
        assertArrayEquals(service.putBodies.get(0), service.putBodies.get(1), "The SDK must replay the same body");
        assertEquals(1, service.downloads, "The duplicate PUT must trigger content verification");
        assertTrue(store.isKnownPayloadToken(token));
        assertEquals(payload, store.download(token), "The returned token must actually be usable");
    }

    @ParameterizedTest
    @CsvSource({"true,409", "false,409", "true,412", "false,412"})
    void committedPutWithLostReply_rejectsDifferentBytes(boolean compressed, int status) {
        OfflineBlobService service = lostReplyService(status);
        service.changeStoredBytes = true;
        assertCollisionRejected(service, compressed);
    }

    @ParameterizedTest
    @CsvSource({"true,409", "false,409", "true,412", "false,412"})
    void committedPutWithLostReply_rejectsDifferentEncoding(boolean compressed, int status) {
        OfflineBlobService service = lostReplyService(status);
        service.changeStoredEncoding = true;
        assertCollisionRejected(service, compressed);
    }

    @ParameterizedTest
    @CsvSource({"true,409", "false,409", "true,412", "false,412"})
    void verificationDownloadFailure_preservesTransientStatus(boolean compressed, int status) {
        OfflineBlobService service = lostReplyService(status);
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
        assertEquals(service.duplicatePutStatus, ((BlobStorageException) failure.getCause()).getStatusCode());
        assertTrue(LargePayloadInterceptor.isPermanentStorageFailure(failure));
        assertEquals(2, service.putUrls.size());
        assertEquals(1, service.downloads);
    }

    @ParameterizedTest
    @CsvSource({
        "true,LeaseIdMissing", "false,LeaseIdMissing",
        "true,ConditionNotMet", "false,ConditionNotMet",
        "true,", "false,"
    })
    void unrelatedConflict_doesNotVerifyOrAcceptMatchingContent(boolean compressed, String errorCode) {
        OfflineBlobService service = lostReplyService(409);
        service.duplicatePutErrorCode = errorCode;
        BlobPayloadStore store = service.createStore("payloads", compressed);

        PayloadStorageException failure = assertThrows(PayloadStorageException.class,
            () -> store.upload("committed payload"));

        assertEquals(409, ((BlobStorageException) failure.getCause()).getStatusCode());
        assertTrue(LargePayloadInterceptor.isPermanentStorageFailure(failure));
        assertEquals(2, service.putUrls.size());
        assertEquals(0, service.downloads, "Only BlobAlreadyExists conflicts may enter verification");
    }

    private static OfflineBlobService lostReplyService(int status) {
        OfflineBlobService service = new OfflineBlobService();
        service.loseFirstPutResponse = true;
        service.duplicatePutStatus = status;
        service.duplicatePutErrorCode = status == 409 ? "BlobAlreadyExists" : "ConditionNotMet";
        return service;
    }
}
