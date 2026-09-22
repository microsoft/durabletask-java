// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobDownloadHeaders;
import com.azure.storage.blob.models.BlobDownloadResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link BlobPayloadStore} — verifies upload/download/compression behavior
 * using a mocked {@link BlobContainerClient}.
 */
class BlobPayloadStoreTest {

    private BlobContainerClient mockContainerClient;
    private BlobClient mockBlobClient;
    private LargePayloadStorageOptions options;

    @BeforeEach
    void setUp() {
        mockContainerClient = mock(BlobContainerClient.class);
        mockBlobClient = mock(BlobClient.class);

        when(mockContainerClient.getBlobContainerName()).thenReturn("durabletask-payloads");
        when(mockContainerClient.getBlobClient(anyString())).thenReturn(mockBlobClient);

        options = new LargePayloadStorageOptions()
            .setCompressionEnabled(true);
    }

    // ==================== Upload tests ====================

    @Test
    void upload_withCompression_uploadsGzipAndReturnsToken() {
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        String payload = "Hello, this is a test payload for upload.";

        String token = store.upload(payload);

        assertTrue(token.startsWith("blob:v1:durabletask-payloads:"));
        verify(mockContainerClient).createIfNotExists();
        // Verify uploadWithResponse was called (compressed path) with If-None-Match: * precondition
        ArgumentCaptor<BlobRequestConditions> conditionsCaptor = ArgumentCaptor.forClass(BlobRequestConditions.class);
        verify(mockBlobClient).uploadWithResponse(
            any(InputStream.class), anyLong(), isNull(),
            any(BlobHttpHeaders.class), isNull(), isNull(), conditionsCaptor.capture(), isNull(), any());
        assertEquals("*", conditionsCaptor.getValue().getIfNoneMatch(),
            "Upload must set If-None-Match: * to prevent overwriting an existing blob.");
    }

    @Test
    void upload_withoutCompression_uploadsRawAndReturnsToken() {
        options.setCompressionEnabled(false);
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        String payload = "Uncompressed payload data";

        String token = store.upload(payload);

        assertTrue(token.startsWith("blob:v1:durabletask-payloads:"));
        verify(mockContainerClient).createIfNotExists();
        // Verify uploadWithResponse was called (uncompressed path) with If-None-Match: * precondition
        ArgumentCaptor<BlobRequestConditions> conditionsCaptor = ArgumentCaptor.forClass(BlobRequestConditions.class);
        verify(mockBlobClient).uploadWithResponse(
            any(InputStream.class), anyLong(), isNull(),
            isNull(), isNull(), isNull(), conditionsCaptor.capture(), isNull(), any());
        assertEquals("*", conditionsCaptor.getValue().getIfNoneMatch(),
            "Upload must set If-None-Match: * to prevent overwriting an existing blob.");
    }

    @Test
    void upload_blobAlreadyExists_throwsPayloadStorageException() {
        // Simulate a 409 BlobAlreadyExists from Azure — must surface as a hard failure
        // rather than silently overwriting.
        BlobStorageException conflict = mock(BlobStorageException.class);
        when(conflict.getStatusCode()).thenReturn(409);
        doThrow(conflict).when(mockBlobClient).uploadWithResponse(
            any(InputStream.class), anyLong(), isNull(),
            any(BlobHttpHeaders.class), isNull(), isNull(), any(BlobRequestConditions.class), isNull(), any());

        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        PayloadStorageException ex = assertThrows(PayloadStorageException.class,
            () -> store.upload("test payload"));
        assertTrue(ex.getMessage().contains("already exists"),
            "Exception message should indicate a blob-name collision: " + ex.getMessage());
    }

    @Test
    void upload_preconditionFailed_throwsPayloadStorageException() {
        // Simulate a 412 ConditionNotMet (from If-None-Match: *) — same treatment as 409.
        BlobStorageException preconditionFailed = mock(BlobStorageException.class);
        when(preconditionFailed.getStatusCode()).thenReturn(412);
        doThrow(preconditionFailed).when(mockBlobClient).uploadWithResponse(
            any(InputStream.class), anyLong(), isNull(),
            any(BlobHttpHeaders.class), isNull(), isNull(), any(BlobRequestConditions.class), isNull(), any());

        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        PayloadStorageException ex = assertThrows(PayloadStorageException.class,
            () -> store.upload("test payload"));
        assertTrue(ex.getMessage().contains("already exists"),
            "Exception message should indicate a blob-name collision: " + ex.getMessage());
    }

    @Test
    void upload_tokenContainsGuidBlobName() {
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);

        String token = store.upload("test");

        // Token format: blob:v1:<container>:<blobName>
        String[] parts = BlobPayloadStore.decodeToken(token);
        assertEquals("durabletask-payloads", parts[0]);
        // Blob name should be a UUID without dashes (32 hex chars)
        assertTrue(parts[1].matches("[0-9a-f]{32}"), "Blob name should be a GUID without dashes: " + parts[1]);
    }

    @Test
    void upload_containerAlreadyExists_succeeds() {
        BlobStorageException conflict = mock(BlobStorageException.class);
        when(conflict.getStatusCode()).thenReturn(409);
        when(conflict.getErrorCode()).thenReturn(BlobErrorCode.CONTAINER_ALREADY_EXISTS);
        doThrow(conflict).when(mockContainerClient).createIfNotExists();

        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        String token = store.upload("test");

        assertNotNull(token);
        assertTrue(token.startsWith("blob:v1:"));
    }

    @Test
    void upload_containerCreationFails_throwsPayloadStorageException() {
        BlobStorageException serverError = mock(BlobStorageException.class);
        when(serverError.getStatusCode()).thenReturn(500);
        doThrow(serverError).when(mockContainerClient).createIfNotExists();

        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);

        assertThrows(PayloadStorageException.class, () -> store.upload("test"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void upload_deletedContainer_recreatesAndRetriesSameBlob(boolean compressed) {
        options.setCompressionEnabled(compressed);
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        store.upload("first payload");

        BlobStorageException missing = storageFailure(404, BlobErrorCode.CONTAINER_NOT_FOUND);
        doThrow(missing).doNothing().when(mockBlobClient).uploadWithResponse(
            any(InputStream.class), anyLong(), isNull(), nullable(BlobHttpHeaders.class),
            isNull(), isNull(), any(BlobRequestConditions.class), isNull(), any());

        String token = store.upload("payload after deletion");

        assertTrue(token.startsWith("blob:v1:durabletask-payloads:"));
        verify(mockContainerClient, times(2)).createIfNotExists();
        verify(mockContainerClient, times(2)).getBlobClient(anyString());
        verify(mockBlobClient, times(3)).uploadWithResponse(
            any(InputStream.class), anyLong(), isNull(), nullable(BlobHttpHeaders.class),
            isNull(), isNull(), any(BlobRequestConditions.class), isNull(), any());
    }

    @Test
    void upload_containerStillMissing_retriesOnlyOnce() {
        BlobStorageException missing = storageFailure(404, BlobErrorCode.CONTAINER_NOT_FOUND);
        doThrow(missing).when(mockBlobClient).uploadWithResponse(
            any(InputStream.class), anyLong(), isNull(), any(BlobHttpHeaders.class),
            isNull(), isNull(), any(BlobRequestConditions.class), isNull(), any());
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);

        PayloadStorageException failure = assertThrows(PayloadStorageException.class, () -> store.upload("payload"));

        assertSame(missing, failure.getCause());
        verify(mockContainerClient, times(2)).createIfNotExists();
        verify(mockContainerClient).getBlobClient(anyString());
        verify(mockBlobClient, times(2)).uploadWithResponse(
            any(InputStream.class), anyLong(), isNull(), any(BlobHttpHeaders.class),
            isNull(), isNull(), any(BlobRequestConditions.class), isNull(), any());
    }

    @ParameterizedTest
    @CsvSource({"404,BlobNotFound", "404,", "403,AuthorizationFailure", "500,InternalError"})
    void upload_otherStorageFailures_doNotRecreateContainer(int status, String errorCode) {
        BlobStorageException storageFailure = storageFailure(
            status, errorCode == null ? null : BlobErrorCode.fromString(errorCode));
        doThrow(storageFailure).when(mockBlobClient).uploadWithResponse(
            any(InputStream.class), anyLong(), isNull(), any(BlobHttpHeaders.class),
            isNull(), isNull(), any(BlobRequestConditions.class), isNull(), any());
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);

        PayloadStorageException failure = assertThrows(PayloadStorageException.class, () -> store.upload("payload"));

        assertSame(storageFailure, failure.getCause());
        verify(mockContainerClient).createIfNotExists();
        verify(mockBlobClient).uploadWithResponse(
            any(InputStream.class), anyLong(), isNull(), any(BlobHttpHeaders.class),
            isNull(), isNull(), any(BlobRequestConditions.class), isNull(), any());
    }

    @Test
    void upload_containerBeingDeleted_doesNotCacheFailedInitialization() {
        BlobStorageException deleting = storageFailure(409, BlobErrorCode.CONTAINER_BEING_DELETED);
        doThrow(deleting).doReturn(true).when(mockContainerClient).createIfNotExists();
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);

        PayloadStorageException failure = assertThrows(PayloadStorageException.class, () -> store.upload("payload"));
        assertSame(deleting, failure.getCause());
        store.upload("later payload");

        verify(mockContainerClient, times(2)).createIfNotExists();
        verify(mockBlobClient).uploadWithResponse(
            any(InputStream.class), anyLong(), isNull(), any(BlobHttpHeaders.class),
            isNull(), isNull(), any(BlobRequestConditions.class), isNull(), any());
    }

    @Test
    void upload_staleContainerNotFound_doesNotInvalidateNewGeneration() throws Exception {
        options.setCompressionEnabled(false);
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        store.upload("initial payload");

        CountDownLatch slowUploadStarted = new CountDownLatch(1);
        CountDownLatch releaseSlowFailure = new CountDownLatch(1);
        AtomicInteger slowAttempts = new AtomicInteger();
        AtomicInteger fastAttempts = new AtomicInteger();
        BlobStorageException missing = storageFailure(404, BlobErrorCode.CONTAINER_NOT_FOUND);
        doAnswer(invocation -> {
            InputStream stream = invocation.getArgument(0);
            String payload = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            if (payload.equals("slow") && slowAttempts.incrementAndGet() == 1) {
                slowUploadStarted.countDown();
                assertTrue(releaseSlowFailure.await(5, TimeUnit.SECONDS));
                throw missing;
            }
            if (payload.equals("fast") && fastAttempts.incrementAndGet() == 1) {
                throw missing;
            }
            return null;
        }).when(mockBlobClient).uploadWithResponse(
            any(InputStream.class), anyLong(), isNull(), isNull(),
            isNull(), isNull(), any(BlobRequestConditions.class), isNull(), any());

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<String> slow = executor.submit(() -> store.upload("slow"));
            assertTrue(slowUploadStarted.await(5, TimeUnit.SECONDS));
            Future<String> fast = executor.submit(() -> store.upload("fast"));
            assertNotNull(fast.get(5, TimeUnit.SECONDS));
            releaseSlowFailure.countDown();
            assertNotNull(slow.get(5, TimeUnit.SECONDS));

            assertEquals(2, slowAttempts.get());
            assertEquals(2, fastAttempts.get());
            verify(mockContainerClient, times(2)).createIfNotExists();
        } finally {
            releaseSlowFailure.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void upload_interruptedInitialization_preservesInterrupt() {
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        Thread.currentThread().interrupt();
        try {
            assertThrows(PayloadStorageException.class, () -> store.upload("payload"));
            assertTrue(Thread.currentThread().isInterrupted());
            verify(mockContainerClient, never()).createIfNotExists();
        } finally {
            Thread.interrupted();
        }
    }

    // ==================== Download tests ====================

    @Test
    void download_uncompressedBlob_returnsOriginalPayload() {
        String expectedPayload = "Hello, uncompressed world!";
        byte[] payloadBytes = expectedPayload.getBytes(StandardCharsets.UTF_8);

        mockDownloadResponse(payloadBytes, null);

        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        String token = BlobPayloadStore.encodeToken("durabletask-payloads", "testblob");

        String result = store.download(token);

        assertEquals(expectedPayload, result);
    }

    @Test
    void download_gzipCompressedBlob_decompressesCorrectly() throws Exception {
        String expectedPayload = "Hello, compressed world!";

        // Create gzip-compressed bytes
        ByteArrayOutputStream compressedBuffer = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(compressedBuffer)) {
            gzip.write(expectedPayload.getBytes(StandardCharsets.UTF_8));
        }
        byte[] compressedBytes = compressedBuffer.toByteArray();

        mockDownloadResponse(compressedBytes, "gzip");

        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        String token = BlobPayloadStore.encodeToken("durabletask-payloads", "compressed-blob");

        String result = store.download(token);

        assertEquals(expectedPayload, result);
    }

    @Test
    void download_blobNotFound_throwsPayloadStorageException() {
        BlobStorageException notFound = mock(BlobStorageException.class);
        when(notFound.getStatusCode()).thenReturn(404);
        doThrow(notFound).when(mockBlobClient).downloadStreamWithResponse(
            any(OutputStream.class), isNull(), isNull(), isNull(), eq(false), isNull(), any());

        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        String token = BlobPayloadStore.encodeToken("durabletask-payloads", "missing-blob");

        PayloadStorageException ex = assertThrows(PayloadStorageException.class,
            () -> store.download(token));
        assertTrue(ex.getMessage().contains("not found"));
    }

    @Test
    void download_nonMatchingContainer_throwsIllegalArgument() {
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        String token = BlobPayloadStore.encodeToken("other-container", "some-blob");

        assertThrows(IllegalArgumentException.class, () -> store.download(token));
    }

    @Test
    void download_blobStorageError_throwsPayloadStorageException() {
        BlobStorageException serverError = mock(BlobStorageException.class);
        when(serverError.getStatusCode()).thenReturn(500);
        doThrow(serverError).when(mockBlobClient).downloadStreamWithResponse(
            any(OutputStream.class), isNull(), isNull(), isNull(), eq(false), isNull(), any());

        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        String token = BlobPayloadStore.encodeToken("durabletask-payloads", "error-blob");

        assertThrows(PayloadStorageException.class, () -> store.download(token));
    }

    // ==================== Upload + Download round-trip ====================

    @Test
    void upload_download_roundTrip_withCompression() throws Exception {
        String originalPayload = "Round-trip test payload with special chars: àéîõü 🎉";

        // Capture what was uploaded
        final byte[][] capturedBytes = new byte[1][];
        doAnswer(inv -> {
            InputStream stream = inv.getArgument(0);
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] tmp = new byte[8192];
            int len;
            while ((len = stream.read(tmp)) != -1) {
                buf.write(tmp, 0, len);
            }
            capturedBytes[0] = buf.toByteArray();
            return null;
        }).when(mockBlobClient).uploadWithResponse(
            any(InputStream.class), anyLong(), isNull(),
            any(BlobHttpHeaders.class), isNull(), isNull(), any(BlobRequestConditions.class), isNull(), any());

        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        String token = store.upload(originalPayload);

        // Now set up download to return the captured bytes
        mockDownloadResponse(capturedBytes[0], "gzip");

        // Need to map the token's blob name back to our mock
        String[] decoded = BlobPayloadStore.decodeToken(token);
        when(mockContainerClient.getBlobClient(decoded[1])).thenReturn(mockBlobClient);

        String downloaded = store.download(token);
        assertEquals(originalPayload, downloaded);
    }

    @Test
    void upload_download_roundTrip_withoutCompression() throws Exception {
        options.setCompressionEnabled(false);
        String originalPayload = "Round-trip test without compression";

        // Capture what was uploaded
        final byte[][] capturedBytes = new byte[1][];
        doAnswer(inv -> {
            InputStream stream = inv.getArgument(0);
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] tmp = new byte[8192];
            int len;
            while ((len = stream.read(tmp)) != -1) {
                buf.write(tmp, 0, len);
            }
            capturedBytes[0] = buf.toByteArray();
            return null;
        }).when(mockBlobClient).uploadWithResponse(
            any(InputStream.class), anyLong(), isNull(),
            isNull(), isNull(), isNull(), any(BlobRequestConditions.class), isNull(), any());

        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        String token = store.upload(originalPayload);

        // Set up download
        mockDownloadResponse(capturedBytes[0], null);

        String[] decoded = BlobPayloadStore.decodeToken(token);
        when(mockContainerClient.getBlobClient(decoded[1])).thenReturn(mockBlobClient);

        String downloaded = store.download(token);
        assertEquals(originalPayload, downloaded);
    }

    // ==================== isKnownPayloadToken tests ====================

    @Test
    void isKnownPayloadToken_validToken_returnsTrue() {
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        assertTrue(store.isKnownPayloadToken(
            "blob:v1:container:0123456789abcdef0123456789abcdef"));
    }

    @Test
    void isKnownPayloadToken_nonToken_returnsFalse() {
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        assertFalse(store.isKnownPayloadToken("just regular data"));
    }

    @Test
    void isKnownPayloadToken_null_returnsFalse() {
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        assertFalse(store.isKnownPayloadToken(null));
    }

    @Test
    void isKnownPayloadToken_empty_returnsFalse() {
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        assertFalse(store.isKnownPayloadToken(""));
    }

    @Test
    void isKnownPayloadToken_prefixOnly_returnsFalse() {
        // Prefix match alone must not be accepted as a token — guards against user
        // strings that happen to start with "blob:v1:" causing spurious blob GETs.
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        assertFalse(store.isKnownPayloadToken("blob:v1:user-controlled-garbage"));
    }

    @Test
    void isKnownPayloadToken_nonHexBlobName_returnsFalse() {
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        assertFalse(store.isKnownPayloadToken("blob:v1:container:not-a-uuid"));
    }

    @Test
    void isKnownPayloadToken_shortBlobName_returnsFalse() {
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        // 31 hex chars — one short of 32.
        assertFalse(store.isKnownPayloadToken(
            "blob:v1:container:0123456789abcdef0123456789abcde"));
    }

    @Test
    void isKnownPayloadToken_invalidContainerChars_returnsFalse() {
        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        // Uppercase is not a legal Azure container char.
        assertFalse(store.isKnownPayloadToken(
            "blob:v1:Container:0123456789abcdef0123456789abcdef"));
    }

    // ==================== Constructor validation ====================

    @Test
    void constructor_nullOptions_throws() {
        assertThrows(IllegalArgumentException.class, () -> new BlobPayloadStore(null));
    }

    @Test
    void constructor_noAuthConfig_throws() {
        LargePayloadStorageOptions opts = new LargePayloadStorageOptions();
        // Neither connection string nor account URI/credential set
        assertThrows(IllegalArgumentException.class, () -> new BlobPayloadStore(opts));
    }

    @Test
    void upload_compressionProducesGzipHeaders() {
        ArgumentCaptor<BlobHttpHeaders> headersCaptor = ArgumentCaptor.forClass(BlobHttpHeaders.class);

        BlobPayloadStore store = new BlobPayloadStore(mockContainerClient, options);
        store.upload("some payload data");

        verify(mockBlobClient).uploadWithResponse(
            any(InputStream.class), anyLong(), isNull(),
            headersCaptor.capture(), isNull(), isNull(), any(BlobRequestConditions.class), isNull(), any());

        BlobHttpHeaders capturedHeaders = headersCaptor.getValue();
        assertEquals("gzip", capturedHeaders.getContentEncoding());
    }

    // ==================== Test helpers ====================

    private static BlobStorageException storageFailure(int status, BlobErrorCode errorCode) {
        BlobStorageException failure = mock(BlobStorageException.class);
        when(failure.getStatusCode()).thenReturn(status);
        when(failure.getErrorCode()).thenReturn(errorCode);
        return failure;
    }

    /**
     * Sets up the mockBlobClient to return a downloadStreamWithResponse that writes
     * the given bytes and returns headers with the specified content-encoding.
     */
    private void mockDownloadResponse(byte[] content, String contentEncoding) {
        BlobDownloadHeaders downloadHeaders = mock(BlobDownloadHeaders.class);
        when(downloadHeaders.getContentEncoding()).thenReturn(contentEncoding);

        BlobDownloadResponse downloadResponse = mock(BlobDownloadResponse.class);
        when(downloadResponse.getDeserializedHeaders()).thenReturn(downloadHeaders);

        doAnswer(inv -> {
            OutputStream out = inv.getArgument(0);
            out.write(content);
            return downloadResponse;
        }).when(mockBlobClient).downloadStreamWithResponse(
            any(OutputStream.class), isNull(), isNull(), isNull(), eq(false), isNull(), any());
    }
}
