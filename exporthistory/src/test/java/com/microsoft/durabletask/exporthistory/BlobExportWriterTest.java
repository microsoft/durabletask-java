// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for {@link BlobExportWriter}. */
class BlobExportWriterTest {

    @Test
    void upload_appliesHeadersMetadataAndOverwriteAtomically() throws IOException {
        BlobServiceClient serviceClient = mock(BlobServiceClient.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);
        BlobClient blobClient = mock(BlobClient.class);
        when(serviceClient.getBlobContainerClient("container")).thenReturn(containerClient);
        when(containerClient.getBlobClient("history.jsonl.gz")).thenReturn(blobClient);

        BlobExportWriter writer = new BlobExportWriter(serviceClient);
        ExportFormat format = new ExportFormat(ExportFormatKind.JSONL, "1.0");

        writer.upload("container", "history.jsonl.gz", "first", format, "instance-1");
        writer.upload("container", "history.jsonl.gz", "second", format, "instance-2");

        ArgumentCaptor<BlobParallelUploadOptions> options =
                ArgumentCaptor.forClass(BlobParallelUploadOptions.class);
        verify(blobClient, times(2)).uploadWithResponse(options.capture(), isNull(), eq(Context.NONE));
        verify(containerClient, times(2)).createIfNotExists();
        verify(blobClient, never()).setHttpHeaders(any(BlobHttpHeaders.class));
        verify(blobClient, never()).setMetadata(anyMap());

        List<BlobParallelUploadOptions> uploads = options.getAllValues();
        assertUpload(uploads.get(0), "first", "instance-1");
        assertUpload(uploads.get(1), "second", "instance-2");
    }

    @Test
    void upload_recreatesAContainerDeletedBetweenUploads() {
        BlobServiceClient service = mock(BlobServiceClient.class);
        BlobContainerClient container = mock(BlobContainerClient.class);
        BlobClient blob = mock(BlobClient.class);
        when(service.getBlobContainerClient("container")).thenReturn(container);
        when(container.getBlobClient("history.json")).thenReturn(blob);
        AtomicBoolean exists = new AtomicBoolean(false);
        BlobStorageException missing = storageFailure(404);
        when(container.createIfNotExists()).thenAnswer(invocation -> {
            exists.set(true);
            return true;
        });
        when(blob.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE)))
                .thenAnswer(invocation -> {
                    if (!exists.get()) {
                        throw missing;
                    }
                    return null;
                });
        BlobExportWriter writer = new BlobExportWriter(service);
        ExportFormat format = new ExportFormat(ExportFormatKind.JSON, "1.0");

        writer.upload("container", "history.json", "[]", format, "instance-1");
        exists.set(false);
        assertDoesNotThrow(() -> writer.upload("container", "history.json", "[]", format, "instance-1"));
        verify(container, times(2)).createIfNotExists();
        verify(blob, times(2)).uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE));
    }

    @Test
    void upload_containerCreationFailurePropagatesAndTheNextAttemptRechecks() {
        BlobServiceClient service = mock(BlobServiceClient.class);
        BlobContainerClient container = mock(BlobContainerClient.class);
        BlobClient blob = mock(BlobClient.class);
        when(service.getBlobContainerClient("container")).thenReturn(container);
        when(container.getBlobClient("history.json")).thenReturn(blob);
        BlobStorageException unavailable = storageFailure(503);
        when(container.createIfNotExists()).thenThrow(unavailable).thenReturn(true);
        BlobExportWriter writer = new BlobExportWriter(service);
        ExportFormat format = new ExportFormat(ExportFormatKind.JSON, "1.0");

        assertSame(unavailable, assertThrows(BlobStorageException.class,
                () -> writer.upload("container", "history.json", "[]", format, "instance-1")));
        verify(blob, never()).uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE));
        assertDoesNotThrow(() -> writer.upload("container", "history.json", "[]", format, "instance-1"));
        verify(container, times(2)).createIfNotExists();
        verify(blob).uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE));
    }

    @Test
    void upload_blobUploadFailurePropagatesAndTheRetryEnsuresTheContainerAgain() {
        BlobServiceClient service = mock(BlobServiceClient.class);
        BlobContainerClient container = mock(BlobContainerClient.class);
        BlobClient blob = mock(BlobClient.class);
        when(service.getBlobContainerClient("container")).thenReturn(container);
        when(container.getBlobClient("history.json")).thenReturn(blob);
        BlobStorageException unavailable = storageFailure(503);
        when(blob.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE)))
                .thenThrow(unavailable).thenReturn(null);
        BlobExportWriter writer = new BlobExportWriter(service);
        ExportFormat format = new ExportFormat(ExportFormatKind.JSON, "1.0");

        assertSame(unavailable, assertThrows(BlobStorageException.class,
                () -> writer.upload("container", "history.json", "[]", format, "instance-1")));
        assertDoesNotThrow(() -> writer.upload("container", "history.json", "[]", format, "instance-1"));
        verify(container, times(2)).createIfNotExists();
        verify(blob, times(2)).uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE));
    }

    private static BlobStorageException storageFailure(int status) {
        HttpResponse response = mock(HttpResponse.class);
        when(response.getStatusCode()).thenReturn(status);
        return new BlobStorageException("Storage failure", response, null);
    }

    private static void assertUpload(
            BlobParallelUploadOptions options, String expectedContent, String expectedInstanceId)
            throws IOException {
        assertNull(options.getRequestConditions(), "Uploads must remain unconditional so retries overwrite.");
        assertEquals("application/jsonl+gzip", options.getHeaders().getContentType());
        assertEquals("gzip", options.getHeaders().getContentEncoding());
        assertEquals(expectedInstanceId, options.getMetadata().get("instanceId"));
        byte[] payload = BinaryData.fromFlux(options.getDataFlux()).block().toBytes();
        try (GZIPInputStream stream = new GZIPInputStream(new ByteArrayInputStream(payload))) {
            assertEquals(expectedContent, new String(stream.readAllBytes(), StandardCharsets.UTF_8));
        }
    }
}