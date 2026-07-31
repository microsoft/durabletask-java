// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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