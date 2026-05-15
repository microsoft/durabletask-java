// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.options;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ExportHistoryStorageOptions} and its Builder.
 */
class ExportHistoryStorageOptionsTest {

    @Test
    void build_withRequiredFields_succeeds() {
        ExportHistoryStorageOptions options = ExportHistoryStorageOptions.newBuilder()
                .connectionString("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net")
                .containerName("export-data")
                .build();

        assertNotNull(options.getConnectionString());
        assertEquals("export-data", options.getContainerName());
        assertNull(options.getPrefix());
    }

    @Test
    void build_withPrefix_succeeds() {
        ExportHistoryStorageOptions options = ExportHistoryStorageOptions.newBuilder()
                .connectionString("connstr")
                .containerName("container")
                .prefix("my-prefix/")
                .build();

        assertEquals("my-prefix/", options.getPrefix());
    }

    @Test
    void build_missingConnectionString_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                ExportHistoryStorageOptions.newBuilder()
                        .containerName("container")
                        .build());
    }

    @Test
    void build_emptyConnectionString_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                ExportHistoryStorageOptions.newBuilder()
                        .connectionString("")
                        .containerName("container")
                        .build());
    }

    @Test
    void build_missingContainerName_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                ExportHistoryStorageOptions.newBuilder()
                        .connectionString("connstr")
                        .build());
    }

    @Test
    void build_emptyContainerName_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                ExportHistoryStorageOptions.newBuilder()
                        .connectionString("connstr")
                        .containerName("")
                        .build());
    }
}
