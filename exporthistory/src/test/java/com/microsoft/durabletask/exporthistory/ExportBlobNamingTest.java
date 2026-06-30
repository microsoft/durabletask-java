// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ExportBlobNaming}.
 */
class ExportBlobNamingTest {

    private static final Instant TS = Instant.parse("2026-06-30T12:00:00Z");
    private static final ExportFormat JSONL = new ExportFormat(ExportFormatKind.JSONL, "1.0");
    private static final ExportFormat JSON = new ExportFormat(ExportFormatKind.JSON, "1.0");

    @Test
    void blobFileName_isDeterministic() {
        String a = ExportBlobNaming.blobFileName(TS, "inst-1", JSONL);
        String b = ExportBlobNaming.blobFileName(TS, "inst-1", JSONL);
        assertEquals(a, b);
    }

    @Test
    void blobFileName_differsByInstance() {
        assertTrue(!ExportBlobNaming.blobFileName(TS, "inst-1", JSONL)
                .equals(ExportBlobNaming.blobFileName(TS, "inst-2", JSONL)));
    }

    @Test
    void blobFileName_hasFormatExtension() {
        assertTrue(ExportBlobNaming.blobFileName(TS, "inst-1", JSONL).endsWith(".jsonl.gz"));
        assertTrue(ExportBlobNaming.blobFileName(TS, "inst-1", JSON).endsWith(".json"));
    }

    @Test
    void blobFileName_hashIs64HexChars() {
        String name = ExportBlobNaming.blobFileName(TS, "inst-1", JSONL);
        String hash = name.substring(0, name.indexOf('.'));
        assertEquals(64, hash.length());
        assertTrue(hash.matches("[0-9a-f]{64}"));
    }

    @Test
    void blobPath_handlesPrefix() {
        assertEquals("file", ExportBlobNaming.blobPath(null, "file"));
        assertEquals("file", ExportBlobNaming.blobPath("", "file"));
        assertEquals("exports/file", ExportBlobNaming.blobPath("exports", "file"));
        assertEquals("exports/file", ExportBlobNaming.blobPath("exports/", "file"));
        assertEquals("exports/file", ExportBlobNaming.blobPath("exports///", "file"));
    }
}
