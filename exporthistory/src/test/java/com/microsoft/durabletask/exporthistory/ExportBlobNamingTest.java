// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
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

    @Test
    void formatTimestamp_hasSevenFractionalDigitsAndUtcOffset() {
        assertEquals("2026-06-30T12:00:00.0000000+00:00", ExportBlobNaming.formatTimestamp(TS));
        assertEquals("2026-06-30T12:00:00.1230000+00:00",
                ExportBlobNaming.formatTimestamp(Instant.parse("2026-06-30T12:00:00.123Z")));
    }

    @Test
    void blobFileName_isSha256OfTimestampAndInstanceId() throws Exception {
        // Blob name = lowercase-hex SHA-256 of "<timestamp>|<instanceId>" + extension.
        String expected = sha256Hex("2026-06-30T12:00:00.0000000+00:00|inst-1") + ".jsonl.gz";
        assertEquals(expected, ExportBlobNaming.blobFileName(TS, "inst-1", JSONL));
    }

    private static String sha256Hex(String value) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
