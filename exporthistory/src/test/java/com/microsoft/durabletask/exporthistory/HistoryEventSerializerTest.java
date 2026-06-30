// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.durabletask.history.GenericEvent;
import com.microsoft.durabletask.history.HistoryEvent;
import com.microsoft.durabletask.history.TaskCompletedEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link HistoryEventSerializer}.
 */
class HistoryEventSerializerTest {

    private static final Instant TS = Instant.parse("2026-06-30T12:00:00Z");

    private static List<HistoryEvent> sampleEvents() {
        return Arrays.asList(
                new TaskCompletedEvent(1, TS, 7, "\"42\""),
                new GenericEvent(2, TS, "payload"));
    }

    @Test
    void jsonl_oneEventPerLine() throws JsonProcessingException {
        ExportFormat format = new ExportFormat(ExportFormatKind.JSONL, "1.0");
        String result = HistoryEventSerializer.serialize(sampleEvents(), format);

        String[] lines = result.split("\n");
        assertEquals(2, lines.length);
        assertTrue(lines[0].contains("\"eventId\":1"));
        assertTrue(lines[0].contains("\"taskScheduledId\":7"));
        assertTrue(lines[1].contains("\"eventId\":2"));
        assertTrue(lines[1].contains("payload"));
    }

    @Test
    void jsonl_omitsNullFields() throws JsonProcessingException {
        // GenericEvent with null data should not emit a "data" property.
        ExportFormat format = new ExportFormat(ExportFormatKind.JSONL, "1.0");
        String result = HistoryEventSerializer.serialize(
                Arrays.asList((HistoryEvent) new GenericEvent(1, TS, null)), format);
        assertFalse(result.contains("\"data\""));
        assertTrue(result.contains("\"eventId\":1"));
    }

    @Test
    void json_producesArray() throws JsonProcessingException {
        ExportFormat format = new ExportFormat(ExportFormatKind.JSON, "1.0");
        String result = HistoryEventSerializer.serialize(sampleEvents(), format).trim();
        assertTrue(result.startsWith("["));
        assertTrue(result.endsWith("]"));
    }

    @Test
    void fileExtension_byFormat() {
        assertEquals("jsonl.gz", HistoryEventSerializer.fileExtension(new ExportFormat(ExportFormatKind.JSONL, "1.0")));
        assertEquals("json", HistoryEventSerializer.fileExtension(new ExportFormat(ExportFormatKind.JSON, "1.0")));
    }

    @Test
    void isCompressed_byFormat() {
        assertTrue(HistoryEventSerializer.isCompressed(new ExportFormat(ExportFormatKind.JSONL, "1.0")));
        assertFalse(HistoryEventSerializer.isCompressed(new ExportFormat(ExportFormatKind.JSON, "1.0")));
    }

    @Test
    void contentType_byFormat() {
        assertEquals("application/jsonl+gzip",
                HistoryEventSerializer.contentType(new ExportFormat(ExportFormatKind.JSONL, "1.0")));
        assertEquals("application/json",
                HistoryEventSerializer.contentType(new ExportFormat(ExportFormatKind.JSON, "1.0")));
    }
}
