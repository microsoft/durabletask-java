// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.durabletask.history.EntityLockGrantedEvent;
import com.microsoft.durabletask.history.EntityLockRequestedEvent;
import com.microsoft.durabletask.history.EntityOperationCalledEvent;
import com.microsoft.durabletask.history.EntityOperationCompletedEvent;
import com.microsoft.durabletask.history.EntityOperationFailedEvent;
import com.microsoft.durabletask.history.EntityOperationSignaledEvent;
import com.microsoft.durabletask.history.EntityUnlockSentEvent;
import com.microsoft.durabletask.history.GenericEvent;
import com.microsoft.durabletask.history.HistoryEvent;
import com.microsoft.durabletask.history.TaskCompletedEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

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

    @Test
    void timestampSerialization_isLocaleInvariant() throws JsonProcessingException {
        Locale original = Locale.getDefault(Locale.Category.FORMAT);
        try {
            Locale.setDefault(Locale.Category.FORMAT, Locale.forLanguageTag("ar-EG"));
            String result = HistoryEventSerializer.serialize(
                    Arrays.asList((HistoryEvent) new GenericEvent(
                            1, Instant.parse("2026-06-30T12:00:00.123Z"), "payload")),
                    new ExportFormat(ExportFormatKind.JSONL, "1.0"));
            assertTrue(result.contains("\"timestamp\":\"2026-06-30T12:00:00.123Z\""));
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, original);
        }
    }

    @Test
    void entityEvents_serializeReflectivelyWithEventTypeDiscriminator() throws JsonProcessingException {
        // The reflective writeEntity path (non-parity by design) covers all 7 entity event types; pin the
        // eventType discriminator plus a representative field for each so a regression in the reflective
        // projection (or the jsr310 Instant module) is caught.
        ExportFormat format = new ExportFormat(ExportFormatKind.JSONL, "1.0");

        assertEntityEvent(format,
                new EntityOperationCalledEvent(1, TS, "req-1", "Add", null, "\"5\"",
                        "@parent@p", "pe1", "@counter@c1"),
                "EntityOperationCalled", "\"requestId\":\"req-1\"", "\"operation\":\"Add\"");
        assertEntityEvent(format,
                new EntityOperationSignaledEvent(2, TS, "req-2", "Increment", null, "\"1\"", "@counter@c2"),
                "EntityOperationSignaled", "\"requestId\":\"req-2\"", "\"operation\":\"Increment\"");
        assertEntityEvent(format,
                new EntityOperationCompletedEvent(3, TS, "req-3", "\"result\""),
                "EntityOperationCompleted", "\"requestId\":\"req-3\"");
        assertEntityEvent(format,
                new EntityOperationFailedEvent(4, TS, "req-4", null),
                "EntityOperationFailed", "\"requestId\":\"req-4\"");
        assertEntityEvent(format,
                new EntityLockRequestedEvent(5, TS, "cs-1", Arrays.asList("@e@a", "@e@b"), 0, "@parent@p"),
                "EntityLockRequested", "\"criticalSectionId\":\"cs-1\"");
        assertEntityEvent(format,
                new EntityLockGrantedEvent(6, TS, "cs-2"),
                "EntityLockGranted", "\"criticalSectionId\":\"cs-2\"");
        assertEntityEvent(format,
                new EntityUnlockSentEvent(7, TS, "cs-3", "@parent@p", "@e@t"),
                "EntityUnlockSent", "\"criticalSectionId\":\"cs-3\"");
    }

    private static void assertEntityEvent(
            ExportFormat format, HistoryEvent event, String eventType, String... expectedFragments)
            throws JsonProcessingException {
        String line = HistoryEventSerializer.serialize(Collections.singletonList(event), format).trim();
        assertTrue(line.startsWith("{\"eventType\":\"" + eventType + "\""),
                eventType + " discriminator missing in: " + line);
        for (String fragment : expectedFragments) {
            assertTrue(line.contains(fragment), eventType + " missing " + fragment + " in: " + line);
        }
    }
}
