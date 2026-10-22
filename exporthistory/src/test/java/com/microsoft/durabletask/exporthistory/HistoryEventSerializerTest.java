// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.durabletask.FailureDetails;
import com.microsoft.durabletask.history.EntityLockRequestedEvent;
import com.microsoft.durabletask.history.EntityOperationCalledEvent;
import com.microsoft.durabletask.history.EntityOperationFailedEvent;
import com.microsoft.durabletask.history.EntityOperationSignaledEvent;
import com.microsoft.durabletask.history.EntityUnlockSentEvent;
import com.microsoft.durabletask.history.ExecutionStartedEvent;
import com.microsoft.durabletask.history.GenericEvent;
import com.microsoft.durabletask.history.HistoryEvent;
import com.microsoft.durabletask.history.OrchestrationInstance;
import com.microsoft.durabletask.history.TaskCompletedEvent;
import com.microsoft.durabletask.history.TaskFailedEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    void failurePropertiesFromExceptionArePreservedAndNullPropertiesRemainOmitted() {
        FailureDetails withProperties = FailureDetails.fromException(
                new IllegalStateException("boom"), exception -> Collections.singletonMap("code", 42));
        FailureDetails withoutProperties = FailureDetails.fromException(new IllegalStateException("boom"), null);
        ExportFormat format = new ExportFormat(ExportFormatKind.JSONL, "1.0");

        String with = HistoryEventSerializer.serialize(
                Collections.singletonList(new TaskFailedEvent(2, TS, 1, withProperties)), format);
        assertTrue(with.contains("\"properties\":{\"code\":42}"));
        String without = HistoryEventSerializer.serialize(
                Collections.singletonList(new TaskFailedEvent(2, TS, 1, withoutProperties)), format);
        assertFalse(without.contains("\"properties\""));
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
    void entityEvents_requireOrchestrationHistoryContext() {
        ExportFormat format = new ExportFormat(ExportFormatKind.JSONL, "1.0");
        List<HistoryEvent> events = Arrays.asList(
                new EntityOperationCalledEvent(1, TS, "req-1", "Add", null, "\"5\"",
                        "@parent@p", "pe1", "@counter@c1"),
                new EntityLockRequestedEvent(5, TS, "cs-1", Arrays.asList("@e@a", "@e@b"), 0, "@parent@p"),
                new EntityUnlockSentEvent(7, TS, "cs-3", "@parent@p", "@e@t"));
        for (HistoryEvent event : events) {
            assertThrows(IllegalArgumentException.class,
                    () -> HistoryEventSerializer.serialize(Collections.singletonList(event), format));
        }
    }

    @Test
    void entityEvents_rejectMissingOrInvalidFields() {
        ExportFormat format = new ExportFormat(ExportFormatKind.JSONL, "1.0");
        List<HistoryEvent> invalidEvents = Arrays.asList(
                new EntityOperationSignaledEvent(1, TS, "req-1", "Add", null, null, null),
                new EntityOperationFailedEvent(2, TS, "req-2", null),
                new EntityLockRequestedEvent(3, TS, "cs-1", Collections.singletonList("@counter@one"), 1, null),
                new EntityLockRequestedEvent(4, TS, "cs-2", Collections.singletonList("@@invalid"), 0, null));
        for (HistoryEvent event : invalidEvents) {
            assertThrows(IllegalArgumentException.class,
                    () -> HistoryEventSerializer.serialize(withExecutionStarted(event), format));
        }
    }

    @Test
    void entityHistoryContext_doesNotLeakAcrossExports() {
        ExportFormat format = new ExportFormat(ExportFormatKind.JSONL, "1.0");
        HistoryEvent call = new EntityOperationCalledEvent(
                1, TS, "req-1", "Add", null, null, null, null, "@counter@one");
        HistoryEventSerializer.serialize(withExecutionStarted(call), format);

        assertThrows(IllegalArgumentException.class,
                () -> HistoryEventSerializer.serialize(Collections.singletonList(call), format));
    }

    private static List<HistoryEvent> withExecutionStarted(HistoryEvent event) {
        return Arrays.asList(
                new ExecutionStartedEvent(0, TS, "Orchestrator", null, null,
                        new OrchestrationInstance("parent", "execution"),
                        null, null, null, null, Collections.emptyMap()),
                event);
    }
}
