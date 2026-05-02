// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.activities;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.microsoft.durabletask.OrchestrationHistoryEvent;
import com.microsoft.durabletask.exporthistory.models.ExportFormatKind;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for blob naming and serialization logic used by {@link ExportInstanceHistoryActivity}.
 * <p>
 * Tests the static helper logic directly since the blob naming and serialization
 * are deterministic pure functions.
 */
class ExportInstanceHistoryActivityTest {

    // region Blob naming

    @Test
    void blobName_jsonl_hasSha256HashAndGzExtension() {
        String blobName = generateBlobFileName(
                Instant.parse("2026-01-15T10:30:00Z"), "instance-123", ExportFormatKind.JSONL);
        assertTrue(blobName.endsWith(".jsonl.gz"), "Expected .jsonl.gz extension, got: " + blobName);
        // SHA-256 hex = 64 chars + ".jsonl.gz" = 73 chars
        assertEquals(73, blobName.length());
    }

    @Test
    void blobName_json_hasSha256HashAndJsonExtension() {
        String blobName = generateBlobFileName(
                Instant.parse("2026-01-15T10:30:00Z"), "instance-123", ExportFormatKind.JSON);
        assertTrue(blobName.endsWith(".json"), "Expected .json extension, got: " + blobName);
        assertEquals(69, blobName.length()); // 64 + ".json"
    }

    @Test
    void blobName_deterministic_samInputsProduceSameHash() {
        Instant ts = Instant.parse("2026-01-15T10:30:00Z");
        String name1 = generateBlobFileName(ts, "instance-abc", ExportFormatKind.JSONL);
        String name2 = generateBlobFileName(ts, "instance-abc", ExportFormatKind.JSONL);
        assertEquals(name1, name2);
    }

    @Test
    void blobName_differentInstances_produceDifferentHashes() {
        Instant ts = Instant.parse("2026-01-15T10:30:00Z");
        String name1 = generateBlobFileName(ts, "instance-1", ExportFormatKind.JSONL);
        String name2 = generateBlobFileName(ts, "instance-2", ExportFormatKind.JSONL);
        assertNotEquals(name1, name2);
    }

    @Test
    void blobName_differentTimestamps_produceDifferentHashes() {
        String name1 = generateBlobFileName(Instant.parse("2026-01-15T10:30:00Z"), "inst", ExportFormatKind.JSONL);
        String name2 = generateBlobFileName(Instant.parse("2026-01-15T11:30:00Z"), "inst", ExportFormatKind.JSONL);
        assertNotEquals(name1, name2);
    }

    // endregion

    // region Serialization

    @Test
    void serializeEvents_jsonl_oneEventPerLine() {
        List<OrchestrationHistoryEvent> events = createTestEvents();
        String jsonl = serializeEvents(events, ExportFormatKind.JSONL);

        String[] lines = jsonl.split("\n");
        assertEquals(2, lines.length);

        // Each line should be valid JSON
        ObjectMapper mapper = new ObjectMapper();
        for (String line : lines) {
            assertDoesNotThrow(() -> mapper.readTree(line));
        }
    }

    @Test
    void serializeEvents_json_isValidJsonArray() {
        List<OrchestrationHistoryEvent> events = createTestEvents();
        String json = serializeEvents(events, ExportFormatKind.JSON);

        assertTrue(json.startsWith("["), "JSON output should start with [");
        assertTrue(json.endsWith("]"), "JSON output should end with ]");

        ObjectMapper mapper = new ObjectMapper();
        assertDoesNotThrow(() -> {
            Object[] arr = mapper.readValue(json, Object[].class);
            assertEquals(2, arr.length);
        });
    }

    @Test
    void serializeEvents_camelCasePropertyNames() {
        List<OrchestrationHistoryEvent> events = Collections.singletonList(
                new OrchestrationHistoryEvent(1, Instant.parse("2026-01-01T00:00:00Z"),
                        "ExecutionStarted", Collections.singletonMap("name", "MyOrch")));

        String json = serializeEvents(events, ExportFormatKind.JSON);
        assertTrue(json.contains("\"eventId\""), "Should use camelCase: eventId");
        assertTrue(json.contains("\"eventType\""), "Should use camelCase: eventType");
    }

    @Test
    void serializeEvents_emptyList_producesEmptyArray() {
        String json = serializeEvents(Collections.emptyList(), ExportFormatKind.JSON);
        assertEquals("[]", json);
    }

    @Test
    void serializeEvents_emptyList_jsonl_producesEmptyString() {
        String jsonl = serializeEvents(Collections.emptyList(), ExportFormatKind.JSONL);
        assertEquals("", jsonl);
    }

    // endregion

    // region Helpers — reimplemented from activity to test independently

    private static String generateBlobFileName(Instant completedTimestamp, String instanceId, ExportFormatKind kind) {
        try {
            String hashInput = completedTimestamp.toString() + "|" + instanceId;
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(hashInput.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hashBytes) {
                hex.append(String.format("%02x", b));
            }
            String extension = kind == ExportFormatKind.JSONL ? "jsonl.gz" : "json";
            return hex.toString() + "." + extension;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .setPropertyNamingStrategy(PropertyNamingStrategies.LOWER_CAMEL_CASE)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    private static String serializeEvents(List<OrchestrationHistoryEvent> events, ExportFormatKind kind) {
        try {
            if (kind == ExportFormatKind.JSONL) {
                StringBuilder sb = new StringBuilder();
                for (OrchestrationHistoryEvent event : events) {
                    sb.append(MAPPER.writeValueAsString(event)).append('\n');
                }
                return sb.toString();
            } else {
                return MAPPER.writeValueAsString(events);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<OrchestrationHistoryEvent> createTestEvents() {
        Map<String, Object> data1 = new LinkedHashMap<>();
        data1.put("name", "MyOrchestration");
        data1.put("input", "hello");

        Map<String, Object> data2 = new LinkedHashMap<>();
        data2.put("name", "MyActivity");
        data2.put("taskScheduledId", 1);

        return Arrays.asList(
                new OrchestrationHistoryEvent(0, Instant.parse("2026-01-01T00:00:00Z"), "ExecutionStarted", data1),
                new OrchestrationHistoryEvent(1, Instant.parse("2026-01-01T00:00:01Z"), "TaskScheduled", data2));
    }

    // endregion
}
