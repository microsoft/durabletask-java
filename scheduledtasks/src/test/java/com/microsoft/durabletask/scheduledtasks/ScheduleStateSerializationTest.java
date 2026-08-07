// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies persisted schedule state uses the .NET wire shape (PascalCase names, numeric status ordinal,
 * {@code TimeSpan} interval, {@code DateTimeOffset} timestamps, string input) and round-trips through the same
 * Jackson configuration the worker uses.
 */
class ScheduleStateSerializationTest {

    private static final ObjectMapper MAPPER = JsonMapper.builder().findAndAddModules().build();
    private static final OffsetDateTime CREATED =
            OffsetDateTime.of(2026, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    private static ScheduleState activeState() {
        ScheduleState state = new ScheduleState();
        state.setStatus(ScheduleStatus.ACTIVE);
        state.setExecutionToken("tok123");
        state.setScheduleCreatedAt(CREATED);
        state.setScheduleLastModifiedAt(CREATED);
        state.setScheduleConfiguration(ScheduleConfiguration.fromCreateOptions(
                new ScheduleCreationOptions("s1", "orch", Duration.ofHours(1))
                        .setOrchestrationInput("hello")
                        .setStartAt(CREATED)));
        return state;
    }

    @Test
    void serializesDotNetShape() throws Exception {
        String json = MAPPER.writeValueAsString(activeState());
        JsonNode node = MAPPER.readTree(json);

        assertEquals(1, node.get("Status").asInt());
        assertEquals("tok123", node.get("ExecutionToken").asText());
        assertTrue(node.get("NextRunAt").isNull());

        JsonNode config = node.get("ScheduleConfiguration");
        assertEquals("s1", config.get("ScheduleId").asText());
        assertEquals("orch", config.get("OrchestrationName").asText());
        assertEquals("hello", config.get("OrchestrationInput").asText());
        assertEquals("01:00:00", config.get("Interval").asText());
        assertFalse(config.get("StartImmediatelyIfLate").asBoolean());
        assertTrue(config.get("StartAt").asText()
                .matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{7}[+-]\\d{2}:\\d{2}"));
    }

    @Test
    void containsNoJavaTypeMetadata() throws Exception {
        String json = MAPPER.writeValueAsString(activeState());
        assertFalse(json.contains("@class"));
        assertFalse(json.contains("__class"));
        assertFalse(json.contains("com.microsoft"));
    }

    @Test
    void roundTrips() throws Exception {
        String json = MAPPER.writeValueAsString(activeState());
        ScheduleState restored = MAPPER.readValue(json, ScheduleState.class);

        assertEquals(ScheduleStatus.ACTIVE, restored.getStatus());
        assertEquals("tok123", restored.getExecutionToken());
        assertNotNull(restored.getScheduleConfiguration());
        assertEquals(Duration.ofHours(1), restored.getScheduleConfiguration().getInterval());
        assertEquals("hello", restored.getScheduleConfiguration().getOrchestrationInput());
        assertEquals(CREATED.toInstant(), restored.getScheduleConfiguration().getStartAt().toInstant());
        assertEquals(CREATED.toInstant(), restored.getScheduleCreatedAt().toInstant());
    }

    @Test
    void deserializesLegacyStringStatus() throws Exception {
        String json = "{\"Status\":\"Active\",\"ExecutionToken\":\"abc\"}";
        ScheduleState restored = MAPPER.readValue(json, ScheduleState.class);
        assertEquals(ScheduleStatus.ACTIVE, restored.getStatus());
        assertEquals("abc", restored.getExecutionToken());
    }

    @Test
    void keepsGeneratedTokenWhenMissing() throws Exception {
        ScheduleState restored = MAPPER.readValue("{\"Status\":1}", ScheduleState.class);
        assertNotNull(restored.getExecutionToken());
        assertFalse(restored.getExecutionToken().isEmpty());
    }
}
