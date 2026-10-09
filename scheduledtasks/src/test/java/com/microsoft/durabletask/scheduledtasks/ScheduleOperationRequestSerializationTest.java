// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.scheduledtasks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.microsoft.durabletask.EntityInstanceId;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Verifies {@link ScheduleOperationRequest} serializes through the same Jackson configuration the worker uses.
 * <p>
 * Regression guard: the request is the orchestration input for every mutating client operation, so if it has no
 * Jackson-discoverable properties the client fails at {@code scheduleNewOrchestrationInstance} before reaching the
 * backend. This is only exercised end-to-end, so it must be covered here.
 */
class ScheduleOperationRequestSerializationTest {

    private static final ObjectMapper MAPPER = JsonMapper.builder().findAndAddModules().build();

    @Test
    void serializesWithOptionsPayload() throws Exception {
        ScheduleOperationRequest request = new ScheduleOperationRequest(
                new EntityInstanceId(Schedule.NAME, "s1"),
                ScheduleTransitions.CREATE_SCHEDULE,
                new ScheduleCreationOptions("s1", "orch", Duration.ofSeconds(30)).setOrchestrationInput("world"));

        String json = MAPPER.writeValueAsString(request);
        assertNotNull(json);

        ScheduleOperationRequest restored = MAPPER.readValue(json, ScheduleOperationRequest.class);
        assertEquals("s1", restored.getEntityId().getKey());
        assertEquals(ScheduleTransitions.CREATE_SCHEDULE, restored.getOperationName());
        assertNotNull(restored.getInput());
    }

    @Test
    void serializesWithNullInput() throws Exception {
        ScheduleOperationRequest request = new ScheduleOperationRequest(
                new EntityInstanceId(Schedule.NAME, "s1"), ScheduleTransitions.PAUSE_SCHEDULE, null);

        ScheduleOperationRequest restored =
                MAPPER.readValue(MAPPER.writeValueAsString(request), ScheduleOperationRequest.class);
        assertEquals(ScheduleTransitions.PAUSE_SCHEDULE, restored.getOperationName());
    }
}
