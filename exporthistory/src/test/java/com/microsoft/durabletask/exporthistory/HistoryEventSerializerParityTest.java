// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.FailureDetails;
import com.microsoft.durabletask.OrchestrationRuntimeStatus;
import com.microsoft.durabletask.history.ContinueAsNewEvent;
import com.microsoft.durabletask.history.EntityLockGrantedEvent;
import com.microsoft.durabletask.history.EventRaisedEvent;
import com.microsoft.durabletask.history.EventSentEvent;
import com.microsoft.durabletask.history.ExecutionCompletedEvent;
import com.microsoft.durabletask.history.ExecutionResumedEvent;
import com.microsoft.durabletask.history.ExecutionRewoundEvent;
import com.microsoft.durabletask.history.ExecutionStartedEvent;
import com.microsoft.durabletask.history.ExecutionSuspendedEvent;
import com.microsoft.durabletask.history.ExecutionTerminatedEvent;
import com.microsoft.durabletask.history.GenericEvent;
import com.microsoft.durabletask.history.HistoryEvent;
import com.microsoft.durabletask.history.HistoryStateEvent;
import com.microsoft.durabletask.history.OrchestrationInstance;
import com.microsoft.durabletask.history.OrchestrationState;
import com.microsoft.durabletask.history.OrchestratorCompletedEvent;
import com.microsoft.durabletask.history.OrchestratorStartedEvent;
import com.microsoft.durabletask.history.ParentInstanceInfo;
import com.microsoft.durabletask.history.SubOrchestrationInstanceCompletedEvent;
import com.microsoft.durabletask.history.SubOrchestrationInstanceCreatedEvent;
import com.microsoft.durabletask.history.SubOrchestrationInstanceFailedEvent;
import com.microsoft.durabletask.history.TaskCompletedEvent;
import com.microsoft.durabletask.history.TaskFailedEvent;
import com.microsoft.durabletask.history.TaskScheduledEvent;
import com.microsoft.durabletask.history.TimerCreatedEvent;
import com.microsoft.durabletask.history.TimerFiredEvent;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins {@link HistoryEventSerializer} output against golden JSON captured from the reference export implementation.
 * The golden lines live in {@code src/test/resources/golden/reference-history-events.jsonl} and must match
 * byte-for-byte.
 */
class HistoryEventSerializerParityTest {

    private static final Instant TS = Instant.parse("2026-06-30T12:00:00Z");
    private static final Instant FIRE = Instant.parse("2026-06-30T12:05:00Z");
    private static final ExportFormat JSONL = new ExportFormat(ExportFormatKind.JSONL, "1.0");
    private static final ExportFormat JSON = new ExportFormat(ExportFormatKind.JSON, "1.0");

    @Test
    void serializesEachEventByteForByteAgainstReference() throws Exception {
        List<String> golden = readGolden();
        List<HistoryEvent> events = buildEvents();
        assertEquals(golden.size(), events.size(), "golden line count vs event count");

        for (int i = 0; i < events.size(); i++) {
            HistoryEvent event = events.get(i);
            String actual = HistoryEventSerializer.serialize(Collections.singletonList(event), JSONL);
            assertEquals(golden.get(i) + "\n", actual,
                    "byte mismatch at index " + i + " (" + event.getClass().getSimpleName() + ")");
        }
    }

    @Test
    void serializesJsonArrayFormat() throws Exception {
        List<String> golden = readGolden();
        List<HistoryEvent> two = Arrays.asList(
                new OrchestratorStartedEvent(0, TS),
                new GenericEvent(15, TS, "some-data"));
        String actual = HistoryEventSerializer.serialize(two, JSON);
        // golden index 0 = OrchestratorStarted, index 19 = GenericEvent.
        assertEquals("[" + golden.get(0) + "," + golden.get(19) + "]", actual);
    }

    @Test
    void escapesStringsLikeReferenceEncoder() throws Exception {
        HistoryEvent event = new EventRaisedEvent(0, TS, "n", "caf\u00e9 \uD83C\uDF89 a&b<c>d'e+f`g");
        String actual = HistoryEventSerializer.serialize(Collections.singletonList(event), JSONL).trim();
        String expectedInput = "caf\\u00E9 \\uD83C\\uDF89 a\\u0026b\\u003Cc\\u003Ed\\u0027e\\u002Bf\\u0060g";
        assertTrue(actual.contains("\"input\":\"" + expectedInput + "\""), actual);
    }

    @Test
    void entityEventGetsJavaNativeEventTypeDiscriminator() throws Exception {
        HistoryEvent event = new EntityLockGrantedEvent(3, TS, "cs-1");
        String actual = HistoryEventSerializer.serialize(Collections.singletonList(event), JSONL).trim();
        assertTrue(actual.startsWith("{\"eventType\":\"EntityLockGranted\""), actual);
        assertTrue(actual.contains("\"eventId\":3"), actual);
    }

    private static List<HistoryEvent> buildEvents() throws Exception {
        FailureDetails inner = failure("System.NullReferenceException", "npe", "  at Bar()", true, null);
        FailureDetails outer = failure("System.InvalidOperationException", "boom", "  at Foo()", false, inner);

        return Arrays.asList(
                new OrchestratorStartedEvent(0, TS),
                new OrchestratorCompletedEvent(0, TS),
                new ExecutionStartedEvent(0, TS, "ProcessOrder", "2.1", "\"widget\"",
                        new OrchestrationInstance("order-42", "e1"),
                        new ParentInstanceInfo(5, "Parent", "1.0", new OrchestrationInstance("parent-1", "pe1")),
                        FIRE, null, null, Collections.emptyMap()),
                new ExecutionCompletedEvent(1, TS, OrchestrationRuntimeStatus.COMPLETED, "\"done\"", null),
                new ExecutionCompletedEvent(1, TS, OrchestrationRuntimeStatus.FAILED, null, outer),
                new ExecutionTerminatedEvent(2, TS, "\"stop\"", false),
                new ExecutionSuspendedEvent(3, TS, "\"pause\""),
                new ExecutionResumedEvent(4, TS, "\"go\""),
                new ExecutionRewoundEvent(5, TS, null, null, null, null, null, null, null, null, null),
                new TaskScheduledEvent(6, TS, "ChargeCard", null, "\"widget\"", null,
                        Collections.singletonMap("env", "prod")),
                new TaskCompletedEvent(7, TS, 6, "\"charged\""),
                new TaskFailedEvent(8, TS, 6, outer),
                new SubOrchestrationInstanceCreatedEvent(9, TS, "child-1", "ChildOrch", "1.0", "\"sub\"", null,
                        Collections.emptyMap()),
                new SubOrchestrationInstanceCompletedEvent(10, TS, 9, "\"subdone\""),
                new SubOrchestrationInstanceFailedEvent(11, TS, 9, outer),
                new TimerCreatedEvent(12, TS, FIRE),
                new TimerFiredEvent(99, TS, FIRE, 12),
                new EventSentEvent(13, TS, "target-1", "approve", "\"payload\""),
                new EventRaisedEvent(14, TS, "approve", "\"payload\""),
                new GenericEvent(15, TS, "some-data"),
                new ContinueAsNewEvent(16, TS, "\"nextInput\""),
                new HistoryStateEvent(17, TS, new OrchestrationState(
                        "order-42", "ProcessOrder", "1.0", OrchestrationRuntimeStatus.COMPLETED,
                        FIRE, TS, TS, null, "\"widget\"", "\"done\"", "custom-status", null, null, null,
                        Collections.emptyMap())));
    }

    private static List<String> readGolden() throws Exception {
        List<String> lines = new ArrayList<>();
        try (InputStream in = HistoryEventSerializerParityTest.class
                .getResourceAsStream("/golden/reference-history-events.jsonl");
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }

    private static FailureDetails failure(
            String errorType, String message, String stackTrace, boolean nonRetriable, FailureDetails inner)
            throws Exception {
        Constructor<FailureDetails> ctor = FailureDetails.class.getDeclaredConstructor(
                String.class, String.class, String.class, boolean.class, FailureDetails.class, Map.class);
        ctor.setAccessible(true);
        return ctor.newInstance(errorType, message, stackTrace, nonRetriable, inner, null);
    }
}
