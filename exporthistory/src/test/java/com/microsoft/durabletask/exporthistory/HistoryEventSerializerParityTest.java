// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.StringValue;
import com.google.protobuf.Value;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.durabletask.DataConverter;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;
import com.microsoft.durabletask.FailureDetails;
import com.microsoft.durabletask.OrchestrationRuntimeStatus;
import com.microsoft.durabletask.history.ContinueAsNewEvent;
import com.microsoft.durabletask.history.EntityLockGrantedEvent;
import com.microsoft.durabletask.history.EntityLockRequestedEvent;
import com.microsoft.durabletask.history.EntityOperationCalledEvent;
import com.microsoft.durabletask.history.EntityOperationCompletedEvent;
import com.microsoft.durabletask.history.EntityOperationFailedEvent;
import com.microsoft.durabletask.history.EntityOperationSignaledEvent;
import com.microsoft.durabletask.history.EntityUnlockSentEvent;
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
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
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
import java.util.concurrent.TimeUnit;

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
        List<String> golden = readGolden("/golden/reference-history-events.jsonl");
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
        List<String> golden = readGolden("/golden/reference-history-events.jsonl");
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
    void entityInputControlCharactersMatchReferenceEncoding() {
        StringBuilder controls = new StringBuilder();
        for (char ch = 0; ch < 0x20; ch++) {
            controls.append(ch);
        }
        HistoryEvent event = new EntityOperationSignaledEvent(
                1, TS, "req-control", "echo", null, controls.toString(), "@counter@one");

        // Newtonsoft inner-message JSON, wrapped with the reference export's System.Text.Json encoder.
        String expected = "{\"eventType\":\"EventSent\",\"instanceId\":\"@counter@one\",\"name\":\"op\","
                + "\"input\":\"{\\u0022op\\u0022:\\u0022echo\\u0022,\\u0022signal\\u0022:true,"
                + "\\u0022input\\u0022:\\u0022"
                + "\\\\u0000\\\\u0001\\\\u0002\\\\u0003\\\\u0004\\\\u0005\\\\u0006\\\\u0007"
                + "\\\\b\\\\t\\\\n\\\\u000b\\\\f\\\\r\\\\u000e\\\\u000f"
                + "\\\\u0010\\\\u0011\\\\u0012\\\\u0013\\\\u0014\\\\u0015\\\\u0016\\\\u0017"
                + "\\\\u0018\\\\u0019\\\\u001a\\\\u001b\\\\u001c\\\\u001d\\\\u001e\\\\u001f"
                + "\\u0022,\\u0022id\\u0022:\\u0022req-control\\u0022}\","
                + "\"eventId\":1,\"isPlayed\":false,\"timestamp\":\"2026-06-30T12:00:00Z\"}";

        assertEquals(expected + "\n", HistoryEventSerializer.serialize(Collections.singletonList(event), JSONL));
        assertEquals("[" + expected + "]", HistoryEventSerializer.serialize(Collections.singletonList(event), JSON));
    }

    @Test
    void entityLockGrantUsesReferenceEventRaisedRepresentation() throws Exception {
        HistoryEvent event = new EntityLockGrantedEvent(3, TS, "cs-1");
        String actual = HistoryEventSerializer.serialize(Collections.singletonList(event), JSONL).trim();
        assertTrue(actual.startsWith("{\"eventType\":\"EventRaised\""), actual);
        assertTrue(actual.contains("\"name\":\"cs-1\""), actual);
        assertTrue(actual.contains("\"isPlayed\":false"), actual);
        assertTrue(actual.contains("\"eventId\":3"), actual);
    }

    @Test
    void entityMessagesMatchReferenceConversionByteForByte() throws Exception {
        // Captured using .NET EntityConversionState and DT Core 3.9.0, including Newtonsoft inner-message JSON.
        List<String> golden = readGolden("/golden/reference-entity-history-events.jsonl");
        List<HistoryEvent> events = buildEntityEvents();
        String[] actual = HistoryEventSerializer.serialize(events, JSONL).split("\n");
        assertEquals(events.size(), actual.length);
        int expectedIndex = 0;
        for (int i = 0; i < events.size(); i++) {
            if (events.get(i) instanceof ExecutionStartedEvent) {
                continue;
            }
            assertEquals(golden.get(expectedIndex++), actual[i],
                    "entity wire-format mismatch at event " + events.get(i).getEventId());
        }
        assertEquals(golden.size(), expectedIndex);
        assertEquals("[" + String.join(",", actual) + "]", HistoryEventSerializer.serialize(events, JSON));
    }

    @Test
    void entityFailureFromStreamedHistoryMatchesReference() throws Exception {
        OrchestratorService.TaskFailureDetails failure = OrchestratorService.TaskFailureDetails.newBuilder()
                .setErrorType("Outer")
                .setErrorMessage("boom")
                .setStackTrace(StringValue.of("at Foo()"))
                .setInnerFailure(OrchestratorService.TaskFailureDetails.newBuilder()
                        .setErrorType("Inner")
                        .setErrorMessage("inner")
                        .setIsNonRetriable(true))
                .putProperties("code", Value.newBuilder().setNumberValue(42).build())
                .build();
        List<HistoryEvent> history = streamEntityFailure(failure);
        String expected = readGolden("/golden/reference-entity-history-events.jsonl").get(5);
        assertEquals(expected + "\n", HistoryEventSerializer.serialize(history, JSONL));
        assertEquals("[" + expected + "]", HistoryEventSerializer.serialize(history, JSON));
    }

    @Test
    void streamedEntityFailureProjectsDatePropertiesWithoutChangingPublicValues() throws Exception {
        Value values = Value.newBuilder().setListValue(ListValue.newBuilder()
                .addValues(propertyString("dt:2026-06-30T12:00:00.0000000Z"))
                .addValues(propertyString("dt:2026-06-30T12:00:00.1234000Z"))
                .addValues(propertyString("dt:2026-06-30T12:00:00.0000000"))
                .addValues(propertyString("dto:2026-06-30T12:00:00.1234567+05:30"))
                .addValues(propertyString("dto:2026-06-30T12:00:00.0000000+00:00"))
                .addValues(propertyString("dto:2026-06-30T12:00:00.1200000-07:00"))
                .addValues(propertyString("ordinary"))
                .addValues(propertyString("dt:not-a-date"))
                .addValues(propertyString("dto:2026-02-30T12:00:00+00:00"))
                .addValues(propertyString("dt:2026-06-30T12:00:00+15:00"))
                .addValues(propertyString("dto:2026-06-30T12:00:00+00:00[UTC]"))
                .addValues(Value.newBuilder().setStructValue(Struct.newBuilder()
                        .putFields("nested", Value.newBuilder().setListValue(ListValue.newBuilder()
                                .addValues(propertyString("dt:2026-06-30T12:00:00.0000001Z"))
                                .addValues(Value.newBuilder().setBoolValue(true))
                                .addValues(Value.newBuilder().setNumberValue(42))
                                .addValues(Value.newBuilder().setNullValueValue(0))).build())).build()))
                .build();
        OrchestratorService.TaskFailureDetails failure = OrchestratorService.TaskFailureDetails.newBuilder()
                .setErrorType("Outer")
                .setErrorMessage("boom")
                .putProperties("values", values)
                .setInnerFailure(OrchestratorService.TaskFailureDetails.newBuilder()
                        .setErrorType("Inner")
                        .setErrorMessage("inner")
                        .putProperties("date", propertyString("dto:2026-06-30T12:00:00.1000000+00:00")))
                .build();
        List<HistoryEvent> history = streamEntityFailure(failure);
        String expectedInput = "{\"result\":\"boom\",\"exceptionType\":\"Outer\",\"failureDetails\":"
                + "{\"ErrorType\":\"Outer\",\"ErrorMessage\":\"boom\",\"StackTrace\":null,\"InnerFailure\":"
                + "{\"ErrorType\":\"Inner\",\"ErrorMessage\":\"inner\",\"StackTrace\":null,\"InnerFailure\":null,"
                + "\"IsNonRetriable\":false,\"Properties\":{\"date\":\"2026-06-30T12:00:00.1+00:00\"}},"
                + "\"IsNonRetriable\":false,\"Properties\":{\"values\":["
                + "\"2026-06-30T12:00:00Z\",\"2026-06-30T12:00:00.1234Z\",\"2026-06-30T12:00:00\","
                + "\"2026-06-30T12:00:00.1234567+05:30\",\"2026-06-30T12:00:00+00:00\","
                + "\"2026-06-30T12:00:00.12-07:00\",\"ordinary\",\"dt:not-a-date\","
                + "\"dto:2026-02-30T12:00:00+00:00\",\"dt:2026-06-30T12:00:00+15:00\","
                + "\"dto:2026-06-30T12:00:00+00:00[UTC]\","
                + "{\"nested\":[\"2026-06-30T12:00:00.0000001Z\",true,42.0,null]}]}}}";
        String expected = "{\"eventType\":\"EventRaised\",\"name\":\"req-failed\",\"input\":\""
                + expectedInput.replace("\"", "\\u0022").replace("+", "\\u002B")
                + "\",\"eventId\":6,\"isPlayed\":false,\"timestamp\":\"2026-06-30T12:00:00.1234567Z\"}";
        assertEquals(expected + "\n", HistoryEventSerializer.serialize(history, JSONL));
        assertEquals("[" + expected + "]", HistoryEventSerializer.serialize(history, JSON));

        FailureDetails publicFailure = ((EntityOperationFailedEvent) history.get(0)).getFailureDetails();
        assertEquals("dt:2026-06-30T12:00:00.0000000Z",
                ((List<?>) publicFailure.getProperties().get("values")).get(0));
        assertEquals("dto:2026-06-30T12:00:00.1000000+00:00",
                publicFailure.getInnerFailure().getProperties().get("date"));
    }

    private static Value propertyString(String text) {
        return Value.newBuilder().setStringValue(text).build();
    }

    @Test
    void ordinaryFailuresFromStreamedHistoryPreserveNestedPropertiesInBothFormats() throws Exception {
        Value values = Value.newBuilder().setListValue(ListValue.newBuilder()
                .addValues(Value.newBuilder().setNumberValue(42.5))
                .addValues(Value.newBuilder().setBoolValue(true))
                .addValues(Value.newBuilder().setNullValueValue(0))
                .addValues(Value.newBuilder().setStructValue(Struct.newBuilder()
                        .putFields("date", propertyString("dt:2026-06-30T12:00:00.1234000Z")))))
                .build();
        OrchestratorService.TaskFailureDetails failure = OrchestratorService.TaskFailureDetails.newBuilder()
                .setErrorType("Outer")
                .setErrorMessage("boom")
                .putProperties("values", values)
                .setInnerFailure(OrchestratorService.TaskFailureDetails.newBuilder()
                        .setErrorType("Inner")
                        .setErrorMessage("inner")
                        .putProperties("date", propertyString("dto:2026-06-30T12:00:00.1000000+00:00")))
                .build();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expectedProperties = mapper.readTree(
                "{\"values\":[42.5,true,null,{\"date\":\"2026-06-30T12:00:00.1234Z\"}]}");
        JsonNode expectedInnerProperties = mapper.readTree("{\"date\":\"2026-06-30T12:00:00.1+00:00\"}");
        for (OrchestratorService.HistoryEvent event : ordinaryFailureEvents(failure)) {
            List<HistoryEvent> history = streamHistoryEvent(event);
            for (ExportFormat format : Arrays.asList(JSONL, JSON)) {
                JsonNode serialized = mapper.readTree(HistoryEventSerializer.serialize(history, format));
                JsonNode exported = format.getKind() == ExportFormatKind.JSON ? serialized.get(0) : serialized;
                assertEquals(expectedProperties, exported.path("failureDetails").get("properties"));
                assertEquals(expectedInnerProperties,
                        exported.path("failureDetails").path("innerFailure").get("properties"));
            }
        }
    }

    @Test
    void ordinaryFailuresFromStreamedHistoryPreserveEmptyPropertyObjects() throws Exception {
        OrchestratorService.TaskFailureDetails failure = OrchestratorService.TaskFailureDetails.newBuilder()
                .setErrorType("Outer")
                .setErrorMessage("boom")
                .setInnerFailure(OrchestratorService.TaskFailureDetails.newBuilder().setErrorType("Inner"))
                .build();
        ObjectMapper mapper = new ObjectMapper();
        for (OrchestratorService.HistoryEvent event : ordinaryFailureEvents(failure)) {
            List<HistoryEvent> history = streamHistoryEvent(event);
            for (ExportFormat format : Arrays.asList(JSONL, JSON)) {
                JsonNode serialized = mapper.readTree(HistoryEventSerializer.serialize(history, format));
                JsonNode exported = format.getKind() == ExportFormatKind.JSON ? serialized.get(0) : serialized;
                assertEquals(mapper.createObjectNode(), exported.path("failureDetails").get("properties"));
                assertEquals(mapper.createObjectNode(),
                        exported.path("failureDetails").path("innerFailure").get("properties"));
            }
        }
    }

    private static List<OrchestratorService.HistoryEvent> ordinaryFailureEvents(
            OrchestratorService.TaskFailureDetails failure) {
        OrchestratorService.HistoryEvent.Builder event = OrchestratorService.HistoryEvent.newBuilder()
                .setEventId(6)
                .setTimestamp(DataConverter.getTimestampFromInstant(TS));
        return Arrays.asList(
                event.clone().setTaskFailed(OrchestratorService.TaskFailedEvent.newBuilder()
                        .setTaskScheduledId(1).setFailureDetails(failure)).build(),
                event.clone().setSubOrchestrationInstanceFailed(
                        OrchestratorService.SubOrchestrationInstanceFailedEvent.newBuilder()
                                .setTaskScheduledId(1).setFailureDetails(failure)).build(),
                event.clone().setExecutionCompleted(OrchestratorService.ExecutionCompletedEvent.newBuilder()
                        .setOrchestrationStatus(OrchestratorService.OrchestrationStatus.ORCHESTRATION_STATUS_FAILED)
                        .setFailureDetails(failure)).build());
    }

    private static List<HistoryEvent> streamEntityFailure(
            OrchestratorService.TaskFailureDetails failure) throws Exception {
        OrchestratorService.HistoryEvent event = OrchestratorService.HistoryEvent.newBuilder()
                .setEventId(6)
                .setTimestamp(DataConverter.getTimestampFromInstant(
                        Instant.parse("2026-06-30T12:00:00.1234567Z")))
                .setEntityOperationFailed(OrchestratorService.EntityOperationFailedEvent.newBuilder()
                        .setRequestId("req-failed")
                        .setFailureDetails(failure))
                .build();
        return streamHistoryEvent(event);
    }

    private static List<HistoryEvent> streamHistoryEvent(OrchestratorService.HistoryEvent event) throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
                    @Override
                    public void streamInstanceHistory(
                            OrchestratorService.StreamInstanceHistoryRequest request,
                            StreamObserver<OrchestratorService.HistoryChunk> responseObserver) {
                        responseObserver.onNext(OrchestratorService.HistoryChunk.newBuilder()
                                .addEvents(event)
                                .build());
                        responseObserver.onCompleted();
                    }
                })
                .build();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        try {
            server.start();
            try (DurableTaskClient client = new DurableTaskGrpcClientBuilder().grpcChannel(channel).build()) {
                List<HistoryEvent> history = client.getOrchestrationHistory("instance-1");
                assertEquals(1, history.size());
                return history;
            }
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
            assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
            assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static List<HistoryEvent> buildEntityEvents() throws Exception {
        Instant timestamp = Instant.parse("2026-06-30T12:00:00.1234567Z");
        Instant due = Instant.parse("2026-06-30T12:05:00.7654321Z");
        FailureDetails inner = failure("Inner", "inner", null, true, null);
        FailureDetails outer = failure(
                "Outer", "boom", "at Foo()", false, inner, Collections.singletonMap("code", 42.0));
        List<String> lockSet = Arrays.asList("@Counter@", "@counter@a@b");
        return Arrays.asList(
                new ExecutionStartedEvent(0, timestamp, "EntityWorkflow", null, null,
                        new OrchestrationInstance("order-42", "e1"),
                        null, null, null, null, Collections.emptyMap()),
                new EntityOperationCalledEvent(1, timestamp, "req-call", "Add", null,
                        "caf\u00e9 &<>'+\u001f\u0085\u2028\u2029",
                        "ignored-parent", "ignored-execution", "@counter@one"),
                new EntityOperationSignaledEvent(2, timestamp, "req-signal", "Increment", due, "1", "@counter@two"),
                new EntityOperationSignaledEvent(3, timestamp, "req-empty", "Reset", null, null, "@counter@two"),
                new EntityOperationCompletedEvent(4, timestamp, "req-null", null),
                new EntityOperationCompletedEvent(5, timestamp, "req-complete", "{\"value\":42}"),
                new EntityOperationFailedEvent(6, timestamp, "req-failed", outer),
                new EntityLockRequestedEvent(7, timestamp, "lock-1", lockSet, 1, "ignored-parent"),
                new EntityLockRequestedEvent(8, timestamp, "lock-0", lockSet, 0, "ignored-parent"),
                new EntityLockGrantedEvent(9, timestamp, "lock-1"),
                new EntityUnlockSentEvent(10, timestamp, "lock-1", "ignored-parent", "@counter@one"),
                new ExecutionStartedEvent(11, timestamp, "EntityWorkflow", null, null,
                        new OrchestrationInstance("order-43", null),
                        null, null, null, null, Collections.emptyMap()),
                new EntityOperationCalledEvent(12, timestamp, "req-next", "Get",
                        Instant.parse("2026-06-30T12:05:00Z"), null, null, null, "@counter@one"));
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

    private static List<String> readGolden(String resource) throws Exception {
        List<String> lines = new ArrayList<>();
        try (InputStream in = HistoryEventSerializerParityTest.class.getResourceAsStream(resource);
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
        return failure(errorType, message, stackTrace, nonRetriable, inner, null);
    }

    private static FailureDetails failure(
            String errorType, String message, String stackTrace, boolean nonRetriable,
            FailureDetails inner, Map<String, Object> properties) throws Exception {
        Constructor<FailureDetails> ctor = FailureDetails.class.getDeclaredConstructor(
                String.class, String.class, String.class, boolean.class, FailureDetails.class, Map.class);
        ctor.setAccessible(true);
        return ctor.newInstance(errorType, message, stackTrace, nonRetriable, inner, properties);
    }
}
