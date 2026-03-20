// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PayloadInterceptionHelper.
 */
public class PayloadInterceptionHelperTest {

    private static final String TOKEN_PREFIX = "test://token/";

    /**
     * A simple in-memory PayloadStore for testing.
     */
    private static class TestPayloadStore implements PayloadStore {
        private final Map<String, String> blobs = new HashMap<>();

        @Override
        public String upload(String payload) {
            String token = TOKEN_PREFIX + UUID.randomUUID().toString();
            blobs.put(token, payload);
            return token;
        }

        @Override
        public String download(String token) {
            String value = blobs.get(token);
            if (value == null) {
                throw new IllegalArgumentException("Unknown token: " + token);
            }
            return value;
        }

        @Override
        public boolean isKnownPayloadToken(String value) {
            return value != null && value.startsWith(TOKEN_PREFIX);
        }

        /** Pre-load a token->payload mapping for resolution tests. */
        void seed(String token, String payload) {
            blobs.put(token, payload);
        }
    }

    private PayloadHelper createHelper(TestPayloadStore store) {
        LargePayloadOptions options = new LargePayloadOptions.Builder()
            .setThresholdBytes(10)
            .setMaxExternalizedPayloadBytes(100_000)
            .build();
        return new PayloadHelper(store, options);
    }

    // ---- Resolve tests ----

    @Test
    void resolveOrchestratorRequest_noTokens_returnsOriginal() {
        TestPayloadStore store = new TestPayloadStore();
        PayloadHelper helper = createHelper(store);

        HistoryEvent event = buildExecutionStartedEvent("regular-input");
        OrchestratorRequest request = OrchestratorRequest.newBuilder()
            .setInstanceId("test-1")
            .addNewEvents(event)
            .build();

        OrchestratorRequest result = PayloadInterceptionHelper.resolveOrchestratorRequestPayloads(request, helper);
        assertSame(request, result, "Should return same object when no tokens found");
    }

    @Test
    void resolveOrchestratorRequest_withToken_resolvesInput() {
        TestPayloadStore store = new TestPayloadStore();
        String token = TOKEN_PREFIX + "abc";
        String originalPayload = "large-payload-data";
        store.seed(token, originalPayload);
        PayloadHelper helper = createHelper(store);

        HistoryEvent event = buildExecutionStartedEvent(token);
        OrchestratorRequest request = OrchestratorRequest.newBuilder()
            .setInstanceId("test-2")
            .addNewEvents(event)
            .build();

        OrchestratorRequest result = PayloadInterceptionHelper.resolveOrchestratorRequestPayloads(request, helper);
        assertNotSame(request, result);
        assertEquals(originalPayload, result.getNewEvents(0).getExecutionStarted().getInput().getValue());
    }

    @Test
    void resolveOrchestratorRequest_taskCompleted_resolvesResult() {
        TestPayloadStore store = new TestPayloadStore();
        String token = TOKEN_PREFIX + "task-result";
        String originalPayload = "{\"key\":\"value\"}";
        store.seed(token, originalPayload);
        PayloadHelper helper = createHelper(store);

        HistoryEvent event = HistoryEvent.newBuilder()
            .setEventId(1)
            .setTaskCompleted(TaskCompletedEvent.newBuilder()
                .setTaskScheduledId(1)
                .setResult(StringValue.of(token))
                .build())
            .build();

        OrchestratorRequest request = OrchestratorRequest.newBuilder()
            .setInstanceId("test-3")
            .addPastEvents(event)
            .build();

        OrchestratorRequest result = PayloadInterceptionHelper.resolveOrchestratorRequestPayloads(request, helper);
        assertNotSame(request, result);
        assertEquals(originalPayload, result.getPastEvents(0).getTaskCompleted().getResult().getValue());
    }

    @Test
    void resolveOrchestratorRequest_eventRaised_resolvesInput() {
        TestPayloadStore store = new TestPayloadStore();
        String token = TOKEN_PREFIX + "event-data";
        String originalPayload = "event payload content";
        store.seed(token, originalPayload);
        PayloadHelper helper = createHelper(store);

        HistoryEvent event = HistoryEvent.newBuilder()
            .setEventId(2)
            .setEventRaised(EventRaisedEvent.newBuilder()
                .setName("TestEvent")
                .setInput(StringValue.of(token))
                .build())
            .build();

        OrchestratorRequest request = OrchestratorRequest.newBuilder()
            .setInstanceId("test-4")
            .addNewEvents(event)
            .build();

        OrchestratorRequest result = PayloadInterceptionHelper.resolveOrchestratorRequestPayloads(request, helper);
        assertEquals(originalPayload, result.getNewEvents(0).getEventRaised().getInput().getValue());
    }

    @Test
    void resolveActivityRequest_withToken_resolvesInput() {
        TestPayloadStore store = new TestPayloadStore();
        String token = TOKEN_PREFIX + "activity-input";
        String originalPayload = "activity input data";
        store.seed(token, originalPayload);
        PayloadHelper helper = createHelper(store);

        ActivityRequest request = ActivityRequest.newBuilder()
            .setName("TestActivity")
            .setInput(StringValue.of(token))
            .build();

        ActivityRequest result = PayloadInterceptionHelper.resolveActivityRequestPayloads(request, helper);
        assertNotSame(request, result);
        assertEquals(originalPayload, result.getInput().getValue());
    }

    @Test
    void resolveActivityRequest_noToken_returnsOriginal() {
        TestPayloadStore store = new TestPayloadStore();
        PayloadHelper helper = createHelper(store);

        ActivityRequest request = ActivityRequest.newBuilder()
            .setName("TestActivity")
            .setInput(StringValue.of("regular data"))
            .build();

        ActivityRequest result = PayloadInterceptionHelper.resolveActivityRequestPayloads(request, helper);
        assertSame(request, result, "Should return same object when no token found");
    }

    // ---- Externalize tests ----

    @Test
    void externalizeOrchestratorResponse_smallPayloads_returnsOriginal() {
        TestPayloadStore store = new TestPayloadStore();
        PayloadHelper helper = createHelper(store);

        OrchestratorResponse response = OrchestratorResponse.newBuilder()
            .setInstanceId("test-5")
            .setCustomStatus(StringValue.of("ok"))
            .addActions(OrchestratorAction.newBuilder()
                .setId(1)
                .setScheduleTask(ScheduleTaskAction.newBuilder()
                    .setName("SmallTask")
                    .setInput(StringValue.of("tiny"))
                    .build())
                .build())
            .build();

        OrchestratorResponse result = PayloadInterceptionHelper.externalizeOrchestratorResponsePayloads(response, helper);
        assertSame(response, result, "Small payloads should not be externalized");
    }

    @Test
    void externalizeOrchestratorResponse_largeScheduleTaskInput_externalizes() {
        TestPayloadStore store = new TestPayloadStore();
        PayloadHelper helper = createHelper(store);

        String largeInput = "x".repeat(100); // > 10 byte threshold
        OrchestratorResponse response = OrchestratorResponse.newBuilder()
            .setInstanceId("test-6")
            .addActions(OrchestratorAction.newBuilder()
                .setId(1)
                .setScheduleTask(ScheduleTaskAction.newBuilder()
                    .setName("LargeTask")
                    .setInput(StringValue.of(largeInput))
                    .build())
                .build())
            .build();

        OrchestratorResponse result = PayloadInterceptionHelper.externalizeOrchestratorResponsePayloads(response, helper);
        assertNotSame(response, result);

        String externalizedInput = result.getActions(0).getScheduleTask().getInput().getValue();
        assertTrue(store.isKnownPayloadToken(externalizedInput), "Input should be externalized to a token");
    }

    @Test
    void externalizeOrchestratorResponse_largeCompleteResult_externalizes() {
        TestPayloadStore store = new TestPayloadStore();
        PayloadHelper helper = createHelper(store);

        String largeResult = "y".repeat(200);
        OrchestratorResponse response = OrchestratorResponse.newBuilder()
            .setInstanceId("test-7")
            .addActions(OrchestratorAction.newBuilder()
                .setId(1)
                .setCompleteOrchestration(CompleteOrchestrationAction.newBuilder()
                    .setOrchestrationStatus(OrchestrationStatus.ORCHESTRATION_STATUS_COMPLETED)
                    .setResult(StringValue.of(largeResult))
                    .build())
                .build())
            .build();

        OrchestratorResponse result = PayloadInterceptionHelper.externalizeOrchestratorResponsePayloads(response, helper);
        assertNotSame(response, result);

        String externalizedResult = result.getActions(0).getCompleteOrchestration().getResult().getValue();
        assertTrue(store.isKnownPayloadToken(externalizedResult));
    }

    @Test
    void externalizeOrchestratorResponse_largeCustomStatus_externalizes() {
        TestPayloadStore store = new TestPayloadStore();
        PayloadHelper helper = createHelper(store);

        String largeStatus = "s".repeat(100);
        OrchestratorResponse response = OrchestratorResponse.newBuilder()
            .setInstanceId("test-8")
            .setCustomStatus(StringValue.of(largeStatus))
            .build();

        OrchestratorResponse result = PayloadInterceptionHelper.externalizeOrchestratorResponsePayloads(response, helper);
        assertNotSame(response, result);

        String externalizedStatus = result.getCustomStatus().getValue();
        assertTrue(store.isKnownPayloadToken(externalizedStatus));
    }

    @Test
    void roundTrip_externalizeAndResolve_scheduleTaskInput() {
        TestPayloadStore store = new TestPayloadStore();
        PayloadHelper helper = createHelper(store);

        String originalInput = "large-task-input-data-that-exceeds-threshold";
        OrchestratorResponse response = OrchestratorResponse.newBuilder()
            .setInstanceId("test-9")
            .addActions(OrchestratorAction.newBuilder()
                .setId(1)
                .setScheduleTask(ScheduleTaskAction.newBuilder()
                    .setName("Task")
                    .setInput(StringValue.of(originalInput))
                    .build())
                .build())
            .build();

        // Externalize
        OrchestratorResponse externalized = PayloadInterceptionHelper.externalizeOrchestratorResponsePayloads(response, helper);
        String token = externalized.getActions(0).getScheduleTask().getInput().getValue();
        assertTrue(store.isKnownPayloadToken(token));

        // Simulate the payload arriving as a TaskScheduled history event
        HistoryEvent taskScheduled = HistoryEvent.newBuilder()
            .setEventId(1)
            .setTaskScheduled(TaskScheduledEvent.newBuilder()
                .setName("Task")
                .setInput(StringValue.of(token))
                .build())
            .build();

        OrchestratorRequest request = OrchestratorRequest.newBuilder()
            .setInstanceId("test-9")
            .addPastEvents(taskScheduled)
            .build();

        // Resolve
        OrchestratorRequest resolved = PayloadInterceptionHelper.resolveOrchestratorRequestPayloads(request, helper);
        assertEquals(originalInput, resolved.getPastEvents(0).getTaskScheduled().getInput().getValue());
    }

    // ---- Helper methods ----

    private static HistoryEvent buildExecutionStartedEvent(String input) {
        return HistoryEvent.newBuilder()
            .setEventId(-1)
            .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                .setName("TestOrchestration")
                .setVersion(StringValue.of(""))
                .setInput(StringValue.of(input))
                .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                    .setInstanceId("test-instance")
                    .build())
                .build())
            .build();
    }
}
