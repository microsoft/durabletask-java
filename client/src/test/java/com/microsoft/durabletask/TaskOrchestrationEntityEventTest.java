// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for orchestration ↔ entity integration (Phase 4).
 * <p>
 * These tests construct {@link HistoryEvent} protobufs manually, run them through
 * {@link TaskOrchestrationExecutor}, and assert on the returned actions and orchestration
 * state. This mirrors the pattern of {@link TaskOrchestrationExecutorTest}.
 */
public class TaskOrchestrationEntityEventTest {

    private static final Logger logger = Logger.getLogger(TaskOrchestrationEntityEventTest.class.getName());
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    // region Helper methods

    private TaskOrchestrationExecutor createExecutor(String orchestratorName, TaskOrchestration orchestration) {
        HashMap<String, TaskOrchestrationFactory> factories = new HashMap<>();
        factories.put(orchestratorName, new TaskOrchestrationFactory() {
            @Override
            public String getName() {
                return orchestratorName;
            }

            @Override
            public TaskOrchestration create() {
                return orchestration;
            }
        });
        return new TaskOrchestrationExecutor(
                factories,
                new JacksonDataConverter(),
                Duration.ofDays(3),
                logger,
                null);
    }

    private HistoryEvent orchestratorStarted() {
        return HistoryEvent.newBuilder()
                .setEventId(-1)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setOrchestratorStarted(OrchestratorStartedEvent.getDefaultInstance())
                .build();
    }

    private HistoryEvent executionStarted(String name, String input) {
        return HistoryEvent.newBuilder()
                .setEventId(-1)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                        .setName(name)
                        .setVersion(StringValue.of(""))
                        .setInput(StringValue.of(input != null ? input : "null"))
                        .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                .setInstanceId("test-instance-id")
                                .build())
                        .build())
                .build();
    }

    private HistoryEvent orchestratorCompleted() {
        return HistoryEvent.newBuilder()
                .setEventId(-1)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setOrchestratorCompleted(OrchestratorCompletedEvent.getDefaultInstance())
                .build();
    }

    private HistoryEvent entityOperationSignaledEvent(int eventId) {
        return HistoryEvent.newBuilder()
                .setEventId(eventId)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setEntityOperationSignaled(EntityOperationSignaledEvent.newBuilder()
                        .setRequestId("signal-request-id")
                        .setOperation("add")
                        .setTargetInstanceId(StringValue.of("@Counter@c1"))
                        .build())
                .build();
    }

    private HistoryEvent entityOperationCalledEvent(int eventId) {
        return HistoryEvent.newBuilder()
                .setEventId(eventId)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setEntityOperationCalled(EntityOperationCalledEvent.newBuilder()
                        .setRequestId("call-request-id")
                        .setOperation("get")
                        .setTargetInstanceId(StringValue.of("@Counter@c1"))
                        .build())
                .build();
    }

    private HistoryEvent entityOperationCompletedEvent(String requestId, String output) {
        EntityOperationCompletedEvent.Builder builder = EntityOperationCompletedEvent.newBuilder()
                .setRequestId(requestId);
        if (output != null) {
            builder.setOutput(StringValue.of(output));
        }
        return HistoryEvent.newBuilder()
                .setEventId(-1)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setEntityOperationCompleted(builder.build())
                .build();
    }

    private HistoryEvent entityOperationFailedEvent(String requestId, String errorType, String errorMessage) {
        return HistoryEvent.newBuilder()
                .setEventId(-1)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setEntityOperationFailed(EntityOperationFailedEvent.newBuilder()
                        .setRequestId(requestId)
                        .setFailureDetails(TaskFailureDetails.newBuilder()
                                .setErrorType(errorType)
                                .setErrorMessage(errorMessage)
                                .build())
                        .build())
                .build();
    }

    private HistoryEvent entityLockRequestedEvent(int eventId) {
        return HistoryEvent.newBuilder()
                .setEventId(eventId)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setEntityLockRequested(EntityLockRequestedEvent.newBuilder()
                        .setCriticalSectionId("lock-cs-id")
                        .addLockSet("@Counter@c1")
                        .setPosition(0)
                        .setParentInstanceId(StringValue.of("test-instance-id"))
                        .build())
                .build();
    }

    private HistoryEvent entityLockGrantedEvent(String criticalSectionId) {
        return HistoryEvent.newBuilder()
                .setEventId(-1)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setEntityLockGranted(EntityLockGrantedEvent.newBuilder()
                        .setCriticalSectionId(criticalSectionId)
                        .build())
                .build();
    }

    private HistoryEvent entityUnlockSentEvent(int eventId) {
        return HistoryEvent.newBuilder()
                .setEventId(eventId)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setEntityUnlockSent(EntityUnlockSentEvent.newBuilder()
                        .setCriticalSectionId("lock-cs-id")
                        .setParentInstanceId(StringValue.of("test-instance-id"))
                        .setTargetInstanceId(StringValue.of("@Counter@c1"))
                        .build())
                .build();
    }

    private HistoryEvent eventSentEvent(int eventId) {
        return HistoryEvent.newBuilder()
                .setEventId(eventId)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setEventSent(EventSentEvent.newBuilder()
                        .setName("someEvent")
                        .setInstanceId("some-instance")
                        .build())
                .build();
    }

    /**
     * Creates an EventRaised HistoryEvent. This simulates the trigger binding code path
     * where DTFx.Core delivers entity responses as EventRaised events (not proto entity events).
     */
    private HistoryEvent eventRaisedEvent(String name, String input) {
        return HistoryEvent.newBuilder()
                .setEventId(-1)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setEventRaised(EventRaisedEvent.newBuilder()
                        .setName(name)
                        .setInput(StringValue.of(input))
                        .build())
                .build();
    }

    /**
     * Builds a DTFx ResponseMessage JSON for a successful entity operation.
     * Matches the format produced by DTFx.Core's TaskEntityDispatcher:
     * - "result": the serialized operation result
     * - Error fields ("exceptionType", "failureDetails") are omitted on success (EmitDefaultValue=false)
     */
    private String successResponseMessageJson(String result) {
        // DTFx serializes with Newtonsoft.Json; result is always present.
        // On success, exceptionType and failureDetails are omitted.
        if (result == null) {
            return "{\"result\":null}";
        }
        return "{\"result\":" + escapeJsonString(result) + "}";
    }

    /**
     * Builds a DTFx ResponseMessage JSON for a failed entity operation.
     * The C# ResponseMessage property ErrorMessage maps to JSON key "exceptionType"
     * (due to [DataMember(Name = "exceptionType")]). FailureDetails uses PascalCase fields.
     */
    private String failedResponseMessageJson(String errorMessage, String errorType) {
        return "{\"result\":null,"
                + "\"exceptionType\":" + escapeJsonString(errorMessage) + ","
                + "\"failureDetails\":{"
                + "\"ErrorType\":" + escapeJsonString(errorType) + ","
                + "\"ErrorMessage\":" + escapeJsonString(errorMessage)
                + "}}";
    }

    /**
     * Builds a DTFx ResponseMessage JSON for a failed entity operation (exceptionType only, no failureDetails).
     */
    private String failedResponseMessageJsonSimple(String errorMessage) {
        return "{\"result\":null,"
                + "\"exceptionType\":" + escapeJsonString(errorMessage) + "}";
    }

    private String escapeJsonString(String value) {
        if (value == null) return "null";
        // Simple JSON string escaping for test purposes
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    /**
     * Extracts the requestId from a callEntity SendEventAction's JSON payload.
     * The JSON has format: {"op":"...","signal":false,"id":"requestId","parent":"...", ...}
     * Call actions have "signal":false, which distinguishes them from signals ("signal":true).
     */
    private String extractCallRequestId(Collection<OrchestratorAction> actions) throws Exception {
        for (OrchestratorAction action : actions) {
            if (action.hasSendEvent()) {
                SendEventAction sendEvent = action.getSendEvent();
                String data = sendEvent.getData().getValue();
                JsonNode json = JSON_MAPPER.readTree(data);
                if (json.has("id") && json.has("signal") && !json.get("signal").asBoolean()) {
                    return json.get("id").asText();
                }
            }
        }
        return null;
    }

    /**
     * Extracts the criticalSectionId from a lockEntities SendEventAction's JSON payload.
     * The JSON has format: {"op":null,"id":"criticalSectionId","lockset":[...], ...}
     */
    private String extractLockCriticalSectionId(Collection<OrchestratorAction> actions) throws Exception {
        for (OrchestratorAction action : actions) {
            if (action.hasSendEvent()) {
                SendEventAction sendEvent = action.getSendEvent();
                String data = sendEvent.getData().getValue();
                JsonNode json = JSON_MAPPER.readTree(data);
                if (json.has("lockset") && json.has("id")) {
                    return json.get("id").asText();
                }
            }
        }
        return null;
    }

    /**
     * Checks if any action is a SendEventAction targeting the given entity instance.
     */
    private boolean hasEntitySendEvent(Collection<OrchestratorAction> actions, String entityInstanceId) {
        for (OrchestratorAction action : actions) {
            if (action.hasSendEvent()) {
                String target = action.getSendEvent().getInstance().getInstanceId();
                if (target.contains(entityInstanceId)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks if any action is a lock request SendEventAction (JSON with "lockset" field).
     */
    private boolean hasLockRequestAction(Collection<OrchestratorAction> actions) throws Exception {
        for (OrchestratorAction action : actions) {
            if (action.hasSendEvent()) {
                String data = action.getSendEvent().getData().getValue();
                JsonNode json = JSON_MAPPER.readTree(data);
                if (json.has("lockset")) {
                    return true;
                }
            }
        }
        return false;
    }

    // endregion

    // region signalEntity tests

    @Test
    void signalEntity_producesSendEventAction() throws Exception {
        final String orchestratorName = "SignalEntityOrchestration";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            ctx.signalEntity(entityId, "add", 5);
            ctx.complete("done");
        });

        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents, null);

        // Should have two actions: sendEvent (signal) and completeOrchestration
        Collection<OrchestratorAction> actions = result.getActions();
        boolean hasSignal = false;
        boolean hasComplete = false;
        for (OrchestratorAction action : actions) {
            if (action.hasSendEvent()) {
                SendEventAction sendEvent = action.getSendEvent();
                String targetInstanceId = sendEvent.getInstance().getInstanceId();
                if (targetInstanceId.contains("@counter@c1")) {
                    JsonNode json = JSON_MAPPER.readTree(sendEvent.getData().getValue());
                    assertEquals("add", json.get("op").asText());
                    assertTrue(json.get("signal").asBoolean());
                    hasSignal = true;
                }
            }
            if (action.hasCompleteOrchestration()) {
                hasComplete = true;
            }
        }
        assertTrue(hasSignal, "Expected a sendEvent action with signal");
        assertTrue(hasComplete, "Expected a completeOrchestration action");
    }

    @Test
    void getEntities_signalEntity_producesSendEventAction() throws Exception {
        final String orchestratorName = "SignalViaEntitiesFeatureOrchestration";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            ctx.getEntities().signalEntity(entityId, "add", 7);
            ctx.complete("done");
        });

        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents, null);

        boolean hasSignal = false;
        for (OrchestratorAction action : result.getActions()) {
            if (action.hasSendEvent()) {
                SendEventAction sendEvent = action.getSendEvent();
                String targetInstanceId = sendEvent.getInstance().getInstanceId();
                if (targetInstanceId.contains("@counter@c1")) {
                    JsonNode json = JSON_MAPPER.readTree(sendEvent.getData().getValue());
                    assertEquals("add", json.get("op").asText());
                    assertTrue(json.get("signal").asBoolean());
                    hasSignal = true;
                }
            }
        }

        assertTrue(hasSignal, "Expected a sendEvent action with signal");
    }

    @Test
    void signalEntity_replayPassesNonDeterminismCheck() {
        final String orchestratorName = "SignalEntityReplay";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            ctx.signalEntity(entityId, "add", 5);
            ctx.complete("done");
        });

        // First execution produces the signal action (eventId = 0)
        // On replay, the ENTITYOPERATIONSIGNALED event confirms the action
        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"),
                entityOperationSignaledEvent(0),
                orchestratorCompleted());
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        // This should NOT throw a NonDeterministicOrchestratorException
        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents, null);

        // Should still have the complete action
        boolean hasComplete = false;
        for (OrchestratorAction action : result.getActions()) {
            if (action.hasCompleteOrchestration()) {
                hasComplete = true;
            }
        }
        assertTrue(hasComplete);
    }

    // endregion

    // region callEntity tests

    @Test
    void callEntity_producesActionAndWaitsForResponse() throws Exception {
        final String orchestratorName = "CallEntityOrchestration";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            int value = ctx.callEntity(entityId, "get", null, int.class).await();
            ctx.complete(value);
        });

        // First execution: produces the call action but blocks because no response yet
        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents, null);

        // Should have the sendEvent (call) action
        boolean hasCall = false;
        for (OrchestratorAction action : result.getActions()) {
            if (action.hasSendEvent()) {
                SendEventAction sendEvent = action.getSendEvent();
                String targetInstanceId = sendEvent.getInstance().getInstanceId();
                if (targetInstanceId.contains("@counter@c1")) {
                    JsonNode json = JSON_MAPPER.readTree(sendEvent.getData().getValue());
                    assertEquals("get", json.get("op").asText());
                    assertFalse(json.get("signal").asBoolean(), "Call action should have signal=false");
                    hasCall = true;
                }
            }
        }
        assertTrue(hasCall, "Expected a sendEvent action with call");

        // Should NOT have a complete action (it's waiting for the response)
        boolean hasComplete = false;
        for (OrchestratorAction action : result.getActions()) {
            if (action.hasCompleteOrchestration()) {
                hasComplete = true;
            }
        }
        assertFalse(hasComplete, "Should not complete while waiting for entity response");
    }

    @Test
    void entities_callEntity_producesActionAndWaitsForResponse() throws Exception {
        final String orchestratorName = "CallViaEntitiesFeatureOrchestration";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            int value = ctx.entities().callEntity(entityId, "get", null, int.class).await();
            ctx.complete(value);
        });

        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents, null);

        boolean hasCall = false;
        boolean hasComplete = false;
        for (OrchestratorAction action : result.getActions()) {
            if (action.hasSendEvent()) {
                SendEventAction sendEvent = action.getSendEvent();
                String targetInstanceId = sendEvent.getInstance().getInstanceId();
                if (targetInstanceId.contains("@counter@c1")) {
                    JsonNode json = JSON_MAPPER.readTree(sendEvent.getData().getValue());
                    assertEquals("get", json.get("op").asText());
                    hasCall = true;
                }
            }
            if (action.hasCompleteOrchestration()) {
                hasComplete = true;
            }
        }

        assertTrue(hasCall, "Expected a sendEvent action with call");
        assertFalse(hasComplete, "Should not complete while waiting for entity response");
    }

    @Test
    void callEntity_completesWhenResponseArrives() throws Exception {
        final String orchestratorName = "CallEntityComplete";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        // We need to figure out the requestId that the executor generates.
        // The executor uses newUUID() which is deterministic based on instanceId + timestamp + counter.
        // For this test we need to get the requestId from the first execution.

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            int value = ctx.callEntity(entityId, "get", null, int.class).await();
            ctx.complete(value);
        });

        // First pass: execute and capture the requestId from the generated action
        List<HistoryEvent> pastEvents1 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents1 = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result1 = executor.execute(pastEvents1, newEvents1, null);

        // Extract the requestId from the call action's JSON payload
        String requestId = null;
        for (OrchestratorAction action : result1.getActions()) {
            if (action.hasSendEvent()) {
                SendEventAction sendEvent = action.getSendEvent();
                if (sendEvent.getInstance().getInstanceId().contains("@counter@c1")) {
                    JsonNode json = JSON_MAPPER.readTree(sendEvent.getData().getValue());
                    if (json.has("id") && json.has("signal") && !json.get("signal").asBoolean()) {
                        requestId = json.get("id").asText();
                    }
                }
            }
        }
        assertNotNull(requestId, "Should have captured the requestId");

        // Second pass (replay): include the sent event in past and provide the response
        executor = createExecutor(orchestratorName, ctx -> {
            int value = ctx.callEntity(entityId, "get", null, int.class).await();
            ctx.complete(value);
        });

        List<HistoryEvent> pastEvents2 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"),
                eventSentEvent(0),
                orchestratorCompleted());
        List<HistoryEvent> newEvents2 = Arrays.asList(
                orchestratorStarted(),
                entityOperationCompletedEvent(requestId, "42"),
                orchestratorCompleted());

        TaskOrchestratorResult result2 = executor.execute(pastEvents2, newEvents2, null);

        // Should now have a complete action with value 42
        boolean hasComplete = false;
        for (OrchestratorAction action : result2.getActions()) {
            if (action.hasCompleteOrchestration()) {
                CompleteOrchestrationAction complete = action.getCompleteOrchestration();
                assertEquals(OrchestrationStatus.ORCHESTRATION_STATUS_COMPLETED, complete.getOrchestrationStatus());
                assertEquals("42", complete.getResult().getValue());
                hasComplete = true;
            }
        }
        assertTrue(hasComplete, "Expected orchestration to complete with entity result");
    }

    @Test
    void callEntity_failedResponse_completesExceptionally() throws Exception {
        final String orchestratorName = "CallEntityFail";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            try {
                ctx.callEntity(entityId, "get", null, int.class).await();
                ctx.complete("should not reach here");
            } catch (EntityOperationFailedException e) {
                ctx.complete("caught: " + e.getFailureDetails().getErrorMessage());
            }
        });

        // First pass: capture requestId
        List<HistoryEvent> pastEvents1 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents1 = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result1 = executor.execute(pastEvents1, newEvents1, null);

        String requestId = null;
        for (OrchestratorAction action : result1.getActions()) {
            if (action.hasSendEvent()) {
                SendEventAction sendEvent = action.getSendEvent();
                if (sendEvent.getInstance().getInstanceId().contains("@counter@c1")) {
                    JsonNode json = JSON_MAPPER.readTree(sendEvent.getData().getValue());
                    if (json.has("id") && json.has("signal") && !json.get("signal").asBoolean()) {
                        requestId = json.get("id").asText();
                    }
                }
            }
        }
        assertNotNull(requestId);

        // Second pass: replay with failed response
        executor = createExecutor(orchestratorName, ctx -> {
            try {
                ctx.callEntity(entityId, "get", null, int.class).await();
                ctx.complete("should not reach here");
            } catch (EntityOperationFailedException e) {
                ctx.complete("caught: " + e.getFailureDetails().getErrorMessage());
            }
        });

        List<HistoryEvent> pastEvents2 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"),
                eventSentEvent(0),
                orchestratorCompleted());
        List<HistoryEvent> newEvents2 = Arrays.asList(
                orchestratorStarted(),
                entityOperationFailedEvent(requestId, "java.lang.RuntimeException", "Entity error!"),
                orchestratorCompleted());

        TaskOrchestratorResult result2 = executor.execute(pastEvents2, newEvents2, null);

        boolean hasComplete = false;
        for (OrchestratorAction action : result2.getActions()) {
            if (action.hasCompleteOrchestration()) {
                CompleteOrchestrationAction complete = action.getCompleteOrchestration();
                assertEquals(OrchestrationStatus.ORCHESTRATION_STATUS_COMPLETED, complete.getOrchestrationStatus());
                assertTrue(complete.getResult().getValue().contains("Entity error!"));
                hasComplete = true;
            }
        }
        assertTrue(hasComplete, "Expected orchestration to complete after catching entity failure");
    }

    // endregion

    // region callEntity via EventRaised (trigger binding path) tests
    //
    // These tests simulate the Azure Functions trigger binding code path where entity
    // operation responses arrive as EventRaised events containing DTFx ResponseMessage JSON,
    // rather than proto EntityOperationCompleted/Failed events (gRPC path).
    //
    // In the trigger binding path:
    //   - Past events: EVENTSENT (no-op) instead of ENTITYOPERATIONCALLED
    //   - New events: EVENTRAISED with ResponseMessage JSON instead of ENTITYOPERATIONCOMPLETED

    @Test
    void callEntity_completesViaEventRaised_triggerBindingPath() throws Exception {
        final String orchestratorName = "CallEntityTriggerPath";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            int value = ctx.callEntity(entityId, "get", null, int.class).await();
            ctx.complete(value);
        });

        // First pass: capture the requestId from the generated action
        List<HistoryEvent> pastEvents1 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents1 = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result1 = executor.execute(pastEvents1, newEvents1, null);

        String requestId = extractCallRequestId(result1.getActions());
        assertNotNull(requestId, "Should have captured the requestId");

        // Second pass (replay): use EVENTSENT in past (trigger binding records EventSent, not EntityOperationCalled)
        // and EVENTRAISED with ResponseMessage JSON in new events (not EntityOperationCompleted)
        executor = createExecutor(orchestratorName, ctx -> {
            int value = ctx.callEntity(entityId, "get", null, int.class).await();
            ctx.complete(value);
        });

        String responseJson = successResponseMessageJson("42");
        List<HistoryEvent> pastEvents2 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"),
                eventSentEvent(0),  // Trigger binding records EventSent, not EntityOperationCalled
                orchestratorCompleted());
        List<HistoryEvent> newEvents2 = Arrays.asList(
                orchestratorStarted(),
                eventRaisedEvent(requestId, responseJson),  // ResponseMessage JSON wrapper
                orchestratorCompleted());

        TaskOrchestratorResult result2 = executor.execute(pastEvents2, newEvents2, null);

        boolean hasComplete = false;
        for (OrchestratorAction action : result2.getActions()) {
            if (action.hasCompleteOrchestration()) {
                CompleteOrchestrationAction complete = action.getCompleteOrchestration();
                assertEquals(OrchestrationStatus.ORCHESTRATION_STATUS_COMPLETED, complete.getOrchestrationStatus());
                assertEquals("42", complete.getResult().getValue());
                hasComplete = true;
            }
        }
        assertTrue(hasComplete, "Expected orchestration to complete with entity result via EventRaised");
    }

    @Test
    void callEntity_stringResultViaEventRaised_triggerBindingPath() throws Exception {
        final String orchestratorName = "CallEntityTriggerPathString";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            String value = ctx.callEntity(entityId, "getName", null, String.class).await();
            ctx.complete(value);
        });

        // First pass: capture requestId
        List<HistoryEvent> pastEvents1 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents1 = Collections.singletonList(orchestratorCompleted());
        TaskOrchestratorResult result1 = executor.execute(pastEvents1, newEvents1, null);

        String requestId = extractCallRequestId(result1.getActions());
        assertNotNull(requestId);

        // Second pass: replay with EventRaised containing a JSON-serialized string result
        executor = createExecutor(orchestratorName, ctx -> {
            String value = ctx.callEntity(entityId, "getName", null, String.class).await();
            ctx.complete(value);
        });

        // DTFx serializes string results as JSON strings: "\"hello\""
        String responseJson = successResponseMessageJson("\"hello\"");
        List<HistoryEvent> pastEvents2 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"),
                eventSentEvent(0),
                orchestratorCompleted());
        List<HistoryEvent> newEvents2 = Arrays.asList(
                orchestratorStarted(),
                eventRaisedEvent(requestId, responseJson),
                orchestratorCompleted());

        TaskOrchestratorResult result2 = executor.execute(pastEvents2, newEvents2, null);

        boolean hasComplete = false;
        for (OrchestratorAction action : result2.getActions()) {
            if (action.hasCompleteOrchestration()) {
                CompleteOrchestrationAction complete = action.getCompleteOrchestration();
                assertEquals(OrchestrationStatus.ORCHESTRATION_STATUS_COMPLETED, complete.getOrchestrationStatus());
                assertEquals("\"hello\"", complete.getResult().getValue());
                hasComplete = true;
            }
        }
        assertTrue(hasComplete, "Expected orchestration to complete with string entity result via EventRaised");
    }

    @Test
    void callEntity_nullResultViaEventRaised_triggerBindingPath() throws Exception {
        final String orchestratorName = "CallEntityTriggerPathNull";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            // Void operation — result is null
            ctx.callEntity(entityId, "reset", null, Void.class).await();
            ctx.complete("done");
        });

        // First pass: capture requestId
        List<HistoryEvent> pastEvents1 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents1 = Collections.singletonList(orchestratorCompleted());
        TaskOrchestratorResult result1 = executor.execute(pastEvents1, newEvents1, null);

        String requestId = extractCallRequestId(result1.getActions());
        assertNotNull(requestId);

        // Second pass: replay with null result in ResponseMessage
        executor = createExecutor(orchestratorName, ctx -> {
            ctx.callEntity(entityId, "reset", null, Void.class).await();
            ctx.complete("done");
        });

        String responseJson = successResponseMessageJson(null);
        List<HistoryEvent> pastEvents2 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"),
                eventSentEvent(0),
                orchestratorCompleted());
        List<HistoryEvent> newEvents2 = Arrays.asList(
                orchestratorStarted(),
                eventRaisedEvent(requestId, responseJson),
                orchestratorCompleted());

        TaskOrchestratorResult result2 = executor.execute(pastEvents2, newEvents2, null);

        boolean hasComplete = false;
        for (OrchestratorAction action : result2.getActions()) {
            if (action.hasCompleteOrchestration()) {
                CompleteOrchestrationAction complete = action.getCompleteOrchestration();
                assertEquals(OrchestrationStatus.ORCHESTRATION_STATUS_COMPLETED, complete.getOrchestrationStatus());
                assertEquals("\"done\"", complete.getResult().getValue());
                hasComplete = true;
            }
        }
        assertTrue(hasComplete, "Expected orchestration to complete after void entity call via EventRaised");
    }

    @Test
    void callEntity_failedViaEventRaised_withFailureDetails_triggerBindingPath() throws Exception {
        final String orchestratorName = "CallEntityTriggerPathFail";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            try {
                ctx.callEntity(entityId, "get", null, int.class).await();
                ctx.complete("should not reach here");
            } catch (EntityOperationFailedException e) {
                ctx.complete("caught: " + e.getFailureDetails().getErrorMessage());
            }
        });

        // First pass: capture requestId
        List<HistoryEvent> pastEvents1 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents1 = Collections.singletonList(orchestratorCompleted());
        TaskOrchestratorResult result1 = executor.execute(pastEvents1, newEvents1, null);

        String requestId = extractCallRequestId(result1.getActions());
        assertNotNull(requestId);

        // Second pass: replay with failed ResponseMessage via EventRaised
        executor = createExecutor(orchestratorName, ctx -> {
            try {
                ctx.callEntity(entityId, "get", null, int.class).await();
                ctx.complete("should not reach here");
            } catch (EntityOperationFailedException e) {
                ctx.complete("caught: " + e.getFailureDetails().getErrorMessage());
            }
        });

        String responseJson = failedResponseMessageJson("Entity error!", "java.lang.RuntimeException");
        List<HistoryEvent> pastEvents2 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"),
                eventSentEvent(0),
                orchestratorCompleted());
        List<HistoryEvent> newEvents2 = Arrays.asList(
                orchestratorStarted(),
                eventRaisedEvent(requestId, responseJson),
                orchestratorCompleted());

        TaskOrchestratorResult result2 = executor.execute(pastEvents2, newEvents2, null);

        boolean hasComplete = false;
        for (OrchestratorAction action : result2.getActions()) {
            if (action.hasCompleteOrchestration()) {
                CompleteOrchestrationAction complete = action.getCompleteOrchestration();
                assertEquals(OrchestrationStatus.ORCHESTRATION_STATUS_COMPLETED, complete.getOrchestrationStatus());
                assertTrue(complete.getResult().getValue().contains("Entity error!"),
                        "Expected result to contain the error message");
                hasComplete = true;
            }
        }
        assertTrue(hasComplete, "Expected orchestration to complete after catching entity failure via EventRaised");
    }

    @Test
    void callEntity_failedViaEventRaised_simpleError_triggerBindingPath() throws Exception {
        final String orchestratorName = "CallEntityTriggerPathFailSimple";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            try {
                ctx.callEntity(entityId, "get", null, int.class).await();
                ctx.complete("should not reach here");
            } catch (EntityOperationFailedException e) {
                ctx.complete("caught: " + e.getFailureDetails().getErrorMessage());
            }
        });

        // First pass: capture requestId
        List<HistoryEvent> pastEvents1 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents1 = Collections.singletonList(orchestratorCompleted());
        TaskOrchestratorResult result1 = executor.execute(pastEvents1, newEvents1, null);

        String requestId = extractCallRequestId(result1.getActions());
        assertNotNull(requestId);

        // Second pass: replay with simple error (exceptionType only, no failureDetails)
        executor = createExecutor(orchestratorName, ctx -> {
            try {
                ctx.callEntity(entityId, "get", null, int.class).await();
                ctx.complete("should not reach here");
            } catch (EntityOperationFailedException e) {
                ctx.complete("caught: " + e.getFailureDetails().getErrorMessage());
            }
        });

        String responseJson = failedResponseMessageJsonSimple("Simple error occurred");
        List<HistoryEvent> pastEvents2 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"),
                eventSentEvent(0),
                orchestratorCompleted());
        List<HistoryEvent> newEvents2 = Arrays.asList(
                orchestratorStarted(),
                eventRaisedEvent(requestId, responseJson),
                orchestratorCompleted());

        TaskOrchestratorResult result2 = executor.execute(pastEvents2, newEvents2, null);

        boolean hasComplete = false;
        for (OrchestratorAction action : result2.getActions()) {
            if (action.hasCompleteOrchestration()) {
                CompleteOrchestrationAction complete = action.getCompleteOrchestration();
                assertEquals(OrchestrationStatus.ORCHESTRATION_STATUS_COMPLETED, complete.getOrchestrationStatus());
                assertTrue(complete.getResult().getValue().contains("Simple error occurred"),
                        "Expected result to contain the simple error message");
                hasComplete = true;
            }
        }
        assertTrue(hasComplete, "Expected orchestration to complete after catching simple entity failure via EventRaised");
    }

    // endregion

    // region EVENTSENT no-op test

    @Test
    void eventSent_doesNotCrash() {
        final String orchestratorName = "EventSentOrchestration";

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            ctx.sendEvent("some-instance", "someEvent", "data");
            ctx.complete("done");
        });

        // Include EVENTSENT in past events — should be handled as a no-op
        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"),
                eventSentEvent(0),
                orchestratorCompleted());
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        // This should NOT throw (EVENTSENT is a no-op)
        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents, null);

        boolean hasComplete = false;
        for (OrchestratorAction action : result.getActions()) {
            if (action.hasCompleteOrchestration()) {
                hasComplete = true;
            }
        }
        assertTrue(hasComplete, "Orchestration should still complete with EVENTSENT in history");
    }

    // endregion

    // region Non-determinism tests

    @Test
    void entityOperationSignaled_nonDeterminism_throwsException() {
        final String orchestratorName = "NonDetSignal";

        // Orchestrator that does NOT signal any entity
        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            ctx.complete("done");
        });

        // But history has an ENTITYOPERATIONSIGNALED event
        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"),
                entityOperationSignaledEvent(0),
                orchestratorCompleted());
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        // The orchestrator already completed, so when the non-determinism is detected,
        // context.fail() throws IllegalStateException ("already completed")
        assertThrows(IllegalStateException.class, () ->
                executor.execute(pastEvents, newEvents, null));
    }

    @Test
    void entityOperationCalled_nonDeterminism_throwsException() {
        final String orchestratorName = "NonDetCall";

        // Orchestrator that does NOT call any entity
        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            ctx.complete("done");
        });

        // But history has an ENTITYOPERATIONCALLED event
        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"),
                entityOperationCalledEvent(0),
                orchestratorCompleted());
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        assertThrows(IllegalStateException.class, () ->
                executor.execute(pastEvents, newEvents, null));
    }

    @Test
    void entityLockRequested_nonDeterminism_throwsException() {
        final String orchestratorName = "NonDetLock";

        // Orchestrator that does NOT lock any entities
        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            ctx.complete("done");
        });

        // But history has an ENTITYLOCKREQUESTED event
        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"),
                entityLockRequestedEvent(0),
                orchestratorCompleted());
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        assertThrows(IllegalStateException.class, () ->
                executor.execute(pastEvents, newEvents, null));
    }

    @Test
    void entityUnlockSent_nonDeterminism_throwsException() {
        final String orchestratorName = "NonDetUnlock";

        // Orchestrator that does NOT unlock any entities
        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            ctx.complete("done");
        });

        // But history has an ENTITYUNLOCKSENT event
        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"),
                entityUnlockSentEvent(0),
                orchestratorCompleted());
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        assertThrows(IllegalStateException.class, () ->
                executor.execute(pastEvents, newEvents, null));
    }

    // endregion

    // region SignalEntityOptions / CallEntityOptions / getLockedEntities / varargs lockEntities tests

    @Test
    void signalEntity_withScheduledTime_setsScheduledTimeOnAction() throws Exception {
        final String orchestratorName = "SignalScheduledTimeTest";
        Instant scheduledTime = Instant.parse("2025-06-15T12:00:00Z");
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            SignalEntityOptions options = new SignalEntityOptions().setScheduledTime(scheduledTime);
            ctx.signalEntity(entityId, "add", 5, options);
            ctx.complete("done");
        });

        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents, null);

        boolean hasScheduledSignal = false;
        for (OrchestratorAction action : result.getActions()) {
            if (action.hasSendEvent()) {
                SendEventAction sendEvent = action.getSendEvent();
                if (sendEvent.getInstance().getInstanceId().contains("@counter@c1")) {
                    JsonNode json = JSON_MAPPER.readTree(sendEvent.getData().getValue());
                    if (json.has("signal") && json.get("signal").asBoolean()) {
                        assertTrue(json.has("due"), "Expected 'due' field for scheduled signal");
                        // The event name should include the scheduled time
                        assertTrue(sendEvent.getName().contains("op@"), "Expected event name to include scheduled time");
                        hasScheduledSignal = true;
                    }
                }
            }
        }
        assertTrue(hasScheduledSignal, "Expected a signal action with scheduledTime");
    }

    @Test
    void signalEntity_withNullOptions_noScheduledTime() throws Exception {
        final String orchestratorName = "SignalNullOptionsTest";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            ctx.signalEntity(entityId, "add", 5, null);
            ctx.complete("done");
        });

        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents, null);

        for (OrchestratorAction action : result.getActions()) {
            if (action.hasSendEvent()) {
                SendEventAction sendEvent = action.getSendEvent();
                if (sendEvent.getInstance().getInstanceId().contains("@counter@c1")) {
                    JsonNode json = JSON_MAPPER.readTree(sendEvent.getData().getValue());
                    if (json.has("signal") && json.get("signal").asBoolean()) {
                        assertFalse(json.has("due"), "Expected no 'due' field when options are null");
                    }
                }
            }
        }
    }

    @Test
    void callEntity_withOptions_producesAction() throws Exception {
        final String orchestratorName = "CallWithOptionsTest";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            CallEntityOptions options = new CallEntityOptions().setTimeout(Duration.ofSeconds(30));
            int value = ctx.callEntity(entityId, "get", null, int.class, options).await();
            ctx.complete(value);
        });

        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents, null);

        boolean hasCall = false;
        for (OrchestratorAction action : result.getActions()) {
            if (action.hasSendEvent()) {
                SendEventAction sendEvent = action.getSendEvent();
                if (sendEvent.getInstance().getInstanceId().contains("@counter@c1")) {
                    JsonNode json = JSON_MAPPER.readTree(sendEvent.getData().getValue());
                    assertEquals("get", json.get("op").asText());
                    hasCall = true;
                }
            }
        }
        assertTrue(hasCall, "Expected a sendEvent action with call");
    }

    @Test
    void callEntity_withTimeout_producesCallAndTimerActions() {
        final String orchestratorName = "CallWithTimeoutTest";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            CallEntityOptions options = new CallEntityOptions().setTimeout(Duration.ofSeconds(30));
            ctx.callEntity(entityId, "get", null, int.class, options).await();
            ctx.complete("done");
        });

        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents, null);

        boolean hasCall = false;
        boolean hasTimer = false;
        for (OrchestratorAction action : result.getActions()) {
            if (action.hasSendEvent() && action.getSendEvent().getInstance().getInstanceId().contains("@counter@c1")) {
                hasCall = true;
            }
            if (action.hasCreateTimer()) {
                hasTimer = true;
            }
        }
        assertTrue(hasCall, "Expected a sendEvent action with call");
        assertTrue(hasTimer, "Expected a createTimer action for the timeout");
    }

    @Test
    void callEntity_withoutTimeout_producesNoTimerAction() {
        final String orchestratorName = "CallNoTimeoutTest";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            // No options = no timeout
            ctx.callEntity(entityId, "get", null, int.class).await();
            ctx.complete("done");
        });

        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents, null);

        boolean hasCall = false;
        boolean hasTimer = false;
        for (OrchestratorAction action : result.getActions()) {
            if (action.hasSendEvent() && action.getSendEvent().getInstance().getInstanceId().contains("@counter@c1")) {
                hasCall = true;
            }
            if (action.hasCreateTimer()) {
                hasTimer = true;
            }
        }
        assertTrue(hasCall, "Expected a sendEvent action with call");
        assertFalse(hasTimer, "Expected no createTimer action when no timeout is specified");
    }

    @Test
    void callEntity_withZeroTimeout_cancelledImmediately() {
        final String orchestratorName = "CallZeroTimeoutTest";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            CallEntityOptions options = new CallEntityOptions().setTimeout(Duration.ZERO);
            try {
                ctx.callEntity(entityId, "get", null, int.class, options).await();
                ctx.complete("should not reach here");
            } catch (TaskCanceledException e) {
                ctx.complete("cancelled");
            }
        });

        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents, null);

        boolean hasComplete = false;
        for (OrchestratorAction action : result.getActions()) {
            if (action.hasCompleteOrchestration()) {
                String output = action.getCompleteOrchestration().getResult().getValue();
                assertEquals("\"cancelled\"", output);
                hasComplete = true;
            }
        }
        assertTrue(hasComplete, "Expected orchestration to complete with 'cancelled' after zero timeout");
    }

    @Test
    void getLockedEntities_insideCriticalSection_returnsLockedIds() {
        final String orchestratorName = "GetLockedEntitiesTest";
        EntityInstanceId entityId = new EntityInstanceId("Counter", "c1");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            ctx.lockEntities(Arrays.asList(entityId)).await();
            // Inside critical section, getLockedEntities should return the locked entities
            List<EntityInstanceId> locked = ctx.getLockedEntities();
            assertFalse(locked.isEmpty(), "Expected locked entities inside critical section");
            assertEquals("counter", locked.get(0).getName());
            assertEquals("c1", locked.get(0).getKey());
            ctx.complete("done");
        });

        // First execution: orchestrator calls lockEntities, which produces a lock request action
        List<HistoryEvent> pastEvents1 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents1 = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result1 = executor.execute(pastEvents1, newEvents1, null);

        // Extract the criticalSectionId from the lock request action
        String criticalSectionId = null;
        try {
            criticalSectionId = extractLockCriticalSectionId(result1.getActions());
        } catch (Exception e) {
            fail("Failed to extract criticalSectionId: " + e.getMessage());
        }
        assertNotNull(criticalSectionId, "Expected a lock request action with criticalSectionId");

        // Second execution: replay with the lock request in past, grant in new events
        List<HistoryEvent> pastEvents2 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"),
                eventSentEvent(0),
                orchestratorCompleted());
        List<HistoryEvent> newEvents2 = Arrays.asList(
                orchestratorStarted(),
                entityLockGrantedEvent(criticalSectionId),
                orchestratorCompleted());

        TaskOrchestratorResult result2 = executor.execute(pastEvents2, newEvents2, null);

        boolean hasComplete = false;
        for (OrchestratorAction action : result2.getActions()) {
            if (action.hasCompleteOrchestration()) {
                hasComplete = true;
            }
        }
        assertTrue(hasComplete, "Expected orchestration to complete after lock granted");
    }

    @Test
    void getLockedEntities_outsideCriticalSection_returnsEmpty() {
        final String orchestratorName = "GetLockedEntitiesEmptyTest";

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            List<EntityInstanceId> locked = ctx.getLockedEntities();
            assertTrue(locked.isEmpty(), "Expected empty locked entities outside critical section");
            ctx.complete("done");
        });

        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents, null);

        boolean hasComplete = false;
        for (OrchestratorAction action : result.getActions()) {
            if (action.hasCompleteOrchestration()) {
                hasComplete = true;
            }
        }
        assertTrue(hasComplete, "Expected orchestration to complete");
    }

    @Test
    void lockEntities_varargs_producesLockAction() {
        final String orchestratorName = "VarargsLockTest";
        EntityInstanceId entityId1 = new EntityInstanceId("Counter", "c1");
        EntityInstanceId entityId2 = new EntityInstanceId("Counter", "c2");

        TaskOrchestrationExecutor executor = createExecutor(orchestratorName, ctx -> {
            // Use varargs overload
            ctx.lockEntities(entityId1, entityId2).await();
            ctx.complete("locked");
        });

        List<HistoryEvent> pastEvents = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"));
        List<HistoryEvent> newEvents = Collections.singletonList(orchestratorCompleted());

        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents, null);

        boolean hasLockRequest = false;
        try {
            hasLockRequest = hasLockRequestAction(result.getActions());
        } catch (Exception e) {
            fail("Failed to check lock request: " + e.getMessage());
        }
        assertTrue(hasLockRequest, "Expected a lock request action from varargs lockEntities");
    }

    // endregion

    // region DurableTaskGrpcWorkerBuilder entity config tests

    @Test
    void maxConcurrentEntityWorkItems_rejectsZero() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.maxConcurrentEntityWorkItems(0));
    }

    @Test
    void maxConcurrentEntityWorkItems_rejectsNegative() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.maxConcurrentEntityWorkItems(-1));
    }

    @Test
    void maxConcurrentEntityWorkItems_acceptsValidValue() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        DurableTaskGrpcWorkerBuilder result = builder.maxConcurrentEntityWorkItems(4);
        assertSame(builder, result, "Builder should return itself for fluent chaining");
    }

    @Test
    void maxConcurrentEntityWorkItems_defaultIsOne() {
        DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder();
        assertEquals(1, builder.maxConcurrentEntityWorkItems,
                "Default maxConcurrentEntityWorkItems should be 1");
    }

    // endregion
}
