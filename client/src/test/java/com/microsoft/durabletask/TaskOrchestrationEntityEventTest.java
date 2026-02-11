// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
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

    // endregion

    // region signalEntity tests

    @Test
    void signalEntity_producesSendEntityMessageAction() {
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

        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents);

        // Should have two actions: sendEntityMessage (signal) and completeOrchestration
        Collection<OrchestratorAction> actions = result.getActions();
        boolean hasSignal = false;
        boolean hasComplete = false;
        for (OrchestratorAction action : actions) {
            if (action.hasSendEntityMessage()) {
                SendEntityMessageAction msg = action.getSendEntityMessage();
                assertTrue(msg.hasEntityOperationSignaled());
                EntityOperationSignaledEvent signal = msg.getEntityOperationSignaled();
                assertEquals("add", signal.getOperation());
                assertEquals("@Counter@c1", signal.getTargetInstanceId().getValue());
                hasSignal = true;
            }
            if (action.hasCompleteOrchestration()) {
                hasComplete = true;
            }
        }
        assertTrue(hasSignal, "Expected a sendEntityMessage action with signal");
        assertTrue(hasComplete, "Expected a completeOrchestration action");
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
        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents);

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
    void callEntity_producesActionAndWaitsForResponse() {
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

        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents);

        // Should have the sendEntityMessage (call) action
        boolean hasCall = false;
        for (OrchestratorAction action : result.getActions()) {
            if (action.hasSendEntityMessage()) {
                SendEntityMessageAction msg = action.getSendEntityMessage();
                assertTrue(msg.hasEntityOperationCalled());
                EntityOperationCalledEvent call = msg.getEntityOperationCalled();
                assertEquals("get", call.getOperation());
                assertEquals("@Counter@c1", call.getTargetInstanceId().getValue());
                hasCall = true;
            }
        }
        assertTrue(hasCall, "Expected a sendEntityMessage action with call");

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
    void callEntity_completesWhenResponseArrives() {
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

        TaskOrchestratorResult result1 = executor.execute(pastEvents1, newEvents1);

        // Extract the requestId from the call action
        String requestId = null;
        for (OrchestratorAction action : result1.getActions()) {
            if (action.hasSendEntityMessage() && action.getSendEntityMessage().hasEntityOperationCalled()) {
                requestId = action.getSendEntityMessage().getEntityOperationCalled().getRequestId();
            }
        }
        assertNotNull(requestId, "Should have captured the requestId");

        // Second pass (replay): include the call event in past and provide the response
        executor = createExecutor(orchestratorName, ctx -> {
            int value = ctx.callEntity(entityId, "get", null, int.class).await();
            ctx.complete(value);
        });

        List<HistoryEvent> pastEvents2 = Arrays.asList(
                orchestratorStarted(),
                executionStarted(orchestratorName, "null"),
                entityOperationCalledEvent(0),
                orchestratorCompleted());
        List<HistoryEvent> newEvents2 = Arrays.asList(
                orchestratorStarted(),
                entityOperationCompletedEvent(requestId, "42"),
                orchestratorCompleted());

        TaskOrchestratorResult result2 = executor.execute(pastEvents2, newEvents2);

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
    void callEntity_failedResponse_completesExceptionally() {
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

        TaskOrchestratorResult result1 = executor.execute(pastEvents1, newEvents1);

        String requestId = null;
        for (OrchestratorAction action : result1.getActions()) {
            if (action.hasSendEntityMessage() && action.getSendEntityMessage().hasEntityOperationCalled()) {
                requestId = action.getSendEntityMessage().getEntityOperationCalled().getRequestId();
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
                entityOperationCalledEvent(0),
                orchestratorCompleted());
        List<HistoryEvent> newEvents2 = Arrays.asList(
                orchestratorStarted(),
                entityOperationFailedEvent(requestId, "java.lang.RuntimeException", "Entity error!"),
                orchestratorCompleted());

        TaskOrchestratorResult result2 = executor.execute(pastEvents2, newEvents2);

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
        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents);

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
                executor.execute(pastEvents, newEvents));
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
                executor.execute(pastEvents, newEvents));
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
                executor.execute(pastEvents, newEvents));
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
                executor.execute(pastEvents, newEvents));
    }

    // endregion
}
