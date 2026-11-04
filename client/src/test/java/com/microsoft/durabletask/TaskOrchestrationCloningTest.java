// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.*;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

class TaskOrchestrationCloningTest {
    private static final String SOURCE_TAG = "MS_DurableFunctions_SourceInstanceId";

    private TaskOrchestrationExecutor executor(TaskOrchestration orchestration) {
        HashMap<String, TaskOrchestrationFactory> factories = new HashMap<>();
        factories.put("Order", new TaskOrchestrationFactory() {
            @Override
            public String getName() {
                return "Order";
            }

            @Override
            public TaskOrchestration create() {
                return orchestration;
            }
        });
        return new TaskOrchestrationExecutor(factories, new JacksonDataConverter(), Duration.ofDays(3),
                Logger.getLogger(TaskOrchestrationCloningTest.class.getName()), null);
    }

    private List<HistoryEvent> startEvents(String sourceId, boolean child) {
        ExecutionStartedEvent.Builder started = ExecutionStartedEvent.newBuilder()
                .setName("Order")
                .setInput(StringValue.of("\"original-input\""))
                .setVersion(StringValue.of("2.0"))
                .setOrchestrationInstance(OrchestrationInstance.newBuilder().setInstanceId("order-retry"));
        if (sourceId != null) {
            started.putTags(SOURCE_TAG, sourceId);
        }
        if (child) {
            started.setParentInstance(ParentInstanceInfo.newBuilder()
                    .setOrchestrationInstance(OrchestrationInstance.newBuilder().setInstanceId("parent-clone")));
        }
        return Arrays.asList(
                HistoryEvent.newBuilder().setEventId(-1).setTimestamp(Timestamp.getDefaultInstance())
                        .setOrchestratorStarted(OrchestratorStartedEvent.getDefaultInstance()).build(),
                HistoryEvent.newBuilder().setEventId(-1).setTimestamp(Timestamp.getDefaultInstance())
                        .setExecutionStarted(started).build());
    }

    @ParameterizedTest
    @ValueSource(strings = {"order-123", "order-123-retry"})
    void immediateSourceIsStableAcrossReplayAndChildCompletion(String sourceId) {
        List<String> sources = new ArrayList<>();
        List<Boolean> replayStates = new ArrayList<>();
        List<String> details = new ArrayList<>();
        TaskOrchestrationExecutor executor = executor(ctx -> {
            sources.add(ctx.getSourceInstanceId());
            replayStates.add(ctx.getIsReplaying());
            details.add(ctx.getInstanceId());
            details.add(ctx.getVersion());
            details.add(ctx.getInput(String.class));
            ctx.callSubOrchestrator("Child", null, String.class).await();
            sources.add(ctx.getSourceInstanceId());
            replayStates.add(ctx.getIsReplaying());
            ctx.complete("done");
        });
        List<HistoryEvent> history = new ArrayList<>(startEvents(sourceId, false));
        TaskOrchestratorResult initial = executor.execute(Collections.emptyList(), history, null);
        OrchestratorAction action = initial.getActions().iterator().next();
        assertTrue(action.hasCreateSubOrchestration());
        assertFalse(action.getCreateSubOrchestration().getTagsMap().containsKey(SOURCE_TAG));
        assertEquals(Collections.singletonList(sourceId), sources);
        assertEquals(Collections.singletonList(false), replayStates);

        history.add(HistoryEvent.newBuilder().setEventId(action.getId())
                .setSubOrchestrationInstanceCreated(SubOrchestrationInstanceCreatedEvent.newBuilder()
                        .setName("Child")
                        .setInstanceId(action.getCreateSubOrchestration().getInstanceId())).build());
        List<HistoryEvent> newEvents = Collections.singletonList(HistoryEvent.newBuilder().setEventId(-1)
                .setSubOrchestrationInstanceCompleted(SubOrchestrationInstanceCompletedEvent.newBuilder()
                        .setTaskScheduledId(action.getId()).setResult(StringValue.of("\"child-result\""))).build());
        TaskOrchestratorResult replay = executor.execute(history, newEvents, null);
        assertEquals(Arrays.asList(sourceId, sourceId, sourceId), sources);
        assertEquals(Arrays.asList(false, true, false), replayStates);
        assertEquals(Arrays.asList("order-retry", "2.0", "original-input",
                "order-retry", "2.0", "original-input"), details);
        CompleteOrchestrationAction completion = replay.getActions().iterator().next().getCompleteOrchestration();
        assertEquals(OrchestrationStatus.ORCHESTRATION_STATUS_COMPLETED, completion.getOrchestrationStatus());
        assertEquals("\"done\"", completion.getResult().getValue());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void ordinaryOrLegacyHistoryHasNoSourceReference(String sourceId) {
        List<String> sources = new ArrayList<>();
        TaskOrchestrationExecutor executor = executor(ctx -> {
            sources.add(ctx.getSourceInstanceId());
            ctx.complete(null);
        });
        List<HistoryEvent> events = startEvents(sourceId, false);
        executor.execute(Collections.emptyList(), events, null);
        executor.execute(events, Collections.emptyList(), null);
        assertEquals(Arrays.asList(null, null), sources);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void childDoesNotExposeInheritedSourceTagDuringLiveExecutionOrReplay(boolean replay) {
        List<String> sources = new ArrayList<>();
        TaskOrchestrationExecutor executor = executor(ctx -> {
            sources.add(ctx.getSourceInstanceId());
            ctx.complete(null);
        });
        List<HistoryEvent> events = startEvents("original-source", true);
        executor.execute(replay ? events : Collections.emptyList(),
                replay ? Collections.emptyList() : events, null);
        assertEquals(Collections.singletonList(null), sources);
    }
}
