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
 * Unit tests for TaskOrchestrationExecutor.
 */
public class TaskOrchestrationExecutorTest {

    private static final Logger logger = Logger.getLogger(TaskOrchestrationExecutorTest.class.getName());

    @Test
    void execute_unregisteredOrchestrationType_failsWithDescriptiveMessage() {
        // Arrange: create executor with no registered orchestrations
        HashMap<String, TaskOrchestrationFactory> factories = new HashMap<>();
        TaskOrchestrationExecutor executor = new TaskOrchestrationExecutor(
                factories,
                new JacksonDataConverter(),
                Duration.ofDays(3),
                logger,
                null);

        String unknownName = "NonExistentOrchestration";

        // Build history events simulating an orchestration start
        HistoryEvent orchestratorStarted = HistoryEvent.newBuilder()
                .setEventId(-1)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setOrchestratorStarted(OrchestratorStartedEvent.getDefaultInstance())
                .build();

        HistoryEvent executionStarted = HistoryEvent.newBuilder()
                .setEventId(-1)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setExecutionStarted(ExecutionStartedEvent.newBuilder()
                        .setName(unknownName)
                        .setVersion(StringValue.of(""))
                        .setInput(StringValue.of("\"test-input\""))
                        .setOrchestrationInstance(OrchestrationInstance.newBuilder()
                                .setInstanceId("test-instance-id")
                                .build())
                        .build())
                .build();

        HistoryEvent orchestratorCompleted = HistoryEvent.newBuilder()
                .setEventId(-1)
                .setTimestamp(Timestamp.getDefaultInstance())
                .setOrchestratorCompleted(OrchestratorCompletedEvent.getDefaultInstance())
                .build();

        List<HistoryEvent> pastEvents = Arrays.asList(orchestratorStarted, executionStarted, orchestratorCompleted);
        List<HistoryEvent> newEvents = Collections.emptyList();

        // Act
        TaskOrchestratorResult result = executor.execute(pastEvents, newEvents);

        // Assert: the result should contain a CompleteOrchestrationAction with FAILED status
        // and a failure message mentioning the unknown orchestration name
        OrchestratorAction action = result.getActions().iterator().next();
        assertTrue(action.hasCompleteOrchestration(), "Expected a CompleteOrchestrationAction");

        CompleteOrchestrationAction completeAction = action.getCompleteOrchestration();
        assertEquals(OrchestrationStatus.ORCHESTRATION_STATUS_FAILED, completeAction.getOrchestrationStatus());
        assertTrue(completeAction.hasFailureDetails(), "Expected failure details");

        TaskFailureDetails failureDetails = completeAction.getFailureDetails();
        assertEquals("java.lang.IllegalStateException", failureDetails.getErrorType());
        assertTrue(failureDetails.getErrorMessage().contains(unknownName),
                "Error message should contain the orchestration name: " + failureDetails.getErrorMessage());
        assertTrue(failureDetails.getErrorMessage().contains("worker"),
                "Error message should mention workers: " + failureDetails.getErrorMessage());
    }
}
