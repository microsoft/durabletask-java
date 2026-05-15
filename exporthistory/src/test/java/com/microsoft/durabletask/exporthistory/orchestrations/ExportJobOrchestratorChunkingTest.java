// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.orchestrations;

import com.microsoft.durabletask.Task;
import com.microsoft.durabletask.TaskOptions;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.exporthistory.models.ExportDestination;
import com.microsoft.durabletask.exporthistory.models.ExportFilter;
import com.microsoft.durabletask.exporthistory.models.ExportFormat;
import com.microsoft.durabletask.exporthistory.models.ExportJobConfiguration;
import com.microsoft.durabletask.exporthistory.models.ExportMode;
import com.microsoft.durabletask.exporthistory.models.ExportRequest;
import com.microsoft.durabletask.exporthistory.models.ExportResult;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ExportJobOrchestrator#exportBatch} chunking behavior.
 * <p>
 * Verifies that activities are scheduled in chunks of {@code maxParallelExports} and
 * each chunk is awaited before the next one is scheduled.
 */
class ExportJobOrchestratorChunkingTest {

    @Test
    void exportBatch_chunksAtMaxParallelExports() {
        int maxParallel = 3;
        int totalInstances = 10;
        AtomicInteger inFlight = new AtomicInteger(0);
        AtomicInteger maxObservedConcurrency = new AtomicInteger(0);
        List<String> scheduledInstanceIds = new ArrayList<>();

        TaskOrchestrationContext ctx = mock(TaskOrchestrationContext.class);
        when(ctx.callActivity(eq("ExportInstanceHistoryActivity"), any(ExportRequest.class),
                any(TaskOptions.class), eq(ExportResult.class)))
                .thenAnswer((InvocationOnMock inv) -> {
                    ExportRequest req = inv.getArgument(1);
                    scheduledInstanceIds.add(req.getInstanceId());
                    int current = inFlight.incrementAndGet();
                    maxObservedConcurrency.updateAndGet(prev -> Math.max(prev, current));

                    @SuppressWarnings("unchecked")
                    Task<ExportResult> task = mock(Task.class);
                    when(task.await()).thenAnswer(awaitInv -> {
                        inFlight.decrementAndGet();
                        return new ExportResult(req.getInstanceId(), true, null);
                    });
                    return task;
                });

        List<String> instanceIds = new ArrayList<>();
        for (int i = 0; i < totalInstances; i++) {
            instanceIds.add("instance-" + i);
        }

        ExportJobConfiguration config = new ExportJobConfiguration(
                ExportMode.BATCH,
                new ExportFilter(),
                new ExportDestination("test-container", null),
                ExportFormat.DEFAULT,
                maxParallel,
                /* maxInstancesPerBatch */ 100);

        ExportJobOrchestrator orchestrator = new ExportJobOrchestrator();
        List<ExportResult> results = orchestrator.exportBatch(ctx, instanceIds, config);

        assertEquals(totalInstances, results.size(), "All instances should be processed");
        assertEquals(totalInstances, scheduledInstanceIds.size(), "All instances should be scheduled");
        assertTrue(maxObservedConcurrency.get() <= maxParallel,
                "In-flight count (" + maxObservedConcurrency.get() + ") must not exceed maxParallelExports ("
                        + maxParallel + ")");
        for (int i = 0; i < totalInstances; i++) {
            assertEquals("instance-" + i, scheduledInstanceIds.get(i),
                    "Instances should be scheduled in input order");
            assertTrue(results.get(i).isSuccess());
        }
    }

    @Test
    void exportBatch_singleChunkWhenInstancesUnderMaxParallel() {
        int maxParallel = 32;
        AtomicInteger maxConcurrency = new AtomicInteger(0);
        AtomicInteger inFlight = new AtomicInteger(0);

        TaskOrchestrationContext ctx = mock(TaskOrchestrationContext.class);
        when(ctx.callActivity(eq("ExportInstanceHistoryActivity"), any(ExportRequest.class),
                any(TaskOptions.class), eq(ExportResult.class)))
                .thenAnswer((InvocationOnMock inv) -> {
                    ExportRequest req = inv.getArgument(1);
                    int current = inFlight.incrementAndGet();
                    maxConcurrency.updateAndGet(prev -> Math.max(prev, current));

                    @SuppressWarnings("unchecked")
                    Task<ExportResult> task = mock(Task.class);
                    when(task.await()).thenAnswer(awaitInv -> {
                        inFlight.decrementAndGet();
                        return new ExportResult(req.getInstanceId(), true, null);
                    });
                    return task;
                });

        List<String> instanceIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            instanceIds.add("instance-" + i);
        }

        ExportJobConfiguration config = new ExportJobConfiguration(
                ExportMode.BATCH,
                new ExportFilter(),
                new ExportDestination("test-container", null),
                ExportFormat.DEFAULT,
                maxParallel,
                100);

        List<ExportResult> results = new ExportJobOrchestrator().exportBatch(ctx, instanceIds, config);

        assertEquals(5, results.size());
        assertEquals(5, maxConcurrency.get(), "All 5 should run in a single chunk");
    }

    @Test
    void exportBatch_emptyList_returnsEmpty() {
        TaskOrchestrationContext ctx = mock(TaskOrchestrationContext.class);
        ExportJobConfiguration config = new ExportJobConfiguration(
                ExportMode.BATCH,
                new ExportFilter(),
                new ExportDestination("test-container", null),
                ExportFormat.DEFAULT,
                32,
                100);

        List<ExportResult> results = new ExportJobOrchestrator().exportBatch(
                ctx, new ArrayList<>(), config);

        assertTrue(results.isEmpty());
    }

    @Test
    void exportBatch_activityFailure_capturesFailureWithInstanceId() {
        TaskOrchestrationContext ctx = mock(TaskOrchestrationContext.class);
        when(ctx.callActivity(eq("ExportInstanceHistoryActivity"), any(ExportRequest.class),
                any(TaskOptions.class), eq(ExportResult.class)))
                .thenAnswer((InvocationOnMock inv) -> {
                    ExportRequest req = inv.getArgument(1);
                    @SuppressWarnings("unchecked")
                    Task<ExportResult> task = mock(Task.class);
                    if ("fail-me".equals(req.getInstanceId())) {
                        when(task.await()).thenThrow(new RuntimeException("activity exhausted retries"));
                    } else {
                        when(task.await()).thenReturn(new ExportResult(req.getInstanceId(), true, null));
                    }
                    return task;
                });

        List<String> instanceIds = new ArrayList<>();
        instanceIds.add("ok-1");
        instanceIds.add("fail-me");
        instanceIds.add("ok-2");

        ExportJobConfiguration config = new ExportJobConfiguration(
                ExportMode.BATCH,
                new ExportFilter(),
                new ExportDestination("test-container", null),
                ExportFormat.DEFAULT,
                32,
                100);

        List<ExportResult> results = new ExportJobOrchestrator().exportBatch(ctx, instanceIds, config);

        assertEquals(3, results.size());
        assertTrue(results.get(0).isSuccess());
        assertFalse(results.get(1).isSuccess());
        assertEquals("fail-me", results.get(1).getInstanceId());
        assertTrue(results.get(1).getError().contains("activity exhausted retries"));
        assertTrue(results.get(2).isSuccess());
    }
}
