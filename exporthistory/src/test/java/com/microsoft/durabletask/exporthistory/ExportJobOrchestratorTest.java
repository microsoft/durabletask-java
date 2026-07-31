// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.EntityInstanceId;
import com.microsoft.durabletask.Task;
import com.microsoft.durabletask.TaskOptions;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.TaskOrchestrationEntityFeature;
import com.microsoft.durabletask.interruption.ContinueAsNewInterruption;
import com.microsoft.durabletask.interruption.OrchestratorBlockedException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ExportJobOrchestrator} control-flow handling.
 * <p>
 * Regression guard: the Java Durable Task SDK suspends an orchestrator by <em>throwing</em>
 * {@link OrchestratorBlockedException} from {@code await()} and {@link ContinueAsNewInterruption} from
 * {@code continueAsNew()}. The orchestrator must rethrow these — never swallow them via a broad
 * {@code catch (RuntimeException)} — or the orchestration gets stuck and the export never progresses.
 */
class ExportJobOrchestratorTest {

    @Test
    void run_rethrowsOrchestratorBlockedException() {
        OrchestratorBlockedException blocked = new OrchestratorBlockedException("blocked");
        TaskOrchestrationContext ctx = contextWhereGetThrows(blocked);

        OrchestratorBlockedException thrown = assertThrows(
                OrchestratorBlockedException.class, () -> new ExportJobOrchestrator().run(ctx));
        assertSame(blocked, thrown);
    }

    @Test
    void run_rethrowsContinueAsNewInterruption() {
        ContinueAsNewInterruption interrupt = new ContinueAsNewInterruption("continueAsNew");
        TaskOrchestrationContext ctx = contextWhereGetThrows(interrupt);

        ContinueAsNewInterruption thrown = assertThrows(
                ContinueAsNewInterruption.class, () -> new ExportJobOrchestrator().run(ctx));
        assertSame(interrupt, thrown);
    }

    @Test
    void run_continuousMode_resumesAfterIdleTimerAndStopsWhenJobDeleted() {
        EntityInstanceId entityId = new EntityInstanceId("ExportJob", "job-1");
        TaskOrchestrationEntityFeature entities = mock(TaskOrchestrationEntityFeature.class);
        TaskOrchestrationContext ctx = context(entityId, entities);

        Task<ExportJobState> initialState = taskReturning(activeState(ExportMode.CONTINUOUS));
        Task<ExportJobState> currentState = taskReturning(activeState(ExportMode.CONTINUOUS));
        Task<ExportJobState> deletedState = taskReturning((ExportJobState) null);
        when(entities.callEntity(any(), eq(ExportJobTransitions.OP_GET), any(), eq(ExportJobState.class)))
                .thenReturn(initialState, currentState, deletedState);

        Task<InstancePage> listTask = taskReturning(new InstancePage(Collections.emptyList(), null));
        when(ctx.callActivity(eq(ListTerminalInstancesActivity.NAME), any(), eq(InstancePage.class)))
                .thenReturn(listTask);
        Task<Void> idleTimer = taskReturning((Void) null);
        when(ctx.createTimer(any(Duration.class))).thenReturn(idleTimer);

        new ExportJobOrchestrator().run(ctx);

        verify(ctx).createTimer(Duration.ofMinutes(1));
        verify(entities, times(3)).callEntity(
                eq(entityId), eq(ExportJobTransitions.OP_GET), any(), eq(ExportJobState.class));
        verify(ctx).callActivity(eq(ListTerminalInstancesActivity.NAME), any(), eq(InstancePage.class));
    }

    @Test
    void run_batchExportFailure_retriesTwiceThenCommitsSuccess() {
        EntityInstanceId entityId = new EntityInstanceId("ExportJob", "job-1");
        TaskOrchestrationEntityFeature entities = mock(TaskOrchestrationEntityFeature.class);
        TaskOrchestrationContext ctx = context(entityId, entities);
        Task<ExportJobState> activeState = taskReturning(activeState(ExportMode.BATCH));
        when(entities.callEntity(any(), eq(ExportJobTransitions.OP_GET), any(), eq(ExportJobState.class)))
                .thenReturn(activeState);

        Task<InstancePage> pageTask = taskReturning(
                new InstancePage(Collections.singletonList("inst-1"), new ExportCheckpoint("inst-1")));
        Task<InstancePage> emptyPageTask = taskReturning(new InstancePage(Collections.emptyList(), null));
        when(ctx.callActivity(eq(ListTerminalInstancesActivity.NAME), any(), eq(InstancePage.class)))
                .thenReturn(pageTask, emptyPageTask);
        Task<ExportResult> exportTask = taskReturning((ExportResult) null);
        when(ctx.callActivity(eq(ExportInstanceHistoryActivity.NAME), any(), any(TaskOptions.class),
                eq(ExportResult.class))).thenReturn(exportTask);

        Task<List<ExportResult>> failedBatch = taskReturning(
                Collections.singletonList(ExportResult.failure("inst-1", "boom")));
        Task<List<ExportResult>> successfulBatch = taskReturning(
                Collections.singletonList(ExportResult.success("inst-1", "history.jsonl.gz")));
        when(ctx.allOf(ArgumentMatchers.<Task<ExportResult>>anyList()))
                .thenReturn(failedBatch, failedBatch, successfulBatch);
        Task<Void> backoffTimer = taskReturning((Void) null);
        when(ctx.createTimer(any(Duration.class))).thenReturn(backoffTimer);

        Task<Void> entityOperation = taskReturning((Void) null);
        when(entities.callEntity(
                eq(entityId), eq(ExportJobTransitions.OP_COMMIT_CHECKPOINT),
                any(CommitCheckpointRequest.class), eq(Void.class))).thenReturn(entityOperation);
        when(entities.callEntity(
                eq(entityId), eq(ExportJobTransitions.OP_MARK_AS_COMPLETED), isNull(), eq(Void.class)))
                .thenReturn(entityOperation);

        new ExportJobOrchestrator().run(ctx);

        InOrder timers = inOrder(ctx);
        timers.verify(ctx).createTimer(Duration.ofSeconds(60));
        timers.verify(ctx).createTimer(Duration.ofSeconds(120));
        verify(ctx, times(3)).callActivity(
                eq(ExportInstanceHistoryActivity.NAME), any(), any(TaskOptions.class), eq(ExportResult.class));
        verify(ctx, times(3)).allOf(ArgumentMatchers.<Task<ExportResult>>anyList());

        ArgumentCaptor<CommitCheckpointRequest> checkpoint = ArgumentCaptor.forClass(CommitCheckpointRequest.class);
        verify(entities).callEntity(
                eq(entityId), eq(ExportJobTransitions.OP_COMMIT_CHECKPOINT), checkpoint.capture(), eq(Void.class));
        assertEquals(1, checkpoint.getValue().getScannedInstances());
        assertEquals(1, checkpoint.getValue().getExportedInstances());
        assertEquals("inst-1", checkpoint.getValue().getCheckpoint().getLastInstanceKey());
        assertNull(checkpoint.getValue().getFailures());
        verify(entities).callEntity(
                eq(entityId), eq(ExportJobTransitions.OP_MARK_AS_COMPLETED), isNull(), eq(Void.class));
    }

    @Test
    void run_batchExportFailure_exhaustsRetriesThenFaultsAndMarksJobFailed() {
        EntityInstanceId entityId = new EntityInstanceId("ExportJob", "job-1");
        TaskOrchestrationEntityFeature entities = mock(TaskOrchestrationEntityFeature.class);
        TaskOrchestrationContext ctx = context(entityId, entities);
        Task<ExportJobState> activeState = taskReturning(activeState(ExportMode.BATCH));
        when(entities.callEntity(any(), eq(ExportJobTransitions.OP_GET), any(), eq(ExportJobState.class)))
                .thenReturn(activeState);

        Task<InstancePage> pageTask = taskReturning(
                new InstancePage(Collections.singletonList("inst-1"), new ExportCheckpoint("inst-1")));
        when(ctx.callActivity(eq(ListTerminalInstancesActivity.NAME), any(), eq(InstancePage.class)))
                .thenReturn(pageTask);
        Task<ExportResult> exportTask = taskReturning((ExportResult) null);
        when(ctx.callActivity(eq(ExportInstanceHistoryActivity.NAME), any(), any(TaskOptions.class),
                eq(ExportResult.class))).thenReturn(exportTask);

        Task<List<ExportResult>> failedBatch = taskReturning(
                Collections.singletonList(ExportResult.failure("inst-1", "poison")));
        when(ctx.allOf(ArgumentMatchers.<Task<ExportResult>>anyList())).thenReturn(failedBatch);
        Task<Void> backoffTimer = taskReturning((Void) null);
        when(ctx.createTimer(any(Duration.class))).thenReturn(backoffTimer);
        Instant finalAttempt = Instant.parse("2026-01-02T00:00:00Z");
        when(ctx.getCurrentInstant()).thenReturn(finalAttempt);

        Task<Void> entityOperation = taskReturning((Void) null);
        when(entities.callEntity(
                eq(entityId), eq(ExportJobTransitions.OP_COMMIT_CHECKPOINT),
                any(CommitCheckpointRequest.class), eq(Void.class))).thenReturn(entityOperation);
        when(entities.callEntity(
                eq(entityId), eq(ExportJobTransitions.OP_MARK_AS_FAILED), any(), eq(Void.class)))
                .thenReturn(entityOperation);

        IllegalStateException thrown = assertThrows(
                IllegalStateException.class, () -> new ExportJobOrchestrator().run(ctx));
        assertEquals(
                "Export job 'job-1' batch export failed after 3 retry attempts. "
                        + "Failure details: InstanceId: inst-1, Reason: poison",
                thrown.getMessage());

        InOrder timers = inOrder(ctx);
        timers.verify(ctx).createTimer(Duration.ofSeconds(60));
        timers.verify(ctx).createTimer(Duration.ofSeconds(120));
        verify(ctx, times(3)).allOf(ArgumentMatchers.<Task<ExportResult>>anyList());

        ArgumentCaptor<CommitCheckpointRequest> checkpoint = ArgumentCaptor.forClass(CommitCheckpointRequest.class);
        verify(entities).callEntity(
                eq(entityId), eq(ExportJobTransitions.OP_COMMIT_CHECKPOINT), checkpoint.capture(), eq(Void.class));
        assertEquals(0, checkpoint.getValue().getScannedInstances());
        assertEquals(0, checkpoint.getValue().getExportedInstances());
        assertNull(checkpoint.getValue().getCheckpoint());
        assertEquals(1, checkpoint.getValue().getFailures().size());
        ExportFailure failure = checkpoint.getValue().getFailures().get(0);
        assertEquals("inst-1", failure.getInstanceId());
        assertEquals("poison", failure.getReason());
        assertEquals(3, failure.getAttemptCount());
        assertEquals(finalAttempt, failure.getLastAttempt());

        // After committing failures the orchestrator throws, and the outer catch marks the job failed.
        verify(entities).callEntity(
                eq(entityId), eq(ExportJobTransitions.OP_MARK_AS_FAILED), any(), eq(Void.class));
        verify(entities, never()).callEntity(
                eq(entityId), eq(ExportJobTransitions.OP_MARK_AS_COMPLETED), any(), eq(Void.class));
    }

    private static TaskOrchestrationContext context(
            EntityInstanceId entityId, TaskOrchestrationEntityFeature entities) {
        TaskOrchestrationContext ctx = mock(TaskOrchestrationContext.class);
        when(ctx.getInput(ExportJobRunRequest.class)).thenReturn(new ExportJobRunRequest(entityId, 0));
        when(ctx.getIsReplaying()).thenReturn(true);
        when(ctx.getEntities()).thenReturn(entities);
        return ctx;
    }

    private static ExportJobState activeState(ExportMode mode) {
        ExportFilter filter = new ExportFilter(Instant.parse("2026-01-01T00:00:00Z"), null, null);
        ExportJobConfiguration config = new ExportJobConfiguration(
                mode, filter, new ExportDestination("container"),
                new ExportFormat(ExportFormatKind.JSONL, "1.0"), 10);
        ExportJobState state = new ExportJobState();
        state.setStatus(ExportJobStatus.ACTIVE);
        state.setConfig(config);
        return state;
    }

    @SuppressWarnings("unchecked")
    private static <T> Task<T> taskReturning(T value) {
        Task<T> task = (Task<T>) mock(Task.class);
        when(task.await()).thenReturn(value);
        return task;
    }

    @SuppressWarnings("unchecked")
    private static TaskOrchestrationContext contextWhereGetThrows(RuntimeException toThrow) {
        TaskOrchestrationContext ctx = mock(TaskOrchestrationContext.class);
        when(ctx.getInput(ExportJobRunRequest.class))
                .thenReturn(new ExportJobRunRequest(new EntityInstanceId("ExportJob", "job-1"), 0));
        when(ctx.getIsReplaying()).thenReturn(true);

        TaskOrchestrationEntityFeature entities = mock(TaskOrchestrationEntityFeature.class);
        when(ctx.getEntities()).thenReturn(entities);

        Task<ExportJobState> blockedTask = (Task<ExportJobState>) mock(Task.class);
        when(blockedTask.await()).thenThrow(toThrow);
        when(entities.callEntity(any(), eq(ExportJobTransitions.OP_GET), any(), eq(ExportJobState.class)))
                .thenReturn(blockedTask);

        return ctx;
    }
}
