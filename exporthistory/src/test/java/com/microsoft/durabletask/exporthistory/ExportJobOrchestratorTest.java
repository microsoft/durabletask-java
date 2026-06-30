// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.EntityInstanceId;
import com.microsoft.durabletask.Task;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.TaskOrchestrationEntityFeature;
import com.microsoft.durabletask.interruption.ContinueAsNewInterruption;
import com.microsoft.durabletask.interruption.OrchestratorBlockedException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
