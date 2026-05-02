// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory.entity;

import com.microsoft.durabletask.DataConverter;
import com.microsoft.durabletask.EntityInstanceId;
import com.microsoft.durabletask.JacksonDataConverter;
import com.microsoft.durabletask.NewOrchestrationInstanceOptions;
import com.microsoft.durabletask.TaskEntityContext;
import com.microsoft.durabletask.TaskEntityOperation;
import com.microsoft.durabletask.TaskEntityState;
import com.microsoft.durabletask.SignalEntityOptions;
import com.microsoft.durabletask.exporthistory.constants.ExportJobOperationNames;
import com.microsoft.durabletask.exporthistory.exception.ExportJobInvalidTransitionException;
import com.microsoft.durabletask.exporthistory.models.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link ExportJob} entity.
 */
class ExportJobTest {

    private ExportJob entity;
    private DataConverter dataConverter;

    @BeforeEach
    void setUp() {
        entity = new ExportJob();
        dataConverter = new JacksonDataConverter();
    }

    // region Create operation

    @Test
    void create_fromPending_setsActiveStatusAndConfig() throws Exception {
        ExportJobCreationOptions options = new ExportJobCreationOptions(
                "test-job", ExportMode.BATCH,
                Instant.now().minusSeconds(3600), Instant.now(),
                new ExportDestination("container"), null, null, 50);

        TaskEntityOperation op = mockOperation(ExportJobOperationNames.CREATE, options, null);
        entity.run(op);

        ExportJobState state = captureState(op);
        assertEquals(ExportJobStatus.ACTIVE, state.getStatus());
        assertNotNull(state.getConfig());
        assertEquals(ExportMode.BATCH, state.getConfig().getMode());
        assertEquals(50, state.getConfig().getMaxInstancesPerBatch());
        assertNotNull(state.getCreatedAt());
        assertEquals(0, state.getScannedInstances());
    }

    @Test
    void create_fromFailed_resetsAndActivates() throws Exception {
        ExportJobState initialState = new ExportJobState();
        initialState.setStatus(ExportJobStatus.FAILED);
        initialState.setLastError("previous error");
        initialState.setScannedInstances(42);

        ExportJobCreationOptions options = new ExportJobCreationOptions(
                "test-job", ExportMode.CONTINUOUS,
                Instant.now().minusSeconds(3600), null,
                new ExportDestination("container"), null, null, 100);

        TaskEntityOperation op = mockOperation(ExportJobOperationNames.CREATE, options, initialState);
        entity.run(op);

        ExportJobState state = captureState(op);
        assertEquals(ExportJobStatus.ACTIVE, state.getStatus());
        assertNull(state.getLastError());
        assertEquals(0, state.getScannedInstances());
    }

    @Test
    void create_fromActive_throws() {
        ExportJobState initialState = new ExportJobState();
        initialState.setStatus(ExportJobStatus.ACTIVE);

        ExportJobCreationOptions options = new ExportJobCreationOptions(
                "test-job", ExportMode.BATCH,
                Instant.now().minusSeconds(3600), Instant.now(),
                new ExportDestination("container"), null, null, 100);

        TaskEntityOperation op = mockOperation(ExportJobOperationNames.CREATE, options, initialState);
        assertThrows(ExportJobInvalidTransitionException.class, () -> entity.run(op));
    }

    // endregion

    // region Get operation

    @Test
    void get_returnsCurrentState() throws Exception {
        ExportJobState initialState = new ExportJobState();
        initialState.setStatus(ExportJobStatus.ACTIVE);
        initialState.setScannedInstances(10);

        TaskEntityOperation op = mockOperation(ExportJobOperationNames.GET, null, initialState);
        Object result = entity.run(op);

        assertNotNull(result);
        assertTrue(result instanceof ExportJobState);
        assertEquals(ExportJobStatus.ACTIVE, ((ExportJobState) result).getStatus());
    }

    @Test
    void get_withNoExistingState_returnsPendingState() throws Exception {
        TaskEntityOperation op = mockOperation(ExportJobOperationNames.GET, null, null);
        Object result = entity.run(op);

        assertNotNull(result);
        assertTrue(result instanceof ExportJobState);
        assertEquals(ExportJobStatus.PENDING, ((ExportJobState) result).getStatus());
    }

    // endregion

    // region CommitCheckpoint operation

    @Test
    void commitCheckpoint_withSuccessfulBatch_updatesProgress() throws Exception {
        ExportJobState initialState = new ExportJobState();
        initialState.setStatus(ExportJobStatus.ACTIVE);
        initialState.setScannedInstances(10);
        initialState.setExportedInstances(8);

        CommitCheckpointRequest request = new CommitCheckpointRequest(
                5, 5, new ExportCheckpoint("next-key"), null);

        TaskEntityOperation op = mockOperation(ExportJobOperationNames.COMMIT_CHECKPOINT, request, initialState);
        entity.run(op);

        ExportJobState state = captureState(op);
        assertEquals(15, state.getScannedInstances());
        assertEquals(13, state.getExportedInstances());
        assertEquals("next-key", state.getCheckpoint().getLastInstanceKey());
        assertEquals(ExportJobStatus.ACTIVE, state.getStatus());
    }

    @Test
    void commitCheckpoint_withFailedBatch_marksAsFailed() throws Exception {
        ExportJobState initialState = new ExportJobState();
        initialState.setStatus(ExportJobStatus.ACTIVE);

        CommitCheckpointRequest request = new CommitCheckpointRequest(
                0, 0, null,
                Collections.singletonList(new ExportFailure("inst-1", "timeout", 3, Instant.now())));

        TaskEntityOperation op = mockOperation(ExportJobOperationNames.COMMIT_CHECKPOINT, request, initialState);
        entity.run(op);

        ExportJobState state = captureState(op);
        assertEquals(ExportJobStatus.FAILED, state.getStatus());
        assertNotNull(state.getLastError());
        assertTrue(state.getLastError().contains("inst-1"));
    }

    // endregion

    // region MarkAsCompleted / MarkAsFailed

    @Test
    void markAsCompleted_fromActive_succeeds() throws Exception {
        ExportJobState initialState = new ExportJobState();
        initialState.setStatus(ExportJobStatus.ACTIVE);

        TaskEntityOperation op = mockOperation(ExportJobOperationNames.MARK_AS_COMPLETED, null, initialState);
        entity.run(op);

        ExportJobState state = captureState(op);
        assertEquals(ExportJobStatus.COMPLETED, state.getStatus());
        assertNull(state.getLastError());
    }

    @Test
    void markAsCompleted_fromPending_throws() {
        ExportJobState initialState = new ExportJobState();
        initialState.setStatus(ExportJobStatus.PENDING);

        TaskEntityOperation op = mockOperation(ExportJobOperationNames.MARK_AS_COMPLETED, null, initialState);
        assertThrows(ExportJobInvalidTransitionException.class, () -> entity.run(op));
    }

    @Test
    void markAsFailed_fromActive_succeeds() throws Exception {
        ExportJobState initialState = new ExportJobState();
        initialState.setStatus(ExportJobStatus.ACTIVE);

        TaskEntityOperation op = mockOperation(ExportJobOperationNames.MARK_AS_FAILED, "some error", initialState);
        entity.run(op);

        ExportJobState state = captureState(op);
        assertEquals(ExportJobStatus.FAILED, state.getStatus());
        assertEquals("some error", state.getLastError());
    }

    // endregion

    // region Delete

    @Test
    void delete_clearsState() throws Exception {
        ExportJobState initialState = new ExportJobState();
        initialState.setStatus(ExportJobStatus.ACTIVE);

        TaskEntityOperation op = mockOperation(ExportJobOperationNames.DELETE, null, initialState);
        entity.run(op);

        // Verify setState(null) was called (entity deletion)
        TaskEntityState mockState = op.getState();
        verify(mockState).setState(null);
    }

    // endregion

    // region Unknown operation

    @Test
    void unknownOperation_throws() {
        TaskEntityOperation op = mockOperation("UnknownOp", null, null);
        assertThrows(IllegalArgumentException.class, () -> entity.run(op));
    }

    // endregion

    // region Helpers

    private TaskEntityOperation mockOperation(String operationName, Object input, ExportJobState initialState) {
        TaskEntityOperation op = mock(TaskEntityOperation.class);
        TaskEntityState mockState = mock(TaskEntityState.class);
        TaskEntityContext mockCtx = mock(TaskEntityContext.class);

        when(op.getName()).thenReturn(operationName);
        when(op.getState()).thenReturn(mockState);
        when(op.getContext()).thenReturn(mockCtx);
        when(mockCtx.getId()).thenReturn(new EntityInstanceId("ExportJob", "test-job"));
        when(mockCtx.startNewOrchestration(anyString(), any(), any(NewOrchestrationInstanceOptions.class)))
                .thenReturn("orchestrator-instance-id");

        // Set up input deserialization
        if (input != null) {
            doReturn(input).when(op).getInput(any());
        }

        // Set up state
        when(mockState.getState(ExportJobState.class)).thenReturn(initialState);

        return op;
    }

    private ExportJobState captureState(TaskEntityOperation op) {
        org.mockito.ArgumentCaptor<Object> captor = org.mockito.ArgumentCaptor.forClass(Object.class);
        // Get the last state that was set
        TaskEntityState mockState = op.getState();
        verify(mockState, atLeastOnce()).setState(captor.capture());
        Object lastValue = captor.getValue();
        if (lastValue instanceof ExportJobState) {
            return (ExportJobState) lastValue;
        }
        fail("Expected ExportJobState but got: " + (lastValue != null ? lastValue.getClass() : "null"));
        return null;
    }

    // endregion
}
