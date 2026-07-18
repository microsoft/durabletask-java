// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.OrchestrationMetadata;
import com.microsoft.durabletask.OrchestrationRuntimeStatus;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link ExportHistoryJobClient}. */
class ExportHistoryJobClientTest {

    @Test
    void delete_terminatesWaitsForAndPurgesExportOrchestrator() throws TimeoutException {
        DurableTaskClient durableTaskClient = mock(DurableTaskClient.class);
        OrchestrationMetadata deleteOperation = mock(OrchestrationMetadata.class);
        OrchestrationMetadata exportOrchestrator = mock(OrchestrationMetadata.class);
        String operationInstanceId = "delete-operation";
        String exportOrchestratorInstanceId = ExportHistoryConstants.getOrchestratorInstanceId("job-1");

        when(durableTaskClient.scheduleNewOrchestrationInstance(
                eq(ExecuteExportJobOperationOrchestrator.NAME), any(ExportJobOperationRequest.class)))
                .thenReturn(operationInstanceId);
        when(durableTaskClient.waitForInstanceCompletion(
                eq(operationInstanceId), any(Duration.class), eq(true)))
                .thenReturn(deleteOperation);
        when(deleteOperation.getRuntimeStatus()).thenReturn(OrchestrationRuntimeStatus.COMPLETED);
        when(durableTaskClient.waitForInstanceCompletion(
                eq(exportOrchestratorInstanceId), any(Duration.class), eq(false)))
                .thenReturn(exportOrchestrator);

        ExportHistoryJobClient client = new ExportHistoryJobClient(
                durableTaskClient, "job-1", new ExportHistoryStorageOptions());
        client.delete();

        InOrder calls = inOrder(durableTaskClient);
        calls.verify(durableTaskClient).scheduleNewOrchestrationInstance(
                eq(ExecuteExportJobOperationOrchestrator.NAME), any(ExportJobOperationRequest.class));
        calls.verify(durableTaskClient).waitForInstanceCompletion(
                eq(operationInstanceId), any(Duration.class), eq(true));
        calls.verify(durableTaskClient).terminate(exportOrchestratorInstanceId, "Export job deleted");
        calls.verify(durableTaskClient).waitForInstanceCompletion(
                eq(exportOrchestratorInstanceId), any(Duration.class), eq(false));
        calls.verify(durableTaskClient).purgeInstance(exportOrchestratorInstanceId);
    }

        @Test
        void delete_failedEntityOperationIsSurfacedWithoutCleaningUpRunner() throws TimeoutException {
                DurableTaskClient durableTaskClient = mock(DurableTaskClient.class);
                OrchestrationMetadata deleteOperation = mock(OrchestrationMetadata.class);
                String operationInstanceId = "delete-operation";
                String exportOrchestratorInstanceId = ExportHistoryConstants.getOrchestratorInstanceId("job-1");

                when(durableTaskClient.scheduleNewOrchestrationInstance(
                                eq(ExecuteExportJobOperationOrchestrator.NAME), any(ExportJobOperationRequest.class)))
                                .thenReturn(operationInstanceId);
                when(durableTaskClient.waitForInstanceCompletion(
                                eq(operationInstanceId), any(Duration.class), eq(true)))
                                .thenReturn(deleteOperation);
                when(deleteOperation.getRuntimeStatus()).thenReturn(OrchestrationRuntimeStatus.FAILED);

                ExportHistoryJobClient client = new ExportHistoryJobClient(
                                durableTaskClient, "job-1", new ExportHistoryStorageOptions());

                assertThrows(ExportJobClientValidationException.class, client::delete);
                verify(durableTaskClient, never()).terminate(eq(exportOrchestratorInstanceId), any());
                verify(durableTaskClient, never()).purgeInstance(exportOrchestratorInstanceId);
        }
}