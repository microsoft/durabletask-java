// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * These integration tests are designed to exercise the core, high-level error-handling features of the Durable Task
 * programming model.
 * <p/>
 * These tests currently require a sidecar process to be running on the local machine (the sidecar is what accepts the
 * client operations and sends invocation instructions to the DurableTaskWorker).
 */
@Tag("integration")
public class ErrorHandlingIntegrationTests extends IntegrationTestBase {
    @Test
    void orchestratorException() {
        final String orchestratorName = "OrchestratorWithException";
        final String errorMessage = "Kah-BOOOOOM!!!";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    throw new RuntimeException(errorMessage);
                })
                .buildAndStart();

        DurableTaskClient client = DurableTaskGrpcClient.newBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

            FailureDetails details = instance.getFailureDetails();
            assertNotNull(details);
            assertEquals("java.lang.RuntimeException", details.getErrorType());
            assertTrue(details.getErrorMessage().contains(errorMessage));
            assertNotNull(details.getStackTrace());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void activityException(boolean handleException) {
        final String orchestratorName = "OrchestratorWithActivityException";
        final String activityName = "Throw";
        final String errorMessage = "Kah-BOOOOOM!!!";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    try {
                        ctx.callActivity(activityName).get();
                    } catch (TaskFailedException ex) {
                        if (handleException) {
                            ctx.complete("handled");
                        } else {
                            throw ex;
                        }
                    }
                })
                .addActivity(activityName, ctx -> {
                    throw new RuntimeException(errorMessage);
                })
                .buildAndStart();

        DurableTaskClient client = DurableTaskGrpcClient.newBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, "");
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);

            if (handleException) {
                String result = instance.readOutputAs(String.class);
                assertNotNull(result);
                assertEquals("handled", result);
            } else {
                assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

                FailureDetails details = instance.getFailureDetails();
                assertNotNull(details);

                String expectedMessage = String.format(
                        "Task '%s' (#0) failed with an unhandled exception: %s",
                        activityName,
                        errorMessage);
                assertEquals(expectedMessage, details.getErrorMessage());
                assertEquals("com.microsoft.durabletask.TaskFailedException", details.getErrorType());
                assertNotNull(details.getStackTrace());
                // CONSIDER: Additional validation of getErrorDetails?
            }
        }
    }
}
