// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for validating the serialization of exceptions in various scenarios.
 */
@Tag("integration")
@ExtendWith(TestRetryExtension.class)
public class ExceptionSerializationIntegrationTest extends IntegrationTestBase {

    @RetryingTest
    void testMultilineExceptionMessage() throws TimeoutException {
        final String orchestratorName = "MultilineExceptionOrchestrator";
        final String multilineErrorMessage = "Line 1\nLine 2\nLine 3";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    throw new RuntimeException(multilineErrorMessage);
                })
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

            FailureDetails details = instance.getFailureDetails();
            assertNotNull(details);
            assertEquals("java.lang.RuntimeException", details.getErrorType());
            assertEquals(multilineErrorMessage, details.getErrorMessage());
            assertNotNull(details.getStackTrace());
        }
    }

    @RetryingTest
    void testNestedExceptions() throws TimeoutException {
        final String orchestratorName = "NestedExceptionOrchestrator";
        final String innerMessage = "Inner exception";
        final String outerMessage = "Outer exception";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    Exception innerException = new IllegalArgumentException(innerMessage);
                    throw new RuntimeException(outerMessage, innerException);
                })
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

            FailureDetails details = instance.getFailureDetails();
            assertNotNull(details);
            assertEquals("java.lang.RuntimeException", details.getErrorType());
            assertEquals(outerMessage, details.getErrorMessage());
            assertNotNull(details.getStackTrace());
            
            // Verify both exceptions are in the stack trace
            String stackTrace = details.getStackTrace();
            assertTrue(stackTrace.contains(outerMessage), "Stack trace should contain outer exception message");
            assertTrue(stackTrace.contains(innerMessage), "Stack trace should contain inner exception message");
            assertTrue(stackTrace.contains("Caused by: java.lang.IllegalArgumentException"), 
                    "Stack trace should include 'Caused by' section for inner exception");
        }
    }

    @RetryingTest
    void testCustomExceptionWithNonStandardToString() throws TimeoutException {
        final String orchestratorName = "CustomExceptionOrchestrator";
        final String customMessage = "Custom exception message";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    throw new CustomException(customMessage);
                })
                .buildAndStart();

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 0);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

            FailureDetails details = instance.getFailureDetails();
            assertNotNull(details);
            String expectedType = CustomException.class.getName();
            assertEquals(expectedType, details.getErrorType());
            assertEquals(customMessage, details.getErrorMessage());
            assertNotNull(details.getStackTrace());
        }
    }

    /**
     * Custom exception class with a non-standard toString implementation.
     */
    private static class CustomException extends RuntimeException {
        public CustomException(String message) {
            super(message);
        }
        
        @Override
        public String toString() {
            return "CUSTOM_EXCEPTION_FORMAT: " + getMessage();
        }
    }
}