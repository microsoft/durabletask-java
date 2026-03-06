// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    void orchestratorException() throws TimeoutException {
        final String orchestratorName = "OrchestratorWithException";
        final String errorMessage = "Kah-BOOOOOM!!!";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    throw new RuntimeException(errorMessage);
                })
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();
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
    void activityException(boolean handleException) throws TimeoutException {
        final String orchestratorName = "OrchestratorWithActivityException";
        final String activityName = "Throw";
        final String errorMessage = "Kah-BOOOOOM!!!";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    try {
                        ctx.callActivity(activityName).await();
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

        DurableTaskClient client = this.createClientBuilder().build();
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

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10})
    public void retryActivityFailures(int maxNumberOfAttempts) throws TimeoutException {
        // There is one task for each activity call and one task between each retry
        int expectedTaskCount = (maxNumberOfAttempts * 2) - 1;
        this.retryOnFailuresCoreTest(maxNumberOfAttempts, expectedTaskCount, ctx -> {
            RetryPolicy retryPolicy = getCommonRetryPolicy(maxNumberOfAttempts);
            ctx.callActivity(
                    "BustedActivity",
                    null,
                    new TaskOptions(retryPolicy)).await();
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10})
    public void retryActivityFailuresWithCustomLogic(int maxNumberOfAttempts) throws TimeoutException {
        // This gets incremented every time the retry handler is invoked
        AtomicInteger retryHandlerCalls = new AtomicInteger();

        // Run the test and get back the details of the last failure
        this.retryOnFailuresCoreTest(maxNumberOfAttempts, maxNumberOfAttempts, ctx -> {
            RetryHandler retryHandler = getCommonRetryHandler(retryHandlerCalls, maxNumberOfAttempts);
            TaskOptions options = new TaskOptions(retryHandler);
            ctx.callActivity("BustedActivity", null, options).await();
        });

        // Assert that the retry handle got invoked the expected number of times
        assertEquals(maxNumberOfAttempts, retryHandlerCalls.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void subOrchestrationException(boolean handleException) throws TimeoutException {
        final String orchestratorName = "OrchestrationWithBustedSubOrchestrator";
        final String subOrchestratorName = "BustedSubOrchestrator";
        final String errorMessage = "Kah-BOOOOOM!!!";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    try {
                        String result = ctx.callSubOrchestrator(subOrchestratorName, "", String.class).await();
                        ctx.complete(result);
                    } catch (TaskFailedException ex) {
                        if (handleException) {
                            ctx.complete("handled");
                        } else {
                            throw ex;
                        }
                    }
                })
                .addOrchestrator(subOrchestratorName, ctx -> {
                    throw new RuntimeException(errorMessage);
                })
                .buildAndStart();
        DurableTaskClient client = this.createClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, 1);
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            if (handleException) {
                assertEquals(OrchestrationRuntimeStatus.COMPLETED, instance.getRuntimeStatus());
                String result = instance.readOutputAs(String.class);
                assertNotNull(result);
                assertEquals("handled", result);
            } else {
                assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());
                FailureDetails details = instance.getFailureDetails();
                assertNotNull(details);
                String expectedMessage = String.format(
                        "Task '%s' (#0) failed with an unhandled exception: %s",
                        subOrchestratorName,
                        errorMessage);
                assertEquals(expectedMessage, details.getErrorMessage());
                assertEquals("com.microsoft.durabletask.TaskFailedException", details.getErrorType());
                assertNotNull(details.getStackTrace());
                // CONSIDER: Additional validation of getStackTrace?
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10})
    public void retrySubOrchestratorFailures(int maxNumberOfAttempts) throws TimeoutException {
        // There is one task for each sub-orchestrator call and one task between each retry
        int expectedTaskCount = (maxNumberOfAttempts * 2) - 1;
        this.retryOnFailuresCoreTest(maxNumberOfAttempts, expectedTaskCount, ctx -> {
                RetryPolicy retryPolicy = getCommonRetryPolicy(maxNumberOfAttempts);
                ctx.callSubOrchestrator(
                        "BustedSubOrchestrator",
                        null,
                        null,
                        new TaskOptions(retryPolicy)).await();
            });
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10})
    public void retrySubOrchestrationFailuresWithCustomLogic(int maxNumberOfAttempts) throws TimeoutException {
        // This gets incremented every time the retry handler is invoked
        AtomicInteger retryHandlerCalls = new AtomicInteger();

        // Run the test and get back the details of the last failure
        this.retryOnFailuresCoreTest(maxNumberOfAttempts, maxNumberOfAttempts, ctx -> {
            RetryHandler retryHandler = getCommonRetryHandler(retryHandlerCalls, maxNumberOfAttempts);
            TaskOptions options = new TaskOptions(retryHandler);
            ctx.callSubOrchestrator("BustedSubOrchestrator", null, null, options).await();
        });

        // Assert that the retry handle got invoked the expected number of times
        assertEquals(maxNumberOfAttempts, retryHandlerCalls.get());
    }

    private static RetryPolicy getCommonRetryPolicy(int maxNumberOfAttempts) {
        // Include a small delay between each retry to exercise the implicit timer path
        return new RetryPolicy(maxNumberOfAttempts, Duration.ofMillis(1));
    }

    private static RetryHandler getCommonRetryHandler(AtomicInteger handlerInvocationCounter, int maxNumberOfAttempts) {
        return ctx -> {
            // Retry handlers get executed on the orchestrator thread and go through replay
            if (!ctx.getOrchestrationContext().getIsReplaying()) {
                handlerInvocationCounter.getAndIncrement();
            }

            // The isCausedBy() method is designed to handle exception inheritance
            if (!ctx.getLastFailure().isCausedBy(Exception.class)) {
                return false;
            }

            // This is the actual exception type we care about
            if (!ctx.getLastFailure().isCausedBy(RuntimeException.class)) {
                return false;
            }

            // Quit after N attempts
            return ctx.getLastAttemptNumber() < maxNumberOfAttempts;
        };
    }

    /**
     * Shared logic for execution an orchestration with an activity that constantly fails.
     * @param maxNumberOfAttempts The expected maximum number of activity execution attempts
     * @param expectedTaskCount The expected number of tasks to be scheduled by the main orchestration.
     * @param mainOrchestration The main orchestration implementation, which is expected to call either the
     *                          "BustedActivity" activity or the "BustedSubOrchestrator" sub-orchestration.
     * @return Returns the details of the <i>last</i> activity or sub-orchestration failure.
     */
    private FailureDetails retryOnFailuresCoreTest(
            int maxNumberOfAttempts,
            int expectedTaskCount,
            TaskOrchestration mainOrchestration) throws TimeoutException {
        final String orchestratorName = "MainOrchestrator";

        AtomicInteger actualAttemptCount = new AtomicInteger();

        // The caller of this test provides the top-level orchestration implementation. This method provides both a
        // failing sub-orchestration and a failing activity implementation for it to use. The expectation is that the
        // main orchestration tries to invoke just one of them and is configured with retry configuration.
        AtomicBoolean isActivityPath = new AtomicBoolean(false);
        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, mainOrchestration)
                .addOrchestrator("BustedSubOrchestrator", ctx -> {
                    actualAttemptCount.getAndIncrement();
                    throw new RuntimeException("Error #" + actualAttemptCount.get());
                })
                .addActivity("BustedActivity", ctx -> {
                    actualAttemptCount.getAndIncrement();
                    isActivityPath.set(true);
                    throw new RuntimeException("Error #" + actualAttemptCount.get());
                })
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, "");
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

            // Make sure the exception details are still what we expect
            FailureDetails details = instance.getFailureDetails();
            assertNotNull(details);

            // Confirm the number of attempts
            assertEquals(maxNumberOfAttempts, actualAttemptCount.get());

            // Make sure the surfaced exception is the last one. This is reflected in both the task ID and the
            // error message. Note that the final task ID depends on how many tasks get executed as part of the main
            // orchestration's definition. This includes any implicit timers created by a retry policy. Validating
            // the final task ID is useful to ensure that changes to retry policy implementations don't break backwards
            // compatibility due to an unexpected history change (this has happened before).
            String expectedExceptionMessage = "Error #" + maxNumberOfAttempts;
            int expectedTaskId = expectedTaskCount - 1; // Task IDs are zero-indexed
            String taskName = isActivityPath.get() ? "BustedActivity" : "BustedSubOrchestrator";
            String expectedMessage = String.format(
                    "Task '%s' (#%d) failed with an unhandled exception: %s",
                    taskName,
                    expectedTaskId,
                    expectedExceptionMessage);
            assertEquals(expectedMessage, details.getErrorMessage());
            assertEquals("com.microsoft.durabletask.TaskFailedException", details.getErrorType());
            assertNotNull(details.getStackTrace());
            return details;
        }
    }

    /**
     * Tests that inner exception details are preserved without a provider, and no properties are included.
     */
    @Test
    void innerExceptionDetailsArePreserved() throws TimeoutException {
        final String orchestratorName = "Parent";
        final String subOrchestratorName = "Sub";
        final String activityName = "ThrowException";

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .addOrchestrator(orchestratorName, ctx -> {
                    ctx.callSubOrchestrator(subOrchestratorName, "", String.class).await();
                })
                .addOrchestrator(subOrchestratorName, ctx -> {
                    ctx.callActivity(activityName).await();
                })
                .addActivity(activityName, ctx -> {
                    throw new RuntimeException("first",
                            new IllegalArgumentException("second",
                                    new IllegalStateException("third")));
                })
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, "");
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

            // Top-level: parent orchestration failed with TaskFailedException wrapping the sub-orchestration
            FailureDetails topLevel = instance.getFailureDetails();
            assertNotNull(topLevel);
            assertEquals("com.microsoft.durabletask.TaskFailedException", topLevel.getErrorType());
            assertTrue(topLevel.getErrorMessage().contains(subOrchestratorName));

            // Level 1: sub-orchestration failed with TaskFailedException wrapping the activity
            assertNotNull(topLevel.getInnerFailure());
            FailureDetails subOrchFailure = topLevel.getInnerFailure();
            assertEquals("com.microsoft.durabletask.TaskFailedException", subOrchFailure.getErrorType());
            assertTrue(subOrchFailure.getErrorMessage().contains(activityName));

            // Level 2: actual exception from the activity - RuntimeException("first")
            assertNotNull(subOrchFailure.getInnerFailure());
            FailureDetails activityFailure = subOrchFailure.getInnerFailure();
            assertEquals("java.lang.RuntimeException", activityFailure.getErrorType());
            assertEquals("first", activityFailure.getErrorMessage());

            // Level 3: inner cause - IllegalArgumentException("second")
            assertNotNull(activityFailure.getInnerFailure());
            FailureDetails innerCause1 = activityFailure.getInnerFailure();
            assertEquals("java.lang.IllegalArgumentException", innerCause1.getErrorType());
            assertEquals("second", innerCause1.getErrorMessage());

            // Level 4: innermost cause - IllegalStateException("third")
            assertNotNull(innerCause1.getInnerFailure());
            FailureDetails innerCause2 = innerCause1.getInnerFailure();
            assertEquals("java.lang.IllegalStateException", innerCause2.getErrorType());
            assertEquals("third", innerCause2.getErrorMessage());
            assertNull(innerCause2.getInnerFailure());

            // No provider registered, so no properties at any level
            assertNull(topLevel.getProperties());
            assertNull(subOrchFailure.getProperties());
            assertNull(activityFailure.getProperties());
            assertNull(innerCause1.getProperties());
            assertNull(innerCause2.getProperties());
        }
    }

    /**
     * Tests that a registered {@link ExceptionPropertiesProvider} extracts custom properties
     * from an activity exception into {@link FailureDetails#getProperties()}.
     */
    @Test
    void customExceptionPropertiesInFailureDetails() throws TimeoutException {
        final String orchestratorName = "OrchestrationWithCustomException";
        final String activityName = "BusinessActivity";

        ExceptionPropertiesProvider provider = exception -> {
            if (exception instanceof IllegalArgumentException) {
                Map<String, Object> props = new HashMap<>();
                props.put("paramName", exception.getMessage());
                return props;
            }
            if (exception instanceof BusinessValidationException) {
                BusinessValidationException bve = (BusinessValidationException) exception;
                Map<String, Object> props = new HashMap<>();
                props.put("errorCode", bve.errorCode);
                props.put("retryCount", bve.retryCount);
                props.put("isCritical", bve.isCritical);
                return props;
            }
            return null;
        };

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .exceptionPropertiesProvider(provider)
                .addOrchestrator(orchestratorName, ctx -> {
                    ctx.callActivity(activityName).await();
                })
                .addActivity(activityName, ctx -> {
                    throw new BusinessValidationException(
                            "Business logic validation failed",
                            "VALIDATION_FAILED",
                            3,
                            true);
                })
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, "");
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

            FailureDetails topLevel = instance.getFailureDetails();
            assertNotNull(topLevel);
            assertEquals("com.microsoft.durabletask.TaskFailedException", topLevel.getErrorType());

            // The activity failure is in the inner failure
            assertNotNull(topLevel.getInnerFailure());
            FailureDetails innerFailure = topLevel.getInnerFailure();
            assertTrue(innerFailure.getErrorType().contains("BusinessValidationException"));
            assertEquals("Business logic validation failed", innerFailure.getErrorMessage());

            // Verify custom properties are included
            assertNotNull(innerFailure.getProperties());
            assertEquals(3, innerFailure.getProperties().size());
            assertEquals("VALIDATION_FAILED", innerFailure.getProperties().get("errorCode"));
            assertEquals(3.0, innerFailure.getProperties().get("retryCount"));
            assertEquals(true, innerFailure.getProperties().get("isCritical"));
        }
    }

    /**
     * Tests that properties from a directly-thrown orchestration exception are on the top-level failure.
     */
    @Test
    void orchestrationDirectExceptionWithProperties() throws TimeoutException {
        final String orchestratorName = "OrchestrationWithDirectException";
        final String paramName = "testParameter";

        ExceptionPropertiesProvider provider = exception -> {
            if (exception instanceof IllegalArgumentException) {
                Map<String, Object> props = new HashMap<>();
                props.put("paramName", exception.getMessage());
                return props;
            }
            return null;
        };

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .exceptionPropertiesProvider(provider)
                .addOrchestrator(orchestratorName, ctx -> {
                    throw new IllegalArgumentException(paramName);
                })
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(orchestratorName, "");
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

            FailureDetails details = instance.getFailureDetails();
            assertNotNull(details);
            assertEquals("java.lang.IllegalArgumentException", details.getErrorType());
            assertTrue(details.getErrorMessage().contains(paramName));

            // Verify custom properties from provider
            assertNotNull(details.getProperties());
            assertEquals(1, details.getProperties().size());
            assertEquals(paramName, details.getProperties().get("paramName"));
        }
    }

    /**
     * Tests that custom properties survive through a parent -> sub-orchestration -> activity chain.
     */
    @Test
    void nestedOrchestrationExceptionPropertiesPreserved() throws TimeoutException {
        final String parentOrchName = "ParentOrch";
        final String subOrchName = "SubOrch";
        final String activityName = "ActivityWithProps";
        final String errorCode = "ERR_123";

        ExceptionPropertiesProvider provider = exception -> {
            if (exception instanceof BusinessValidationException) {
                BusinessValidationException bve = (BusinessValidationException) exception;
                Map<String, Object> props = new HashMap<>();
                props.put("errorCode", bve.errorCode);
                props.put("retryCount", bve.retryCount);
                props.put("isCritical", bve.isCritical);
                return props;
            }
            return null;
        };

        DurableTaskGrpcWorker worker = this.createWorkerBuilder()
                .exceptionPropertiesProvider(provider)
                .addOrchestrator(parentOrchName, ctx -> {
                    ctx.callSubOrchestrator(subOrchName, "", String.class).await();
                })
                .addOrchestrator(subOrchName, ctx -> {
                    ctx.callActivity(activityName).await();
                })
                .addActivity(activityName, ctx -> {
                    throw new BusinessValidationException("nested error", errorCode, 5, false);
                })
                .buildAndStart();

        DurableTaskClient client = this.createClientBuilder().build();
        try (worker; client) {
            String instanceId = client.scheduleNewOrchestrationInstance(parentOrchName, "");
            OrchestrationMetadata instance = client.waitForInstanceCompletion(instanceId, defaultTimeout, true);
            assertNotNull(instance);
            assertEquals(OrchestrationRuntimeStatus.FAILED, instance.getRuntimeStatus());

            // Parent -> TaskFailedException wrapping sub-orch
            FailureDetails topLevel = instance.getFailureDetails();
            assertNotNull(topLevel);
            assertTrue(topLevel.isCausedBy(TaskFailedException.class));

            // Sub-orch -> TaskFailedException wrapping activity
            assertNotNull(topLevel.getInnerFailure());
            assertTrue(topLevel.getInnerFailure().isCausedBy(TaskFailedException.class));

            // Activity -> BusinessValidationException with properties
            assertNotNull(topLevel.getInnerFailure().getInnerFailure());
            FailureDetails activityFailure = topLevel.getInnerFailure().getInnerFailure();
            assertTrue(activityFailure.getErrorType().contains("BusinessValidationException"));

            // Verify properties survived the full chain
            assertNotNull(activityFailure.getProperties());
            assertEquals(errorCode, activityFailure.getProperties().get("errorCode"));
            assertEquals(5.0, activityFailure.getProperties().get("retryCount"));
            assertEquals(false, activityFailure.getProperties().get("isCritical"));
        }
    }

    static class BusinessValidationException extends RuntimeException {
        final String errorCode;
        final int retryCount;
        final boolean isCritical;

        BusinessValidationException(String message, String errorCode, int retryCount, boolean isCritical) {
            super(message);
            this.errorCode = errorCode;
            this.retryCount = retryCount;
            this.isCritical = isCritical;
        }
    }
}
