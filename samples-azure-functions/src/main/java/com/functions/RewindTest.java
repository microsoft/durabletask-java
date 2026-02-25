// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.OrchestrationMetadata;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Sample functions to test the rewind functionality.
 * Rewind allows a failed orchestration to be replayed from its last known good state.
 */
public class RewindTest {

    // Flag to control whether the activity should fail (first call fails, subsequent calls succeed)
    private static final AtomicBoolean shouldFail = new AtomicBoolean(true);

    /**
     * HTTP trigger that starts a rewindable orchestration, waits for it to fail,
     * then rewinds it using client.rewindInstance(). Returns the check status response
     * so the caller can poll for the orchestration to complete after the rewind.
     */
    @FunctionName("StartRewindableOrchestration")
    public HttpResponseMessage startRewindableOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Starting rewindable orchestration.");

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("RewindableOrchestration");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);

        // Wait for the orchestration to reach a terminal state (expected: Failed)
        try {
            OrchestrationMetadata metadata = client.waitForInstanceCompletion(instanceId, Duration.ofSeconds(30), false);
            context.getLogger().info("Orchestration reached terminal state: " + metadata.getRuntimeStatus());
        } catch (TimeoutException e) {
            context.getLogger().severe("Orchestration did not reach terminal state in time.");
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Orchestration did not fail within the expected time.")
                    .build();
        }

        // Rewind the failed orchestration using the client method
        client.rewindInstance(instanceId, "Testing rewind functionality");
        context.getLogger().info("Rewind request sent for instance: " + instanceId);

        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * Orchestration that calls an activity which will fail on the first attempt.
     * After rewinding, the orchestration will replay and the activity will succeed.
     */
    @FunctionName("RewindableOrchestration")
    public String rewindableOrchestration(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        // Call the activity that may fail
        String result = ctx.callActivity("FailOnceActivity", "RewindTest", String.class).await();
        return result;
    }

    /**
     * Activity that fails on the first call but succeeds on subsequent calls.
     * This simulates a transient failure that can be recovered by rewinding.
     */
    @FunctionName("FailOnceActivity")
    public String failOnceActivity(
            @DurableActivityTrigger(name = "input") String input,
            final ExecutionContext context) {
        if (shouldFail.compareAndSet(true, false)) {
            context.getLogger().warning("FailOnceActivity: Simulating failure for input: " + input);
            throw new RuntimeException("Simulated transient failure - rewind to retry");
        }
        context.getLogger().info("FailOnceActivity: Success for input: " + input);
        return input + "-rewound-success";
    }

    /**
     * HTTP trigger to reset the failure flag (useful for testing).
     */
    @FunctionName("ResetRewindFailureFlag")
    public HttpResponseMessage resetRewindFailureFlag(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        shouldFail.set(true);
        context.getLogger().info("Reset failure flag to true.");
        return request.createResponseBuilder(HttpStatus.OK)
                .body("Failure flag reset to true")
                .build();
    }
}
