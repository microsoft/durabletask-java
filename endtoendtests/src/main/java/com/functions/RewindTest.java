package com.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
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

    // Separate flag for sub-orchestration rewind test
    private static final AtomicBoolean shouldSubFail = new AtomicBoolean(true);

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

        // Reset the failure flag so the first activity call will fail
        shouldFail.set(true);

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("RewindableOrchestration");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);

        // Wait for the orchestration to reach a terminal state (expected: Failed)
        try {
            OrchestrationMetadata metadata = client.waitForInstanceCompletion(instanceId, Duration.ofSeconds(30), false);
            context.getLogger().info("Orchestration reached terminal state: " + metadata.getRuntimeStatus());
        } catch (TimeoutException e) {
            context.getLogger().severe("Orchestration did not reach terminal state in time.");
            return request.createResponseBuilder(com.microsoft.azure.functions.HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Orchestration did not fail within the expected time.")
                    .build();
        }

        // Rewind the failed orchestration using the client method
        client.rewindInstance(instanceId, "Testing rewind functionality");
        context.getLogger().info("Rewind request sent for instance: " + instanceId);

        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * HTTP trigger that starts a non-failing orchestration, waits for it to complete,
     * then attempts to rewind it using client.rewindInstance(). Returns the check status
     * response so the caller can verify the orchestration remains in the Completed state.
     */
    @FunctionName("StartRewindNonFailedOrchestration")
    public HttpResponseMessage startRewindNonFailedOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Starting non-failing orchestration for rewind test.");

        // Ensure the activity will NOT fail
        shouldFail.set(false);

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("RewindableOrchestration");
        context.getLogger().info("Created orchestration with instance ID = " + instanceId);

        // Wait for the orchestration to complete successfully
        try {
            client.waitForInstanceCompletion(instanceId, Duration.ofSeconds(30), false);
            context.getLogger().info("Orchestration completed successfully.");
        } catch (TimeoutException e) {
            context.getLogger().severe("Orchestration did not complete in time.");
            return request.createResponseBuilder(com.microsoft.azure.functions.HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Orchestration did not complete within the expected time.")
                    .build();
        }

        // Attempt to rewind the non-failed orchestration using the client method.
        // The sidecar rejects this with FAILED_PRECONDITION since the instance is not
        // in a Failed state. The client translates this to an IllegalStateException.
        try {
            client.rewindInstance(instanceId, "Testing rewind on non-failed orchestration");
            context.getLogger().info("Rewind request sent for non-failed instance: " + instanceId);
        } catch (IllegalStateException e) {
            context.getLogger().info("Rewind on non-failed instance was rejected (expected): " + e.getMessage());
        }

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
        return request.createResponseBuilder(com.microsoft.azure.functions.HttpStatus.OK)
                .body("Failure flag reset to true")
                .build();
    }

    // --- Sub-orchestration rewind test functions ---

    /**
     * HTTP trigger that starts a parent orchestration with a failing sub-orchestration,
     * waits for it to fail, then rewinds it using client.rewindInstance().
     * Returns the check status response so the caller can poll for completion.
     */
    @FunctionName("StartRewindableSubOrchestration")
    public HttpResponseMessage startRewindableSubOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Starting rewindable sub-orchestration test.");

        // Reset the sub failure flag so the first activity call will fail
        shouldSubFail.set(true);

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("RewindableParentOrchestration");
        context.getLogger().info("Created parent orchestration with instance ID = " + instanceId);

        // Wait for the parent orchestration to reach a terminal state (expected: Failed)
        try {
            OrchestrationMetadata metadata = client.waitForInstanceCompletion(instanceId, Duration.ofSeconds(30), false);
            context.getLogger().info("Parent orchestration reached terminal state: " + metadata.getRuntimeStatus());
        } catch (TimeoutException e) {
            context.getLogger().severe("Parent orchestration did not reach terminal state in time.");
            return request.createResponseBuilder(com.microsoft.azure.functions.HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Parent orchestration did not fail within the expected time.")
                    .build();
        }

        // Rewind the failed parent orchestration using the client method
        client.rewindInstance(instanceId, "Testing rewind with sub-orchestration failure");
        context.getLogger().info("Rewind request sent for parent instance: " + instanceId);

        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * Parent orchestration that calls a sub-orchestration which may fail.
     */
    @FunctionName("RewindableParentOrchestration")
    public String rewindableParentOrchestration(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        String result = ctx.callSubOrchestrator("RewindableChildOrchestration", "SubRewindTest", String.class).await();
        return "Parent:" + result;
    }

    /**
     * Sub-orchestration that calls an activity which will fail on the first attempt.
     */
    @FunctionName("RewindableChildOrchestration")
    public String rewindableChildOrchestration(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        String result = ctx.callActivity("FailOnceSubActivity", "SubRewindTest", String.class).await();
        return result;
    }

    /**
     * Activity for sub-orchestration test that fails on the first call but succeeds on subsequent calls.
     */
    @FunctionName("FailOnceSubActivity")
    public String failOnceSubActivity(
            @DurableActivityTrigger(name = "input") String input,
            final ExecutionContext context) {
        if (shouldSubFail.compareAndSet(true, false)) {
            context.getLogger().warning("FailOnceSubActivity: Simulating failure for input: " + input);
            throw new RuntimeException("Simulated sub-orchestration transient failure - rewind to retry");
        }
        context.getLogger().info("FailOnceSubActivity: Success for input: " + input);
        return input + "-sub-rewound-success";
    }

    /**
     * HTTP trigger that attempts to rewind a non-existent orchestration instance.
     * This should result in an IllegalArgumentException being thrown by the client
     * when the server returns a NOT_FOUND gRPC status.
     */
    @FunctionName("StartRewindNonExistentOrchestration")
    public HttpResponseMessage startRewindNonExistentOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Attempting to rewind a non-existent orchestration instance.");

        DurableTaskClient client = durableContext.getClient();
        String nonExistentInstanceId = "non-existent-instance-" + System.currentTimeMillis();

        try {
            client.rewindInstance(nonExistentInstanceId, "Testing rewind on non-existent instance");
            // If we get here, the rewind did not throw as expected
            return request.createResponseBuilder(com.microsoft.azure.functions.HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("IllegalArgumentException was not thrown")
                    .build();
        } catch (IllegalArgumentException e) {
            context.getLogger().info("Rewind on non-existent instance threw expected exception: " + e.getMessage());
            return request.createResponseBuilder(com.microsoft.azure.functions.HttpStatus.OK)
                    .body(e.getMessage())
                    .build();
        }
    }

    /**
     * HTTP trigger to reset the sub-orchestration failure flag.
     */
    @FunctionName("ResetSubRewindFailureFlag")
    public HttpResponseMessage resetSubRewindFailureFlag(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        shouldSubFail.set(true);
        context.getLogger().info("Reset sub failure flag to true.");
        return request.createResponseBuilder(com.microsoft.azure.functions.HttpStatus.OK)
                .body("Sub failure flag reset to true")
                .build();
    }
}
