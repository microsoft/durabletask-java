package com.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

import java.util.Optional;
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
     * HTTP trigger to start the rewindable orchestration.
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
     * HTTP trigger to start the parent orchestration for sub-orchestration rewind test.
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
