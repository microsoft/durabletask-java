package com.functions;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

public class ParallelFunctions {

    private static final AtomicBoolean throwException = new AtomicBoolean(true);
    @FunctionName("StartParallelOrchestration")
    public HttpResponseMessage startParallelOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("Parallel");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("Parallel")
    public List<String> parallelOrchestratorSad(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        RetryPolicy retryPolicy = new RetryPolicy(2, Duration.ofSeconds(5));
        TaskOptions taskOptions = new TaskOptions(retryPolicy);
        List<Task<String>> tasks = new ArrayList<>();
        tasks.add(ctx.callActivity("AppendSad", "Input1", taskOptions, String.class));
        tasks.add(ctx.callActivity("AppendSad", "Input2", taskOptions, String.class));
        tasks.add(ctx.callActivity("AppendHappy", "Input3", taskOptions, String.class));
        tasks.add(ctx.callActivity("AppendHappy", "Input4", taskOptions, String.class));
        tasks.add(ctx.callActivity("AppendHappy", "Input5", String.class));
        tasks.add(ctx.callActivity("AppendHappy", "Input6", String.class));
        return ctx.allOf(tasks).await();
    }

    @FunctionName("AppendHappy")
    public String appendHappy(
            @DurableActivityTrigger(name = "name") String name,
            final ExecutionContext context) {
        context.getLogger().info("AppendHappy: " + name);
        return name + "-test-happy";
    }

    @FunctionName("AppendSad")
    public String appendSad(
            @DurableActivityTrigger(name = "name") String name,
            final ExecutionContext context) {
        if (throwException.get()) {
            throwException.compareAndSet(true, false);
            throw new RuntimeException("Test sad path for retry");
        }
        context.getLogger().info("AppendSad: " + name);
        return name + "-test-sad";
    }


    @FunctionName("StartParallelAnyOf")
    public HttpResponseMessage startParallelAnyOf(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("ParallelAnyOf");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("ParallelAnyOf")
    public String parallelAnyOf(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        RetryPolicy retryPolicy = new RetryPolicy(2, Duration.ofSeconds(5));
        TaskOptions taskOptions = new TaskOptions(retryPolicy);
        List<Task<String>> tasks = new ArrayList<>();
        tasks.add(ctx.callActivity("AppendHappy", "AnyOf1", taskOptions, String.class));
        tasks.add(ctx.callActivity("AppendHappy", "AnyOf2", String.class));
        return ctx.anyOf(tasks).await();
    }
}