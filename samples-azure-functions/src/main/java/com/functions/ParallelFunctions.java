package com.functions;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.time.Duration;
import java.util.*;

import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

public class ParallelFunctions {
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
    public List<String> parallelOrchestrator(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        RetryPolicy retryPolicy = new RetryPolicy(2, Duration.ofSeconds(5));
        TaskOptions taskOptions = new TaskOptions(retryPolicy);
        List<Task<String>> tasks = new ArrayList<>();
        tasks.add(ctx.callActivity("Append", "Input1", taskOptions, String.class));
        tasks.add(ctx.callActivity("Append", "Input2", taskOptions, String.class));
        tasks.add(ctx.callActivity("Append", "Input3", taskOptions, String.class));
        return ctx.allOf(tasks).await();
    }

    @FunctionName("Append")
    public String append(
            @DurableActivityTrigger(name = "name") String name,
            final ExecutionContext context) {
        context.getLogger().info("Append: " + name);
        return name + "-test";
    }
}
