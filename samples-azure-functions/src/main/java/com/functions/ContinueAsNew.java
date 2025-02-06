package com.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.Task;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

import java.time.Duration;
import java.util.Optional;

public class ContinueAsNew {
    @FunctionName("ContinueAsNew")
    public HttpResponseMessage continueAsNew(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("EternalOrchestrator");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("EternalOrchestrator")
    public void eternalOrchestrator(@DurableOrchestrationTrigger(name = "runtimeState") TaskOrchestrationContext ctx)
    {
        System.out.println("Processing stuff...");
        ctx.createTimer(Duration.ofSeconds(2)).await();
        ctx.continueAsNew(null);
    }

    @FunctionName("ContinueAsNewExternalEvent")
    public HttpResponseMessage continueAsNewExternalEvent(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("EternalEvent");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("EternalEvent")
    public void eternalEvent(@DurableOrchestrationTrigger(name = "runtimeState") TaskOrchestrationContext ctx)
    {
        System.out.println("Waiting external event...");
        Task<Void> event = ctx.waitForExternalEvent("event");
        Task<Void> timer = ctx.createTimer(Duration.ofSeconds(10));
        Task<?> result = ctx.anyOf(event, timer).await();
        if (result == event) {
            ctx.continueAsNew(null);
        }
    }
}
