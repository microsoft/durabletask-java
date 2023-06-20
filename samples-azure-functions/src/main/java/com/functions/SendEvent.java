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
import com.microsoft.durabletask.Task;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

public class SendEvent {
    /**
     * This HTTP-triggered function starts the orchestration.
     */
    @FunctionName("WaitEventOrchestration")
    public HttpResponseMessage waitEventOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) throws TimeoutException, InterruptedException {
        context.getLogger().info("Java HTTP trigger processed a request.");
        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("WaitEvent", null, "123");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("SendEventOrchestration")
    public HttpResponseMessage sendEventOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) throws TimeoutException, InterruptedException {
        context.getLogger().info("Java HTTP trigger processed a request.");
        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("SendEvent");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }
    //
    @FunctionName("WaitEvent")
    public String waitEvent(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        String await = ctx.waitForExternalEvent("kcevent", String.class).await();
        return ctx.callActivity("Capitalize", await, String.class).await();
    }

    @FunctionName("SendEvent")
    public void sendEvent(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        ctx.sendEvent("123", "kcevent", "Hello World!");
        return;
    }
}
