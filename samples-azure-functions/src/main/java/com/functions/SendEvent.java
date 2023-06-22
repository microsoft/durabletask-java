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

    private final String instanceId = "waitEventID";
    private final String eventName = "testEvent";
    private final String eventData = "Hello World!";
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
        client.scheduleNewOrchestrationInstance("WaitEvent", null, instanceId);
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
        String instanceId = client.scheduleNewOrchestrationInstance("SendEvent", null, "sendEventID");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }
    //
    @FunctionName("WaitEvent")
    public String waitEvent(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx,
            final ExecutionContext context) {
        String await = ctx.waitForExternalEvent(eventName, String.class).await();
        context.getLogger().info("Event received with payload: " + await);
        return ctx.callActivity("Capitalize", await, String.class).await();
    }

    @FunctionName("SendEvent")
    public void sendEvent(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx,
            final ExecutionContext context) {
        ctx.sendEvent(instanceId, eventName, eventData);
        context.getLogger().info("Event sent");
        return;
    }
}
