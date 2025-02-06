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

public class WaitExternalEvent {
    @FunctionName("ExternalEventHttp")
    public HttpResponseMessage externalEventHttp(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("ExternalEventOrchestrator");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("ExternalEventOrchestrator")
    public void externalEventOrchestrator(@DurableOrchestrationTrigger(name = "runtimeState") TaskOrchestrationContext ctx)
    {
        System.out.println("Waiting external event...");
        Task<String> event = ctx.waitForExternalEvent("event", String.class);
        Task<Void> timer = ctx.createTimer(Duration.ofSeconds(10));
        Task<?> winner = ctx.anyOf(event, timer).await();
        if (winner == event) {
            String eventResult = event.await();
            ctx.complete(eventResult);
        } else {
            ctx.complete("time out");
        }
    }
}
