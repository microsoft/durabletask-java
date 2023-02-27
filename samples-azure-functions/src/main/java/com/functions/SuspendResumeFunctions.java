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

public class SuspendResumeFunctions {
    /**
     * This HTTP-triggered function starts the orchestration.
     */
    @FunctionName("SuspendResumeOrchestration")
    public HttpResponseMessage suspendResumeOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) throws InterruptedException {
        context.getLogger().info("Java HTTP trigger processed a request.");

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("Cities");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        client.suspendInstance("Need to pause workflow");
        Thread.sleep(10000);
        client.resumeInstance("Continue workflow");
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    /**
     * This is the orchestrator function, which can schedule activity functions, create durable timers,
     * or wait for external events in a way that's completely fault-tolerant.
     */
    @FunctionName("SuspendResumeCities")
    public String Cities(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) throws InterruptedException {
        String result = "";
        result += ctx.callActivity("Capitalize", "Tokyo", String.class).await() + ", ";
        result += ctx.callActivity("Capitalize", "London", String.class).await() + ", ";
        Thread.sleep(5000);
        result += ctx.callActivity("Capitalize", "Seattle", String.class).await() + ", ";
        result += ctx.callActivity("Capitalize", "Austin", String.class).await();
        return result;
    }

    /**
     * This is the activity function that gets invoked by the orchestration.
     */
    @FunctionName("SuspendResumeCapitalize")
    public String Capitalize(
            @DurableActivityTrigger(name = "name") String name,
            final ExecutionContext context) {
        context.getLogger().info("Capitalizing: " + name);
        return name.toUpperCase();
    }
}
