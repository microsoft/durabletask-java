package com.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.RetryPolicy;
import com.microsoft.durabletask.TaskOptions;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class RetriableTask {
    private static final AtomicBoolean exceptionThrew = new AtomicBoolean(false);
    @FunctionName("RetriableOrchestration")
    public HttpResponseMessage retriableOrchestration(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("RetriableTask");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("RetriableTask")
    public String retriableTask(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        RetryPolicy retryPolicy = new RetryPolicy(2, Duration.ofSeconds(1));
        TaskOptions taskOptions = new TaskOptions(retryPolicy);
        return ctx.callActivity("Append", "Test-Input", taskOptions, String.class).await();
    }

    @FunctionName("Append")
    public String append(
            @DurableActivityTrigger(name = "name") String name,
            final ExecutionContext context) {
        if (!exceptionThrew.get()) {
            exceptionThrew.compareAndSet(false, true);
            throw new RuntimeException("Test for retry");
        }
        context.getLogger().info("Append: " + name);
        return name + "-test";
    }
}
