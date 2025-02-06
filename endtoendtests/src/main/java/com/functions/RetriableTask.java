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
import java.util.concurrent.atomic.AtomicInteger;

public class RetriableTask {
    private static final AtomicBoolean throwException = new AtomicBoolean(true);
    private static final AtomicInteger failedCounter = new AtomicInteger(0);
    private static final AtomicInteger successCounter = new AtomicInteger(0);
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
        if (throwException.get()) {
            throwException.compareAndSet(true, false);
            throw new RuntimeException("Test for retry");
        }
        context.getLogger().info("Append: " + name);
        return name + "-test";
    }

    @FunctionName("RetriableOrchestrationFail")
    public HttpResponseMessage retriableOrchestrationFail(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("RetriableTaskFail");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("RetriableOrchestrationSuccess")
    public HttpResponseMessage retriableOrchestrationSuccess(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("RetriableTaskSuccess");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("RetriableTaskFail")
    public String retriableTaskFail(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        RetryPolicy retryPolicy = new RetryPolicy(2, Duration.ofSeconds(1));
        TaskOptions taskOptions = new TaskOptions(retryPolicy);
        return ctx.callActivity("AppendFail", "Test-Input", taskOptions, String.class).await();
    }

    @FunctionName("RetriableTaskSuccess")
    public String retriableTaskSuccess(
            @DurableOrchestrationTrigger(name = "ctx") TaskOrchestrationContext ctx) {
        RetryPolicy retryPolicy = new RetryPolicy(3, Duration.ofSeconds(1));
        TaskOptions taskOptions = new TaskOptions(retryPolicy);
        return ctx.callActivity("AppendSuccess", "Test-Input", taskOptions, String.class).await();
    }

    @FunctionName("AppendFail")
    public String appendFail(
            @DurableActivityTrigger(name = "name") String name,
            final ExecutionContext context) {
        if (failedCounter.get() < 2) {
            failedCounter.incrementAndGet();
            throw new RuntimeException("Test for retry");
        }
        context.getLogger().info("Append: " + name);
        return name + "-test";
    }

    @FunctionName("AppendSuccess")
    public String appendSuccess(
            @DurableActivityTrigger(name = "name") String name,
            final ExecutionContext context) {
        if (successCounter.get() < 2) {
            successCounter.incrementAndGet();
            throw new RuntimeException("Test for retry");
        }
        context.getLogger().info("Append: " + name);
        return name + "-test";
    }
}
